"""SYNTHETIC, network-free broker/market lifecycle proof; no observed paper orders."""
import argparse
import asyncio
import copy
from contextlib import AsyncExitStack
from decimal import Decimal
import html
import json
import os
from pathlib import Path
import time
from types import MappingProxyType

import httpx

from .analysis import DEFAULT_MODEL, ModelRequestBudget, TimedAnalyzer
from .analysis_demo import SOURCE, context_for
from .candidates import CandidateExtractor, build_catalogue, select_payload, sha
from .context import LIQUIDITY_POLICY, expected_liquidity_end
from .execution import PaperExecution
from .journal import Journal
from .issuer import verify_issuer
from .paper_broker import EntryPermit, PaperBroker, PaperError, TERMINAL
from .portfolio import PaperLedger
from .report import atomic_text
from .timebase import DEADLINE_NS, SECOND, MarketCalendar, Stamp, iso_utc, local_time, parse_rfc3339

VERSION = "SYNTHETIC-paper-lifecycle-v1"
PUBLICATION_UTC = "2026-11-02T21:30:00Z"
ACCOUNT = MappingProxyType({"id": "SYNTHETIC_DEMO_ACCOUNT_NEVER_NETWORKED", "status": "ACTIVE", "currency": "USD",
    "trading_blocked": False, "account_blocked": False})
CONFIG = MappingProxyType({"no_shorting": True, "max_margin_multiplier": "1", "max_options_trading_level": 0,
    "disable_overnight_trading": True, "suspend_trade": False})
SCENARIOS = {"complete", "late_market", "queued_admission", "queued_dispatch", "source_gap", "context_gap", "model_rejects_revenue"}


class SyntheticClock:
    """Real elapsed monotonic entry time, with a fictional calendar and explicit jumps."""
    def __init__(self):
        self.anchor_mono = time.monotonic_ns()
        self.anchor_utc = parse_rfc3339("2026-11-02T21:30:04Z")[0]
        self.offset_ns = 0
        self.jumps = []

    def now(self):
        measured = time.monotonic_ns()
        return Stamp(self.anchor_utc+measured-self.anchor_mono+self.offset_ns, measured+self.offset_ns)

    def jump(self, ns, reason):
        if ns < 0:
            raise ValueError("Synthetic clock cannot jump backwards")
        before = self.now()
        self.offset_ns += ns
        self.jumps.append({"reason": reason, "fictional_from_utc": iso_utc(before.utc_ns),
            "fictional_to_utc": iso_utc(before.utc_ns+ns), "injected_elapsed_ns": ns,
            "observed_market_time": False})


class SyntheticJournal:
    """Tag all component records and preserve real record time alongside fictional UTC."""
    def __init__(self, journal, clock):
        self.journal, self.clock = journal, clock
        self.run_id, self.path = journal.run_id, journal.path

    def emit(self, kind, data, stamp=None):
        self.journal.emit(kind, {**data, "mode": "SYNTHETIC", "synthetic_fixture": True,
            "observed_market_data": False, "broker_network_requests": 0,
            "real_recorded_utc": iso_utc(Stamp.now().utc_ns),
            "stamp_clock_domain": "fictional UTC; measured monotonic entry plus explicit injected jumps"}, stamp or self.clock.now())

    async def wait_durable(self, deadline_mono_ns, *, clock=Stamp.now):
        # The underlying SQLite writer is real even though the market/broker are
        # fabricated. Keep its commit inside the same measured entry budget.
        return await self.journal.wait_durable(deadline_mono_ns, clock=clock)


class SimulationBroker:
    """In-memory only. No network-client parameter, credentials, sockets or HTTP calls."""
    simulation = True
    stream_ready = True
    allow_orders = True

    def __init__(self, clock, journal=None, *, queue_delay_ns=0):
        if not isinstance(clock, SyntheticClock):
            raise TypeError("SimulationBroker requires its explicit synthetic clock")
        if journal is not None and not isinstance(journal,SyntheticJournal):
            raise TypeError("SimulationBroker accepts only a tagged synthetic journal, never a network client")
        self.clock, self.journal, self.queue_delay_ns = clock, journal, queue_delay_ns
        self.killed, self.account_authorized = lambda: True, lambda: False
        self._orders, self.dispatches, self.cancellations = {}, [], []
        self._cash, self._shares = Decimal("100000"), Decimal(0)

    def emit(self, kind, data):
        if self.journal:
            self.journal.emit(kind, data, self.clock.now())

    def positions(self):
        return [] if not self._shares else [{"symbol": "ZZTEST", "qty": str(self._shares), "qty_available": str(self._shares),
            "market_value": str(self._shares*Decimal("10.02")), "side": "long", "source": "SYNTHETIC"}]

    async def read(self, path, *, params=None, missing_ok=False):
        if path == "/v2/account":
            return dict(ACCOUNT, cash=str(self._cash), equity=str(self._cash+self._shares*Decimal("10.02")), last_equity="100000")
        if path == "/v2/account/configurations":
            return dict(CONFIG)
        if path == "/v2/positions":
            return self.positions()
        if path == "/v2/orders":
            return copy.deepcopy([v for v in self._orders.values() if v['status'] not in TERMINAL])
        if path == "/v2/orders:by_client_order_id":
            return await self.order((params or {}).get('client_order_id'))
        raise PaperError("unrecognized_synthetic_read_path")

    async def snapshot(self):
        started = self.clock.now()
        return {"account": await self.read('/v2/account'), "config": await self.read('/v2/account/configurations'),
            "positions": await self.read('/v2/positions'), "open_orders": await self.read('/v2/orders'),
            "started": started, "received": self.clock.now(), "source": "SYNTHETIC in-memory simulation"}

    async def ensure_long_only_config(self):
        return dict(CONFIG)

    async def order(self, client_id):
        return copy.deepcopy(self._orders.get(client_id))

    async def submit(self, payload, *, permit=None, exit_check=None, on_dispatch=None, reservation_check=None, on_abort=None):
        PaperBroker.validate_payload(payload)  # Static shape/risk check only; never construct PaperBroker.
        if not self.allow_orders or self.account_authorized() is not True:
            raise PaperError("synthetic_orders_not_authorized")
        if payload['client_order_id'] in self._orders:
            raise PaperError("duplicate_synthetic_submission")
        if payload['side'] == 'buy':
            if not isinstance(permit, EntryPermit) or permit.mode != 'SYNTHETIC' or permit.client_order_id != payload['client_order_id']:
                raise PaperError("synthetic_matching_entry_permit_required")
            permit.check_timing(self.clock.now(), self.killed)
        elif exit_check is None:
            raise PaperError("synthetic_long_exit_proof_required")
        if self.queue_delay_ns and payload['side'] == 'buy':
            self.clock.jump(self.queue_delay_ns, "SYNTHETIC connection/admission queue fault before dispatch")
            self.queue_delay_ns = 0
        if reservation_check:
            reservation_check()
        if payload['side'] == 'buy':
            permit.check_timing(self.clock.now(), self.killed)
        else:
            exit_check(self.clock.now())
        if on_dispatch:
            on_dispatch(self.clock.now())
        try:
            # Audit/callback work cannot extend the deadline.
            if reservation_check:
                reservation_check()
            if payload['side'] == 'buy':
                permit.check_timing(self.clock.now(), self.killed)
            else:
                exit_check(self.clock.now())
        except PaperError as exc:
            if on_abort:
                on_abort(exc, self.clock.now())
            raise
        stamp = self.clock.now()
        dispatch = {"payload": copy.deepcopy(payload), "fictional_dispatch_utc": iso_utc(stamp.utc_ns),
            "dispatch_mono_ns": stamp.mono_ns, "simulated_dispatch": True, "http_request_sent": False,
            "elapsed_from_receipt_ns": stamp.mono_ns-permit.receipt.mono_ns if permit else None}
        self.dispatches.append(dispatch)
        self.emit('synthetic_order_dispatch', dispatch)
        value = dict(payload, id="SYNTHETIC_ORDER_"+str(len(self.dispatches)), status="new", filled_qty="0",
            filled_avg_price=None, updated_at=iso_utc(stamp.utc_ns))
        self._orders[payload['client_order_id']] = value
        return copy.deepcopy(value)

    async def cancel(self, order_id):
        if self.account_authorized() is not True:
            raise PaperError('synthetic_account_identity_not_verified')
        if not any(v['id'] == order_id for v in self._orders.values()):
            raise PaperError('unknown_synthetic_order')
        self.cancellations.append({"order_id": order_id, "fictional_requested_utc": iso_utc(self.clock.now().utc_ns),
            "confirmed": False, "http_request_sent": False})
        self.emit('synthetic_cancel_request', self.cancellations[-1])
        return 204  # A fixture acknowledgment is not cancellation confirmation.

    def fill(self, client_id, cumulative_qty, *, execution_id):
        value = self._orders[client_id]
        cumulative = Decimal(cumulative_qty)
        previous, total = Decimal(value['filled_qty']), Decimal(value['qty'])
        if value['status'] in TERMINAL or not previous < cumulative <= total:
            raise PaperError('invalid_synthetic_fill')
        delta, price = cumulative-previous, Decimal(value['limit_price'])
        if value['side'] == 'sell' and delta > self._shares:
            raise PaperError('synthetic_exit_would_short')
        self._shares += delta if value['side'] == 'buy' else -delta
        self._cash += delta*price*(-1 if value['side'] == 'buy' else 1)
        value.update(filled_qty=str(cumulative), filled_avg_price=str(price),
            status='filled' if cumulative == total else 'partially_filled', updated_at=iso_utc(self.clock.now().utc_ns))
        return {"event": 'fill' if cumulative == total else 'partial_fill', "execution_id": execution_id,
            "qty": str(delta), "price": str(price), "order": copy.deepcopy(value), "synthetic_fixture": True}

    def confirm_cancel(self, client_id):
        value = self._orders[client_id]
        if value['status'] not in TERMINAL:
            value.update(status='canceled', updated_at=iso_utc(self.clock.now().utc_ns))
        for item in self.cancellations:
            if item['order_id'] == value['id']:
                item['confirmed'] = True
        return {'event': 'canceled', 'order': copy.deepcopy(value), 'synthetic_fixture': True}


class SyntheticCandidateExtractor(CandidateExtractor):
    """Offline ID fixture; uses production catalogue and inherited independent proof.

    This is not an LLM and measures no real API latency. No network client or key is
    accepted. Real API mode uses CandidateExtractor itself, without this subclass.
    """
    model = 'SYNTHETIC_OFFLINE_ID_SELECTOR'
    mode = 'SYNTHETIC_OFFLINE_CANDIDATE_SELECTOR'
    prompt_version = 'SYNTHETIC_OFFLINE_ID_SELECTION_V1'
    prompt = 'SYNTHETIC offline fixture: accept offered immutable IDs, except explicitly injected rejection faults. No model is called.'
    implementation_source = Path(__file__).read_text()

    def __init__(self, journal, calls, *, reject_revenue=False):
        self.journal, self.calls, self.reject_revenue = journal, calls, reject_revenue

    async def extract_document(self, document, ticker):
        catalogue = await asyncio.to_thread(build_catalogue,document['text'],ticker,document.get('raw_html'))
        base = {'candidate_catalogue':catalogue,'candidate_version':catalogue['version'],
            'candidate_prompt_version':self.prompt_version,'model':self.model}
        self.journal.emit('candidate_catalogue',{'ticker':ticker,**base})
        if catalogue['fatal_errors'] or not catalogue['preferred_ids']:
            return {**base,'payload':{'ticker':ticker,'facts':[],'missing_information':catalogue['missing_information']},
                'decoding_error':'deterministic_candidate_catalogue_not_usable'}
        rejected = [item['id'] for item in catalogue['candidates'] if item['id'] in catalogue['preferred_ids']
            and self.reject_revenue and item['fact']['name']=='REVENUE']
        selection = {'ticker':ticker,'selected_ids':[i for i in catalogue['preferred_ids'] if i not in rejected],
            'rejected_ids':rejected,'missing_information':['SYNTHETIC selector fixture rejected revenue'] if rejected else []}
        self.calls.append({'real_api_request':False,'offered_ids':len(catalogue['preferred_ids'])})
        result = {**base,'raw_output':json.dumps(selection),'payload':select_payload(catalogue,selection),
            'candidate_catalogue_sha256':sha(catalogue),'candidate_input_sha256':sha(catalogue['preferred_ids']),
            'candidate_model_schema_sha256':sha(self.model_schema),'response_id':'SYNTHETIC_RESPONSE'}
        self.journal.emit('synthetic_candidate_id_selection',{'real_api_request':False,**result})
        return result


def prepared_fixture(published_ns, calendar):
    prepared = context_for(published_ns)
    common = {'source': 'SYNTHETIC', 'available_at': iso_utc(published_ns-60*SECOND)}
    prepared.update(announcement_key='SYNTHETIC:ZZTEST:earnings:2026-06-30:QUARTER',
        company={**common, 'ticker': 'ZZTEST', 'security_type': 'US_COMMON_STOCK', 'exchange': 'NASDAQ',
            'tradable': True, 'price_tick': '0.01', 'tick_source': 'SYNTHETIC', 'classification_verified':True,
            'canonical_company_id':'SYNTHETIC:ZZTEST:COMMON',
            'legal_issuer_name': 'SYNTHETIC ZZTEST', 'issuer_identity_verified': True,
            'verified_issuer_aliases': ['ZZTEST', 'SYNTHETIC ZZTEST'], 'exchange_ticker_verified': True,
            'issuer_name_proof': [{'document_id': 'SYNTHETIC_ISSUER_DEFINITION', 'type': 'text', 'passage': 'SYNTHETIC ZZTEST'}]},
        liquidity={**common, 'session_count': 20, 'average_daily_dollar_volume': '50000000',
            'last_session_close': iso_utc(expected_liquidity_end(calendar,published_ns)),
            'measurement_policy': LIQUIDITY_POLICY, 'liquidity_policy': LIQUIDITY_POLICY},
        reference={**common, 'price': '10.00', 'observed_at': iso_utc(published_ns-61*SECOND), 'reliable': True})
    return prepared


async def run_paper_demo(journal, state_path, *, use_api=False, model=DEFAULT_MODEL, scenario='complete'):
    """One selector attempt, no retries. Existing ledger paths are rejected.

    Real API mode can contact OpenAI once; the broker and quotes always stay in-process.
    Fault scenarios inject fictional time rather than sleeping through deadlines.
    """
    if scenario not in SCENARIOS:
        raise ValueError('unknown synthetic paper demo scenario')
    if use_api and scenario != 'complete':
        raise ValueError('Real API demo is restricted to the complete fabricated-input scenario')
    state_path = Path(state_path)
    if state_path.exists():
        raise ValueError('Synthetic demo requires a new, separate ledger path')
    clock = SyntheticClock()
    tagged = SyntheticJournal(journal, clock)
    published = parse_rfc3339(PUBLICATION_UTC)[0]
    calendar = MarketCalendar(published)
    prepared = prepared_fixture(published, calendar)
    if scenario == 'context_gap':
        prepared['eps']['comparability_verified'] = False
    ledger = PaperLedger(state_path, mode='SYNTHETIC', clock=clock.now)
    broker = SimulationBroker(clock, tagged, queue_delay_ns=DEADLINE_NS if scenario == 'queued_dispatch' else 0)
    market_calls = 0

    async def market(ticker):
        nonlocal market_calls
        market_calls += 1
        if scenario == 'late_market' and market_calls == 1:
            clock.jump(DEADLINE_NS, 'SYNTHETIC market response delay fault')
        now = clock.now()
        regular = calendar.session(now.utc_ns) == 'regular'
        quote = {'ticker': ticker, 'bid': '10.02' if regular else '10.00', 'ask': '10.04' if regular else '10.02',
            'ask_size_shares': '200', 'bid_size_shares': '200', 'source_utc': iso_utc(now.utc_ns),
            'available_utc': iso_utc(now.utc_ns), 'available_mono_ns': now.mono_ns,
            'coverage_verified': True, 'conditions_eligible': True, 'size_unit_verified': True,
            'broker_session_supported': True, 'market_status': 'tradable',
            'market_status_available_utc': iso_utc(now.utc_ns), 'source': 'SYNTHETIC in-memory quote',
            'reason_codes': [], 'observed_market_data': False}
        tagged.emit('synthetic_market_quote', quote, now)
        return quote

    engine = PaperExecution(ledger, broker, tagged, calendar, market,
        kill_path=str(state_path)+'.SYNTHETIC-kill', clock=clock.now)
    await engine.initialize()
    selector_calls = []

    async with AsyncExitStack() as stack:
        budget = ModelRequestBudget(tagged, limit=1, clock=clock.now) if use_api else None
        previous_api_attempts = len(budget.starts) if use_api else 0
        if use_api:
            client = await stack.enter_async_context(httpx.AsyncClient(timeout=5,follow_redirects=False))
            extractor = CandidateExtractor(client, model=model, budget=budget, journal=tagged)
        else:
            extractor = SyntheticCandidateExtractor(tagged,selector_calls,reject_revenue=scenario=='model_rejects_revenue')
        analyzer = TimedAnalyzer(extractor, tagged, clock=clock.now)
        tagged.emit('synthetic_demo_definition', {'version': VERSION, 'scenario': scenario,
            'selector': 'real OpenAI API on fabricated inputs' if use_api else 'SYNTHETIC offline deterministic ID selector',
            'api_attempt_limit': 1 if use_api else 0, 'prepared': prepared, 'source': SOURCE,
            'publication_utc_is_fictional': True, 'live_order_submission_enabled': False})
        actual_started = Stamp.now()
        receipt = clock.now()
        tagged.emit('synthetic_alert_receipt', {'event_id': 'SYNTHETIC-paper-demo', 'ticker': 'ZZTEST',
            'fictional_publication_utc': PUBLICATION_UTC, 'fictional_receipt_utc': iso_utc(receipt.utc_ns),
            'fictional_publication_to_receipt_ns': receipt.utc_ns-published,
            'publication_timestamp_meaning': 'fabricated publication instant, not vendor latency evidence',
            'publication_precision_ns': SECOND, 'receipt_mono_ns': receipt.mono_ns}, receipt)

        async def load():
            await asyncio.sleep(0)  # Simulated asynchronous document handoff, no retrieval network.
            document = {'full_text_verified': scenario != 'source_gap', 'text': SOURCE,
                'source': 'SYNTHETIC in-memory full release'}
            document['issuer_identity'] = verify_issuer(document,prepared['company'])
            document['issuer_identity']['analysis_source_sha256'] = sha(SOURCE)
            tagged.emit('synthetic_document_ready', {'full_text_verified': document['full_text_verified'],
                'text': SOURCE, 'retrieval_network_requests': 0})
            return document

        analysis = await analyzer.process('SYNTHETIC-paper-demo', 'ZZTEST', receipt, load,
            context=prepared, published_ns=published, mode='SYNTHETIC', defer_execution_checks=True)
        decisions = []

        def decided(result, stamp):
            decisions.append(copy.deepcopy(result))
            tagged.emit('synthetic_paper_demo_decision', result, stamp)

        if scenario == 'queued_admission':
            await engine.entry_lock.acquire()
            task = asyncio.create_task(engine.enter(analysis, receipt, published, prepared, decided))
            await asyncio.sleep(0)
            clock.jump(DEADLINE_NS, 'SYNTHETIC serialized-admission queue fault')
            engine.entry_lock.release()
            result = await task
        else:
            result = await engine.enter(analysis, receipt, published, prepared, decided)
        actual_finished = Stamp.now()
    entry_phase_clock = clock.now()
    phase_jumps = copy.deepcopy(clock.jumps)
    lifecycle = []

    def snapshot_lifecycle(phase):
        rows = ledger.orders()
        record = {'phase': phase, 'fictional_utc': iso_utc(clock.now().utc_ns),
            'orders': rows, 'positions': broker.positions(), 'remaining_shares': str(broker._shares),
            'cash': str(broker._cash), 'synthetic_fixture': True}
        lifecycle.append(record)
        tagged.emit('synthetic_lifecycle_checkpoint', record)
        return rows

    snapshot_lifecycle('after_entry_acknowledgment; no fill inferred')
    if result.get('decision') == 'synthetic_entry_ready' and broker.dispatches:
        client_id = result['client_order_id']
        quantity = Decimal((await broker.order(client_id))['qty'])
        partial = min(Decimal(25), quantity)
        clock.jump(SECOND//4, 'SYNTHETIC later partial-fill event')
        update = broker.fill(client_id, str(partial), execution_id='SYNTHETIC_EXEC_PARTIAL_ENTRY')
        await engine.update(update, clock.now())
        await engine.refresh()
        snapshot_lifecycle('entry_partial_fill_confirmed')
        clock.jump(2*SECOND, 'SYNTHETIC entry limit expiry reached')
        await engine.maintain_orders()
        snapshot_lifecycle('entry_expired_cancel_requested; remainder still reserved')
        clock.jump(SECOND//10, 'SYNTHETIC separate cancellation confirmation')
        await engine.update(broker.confirm_cancel(client_id), clock.now())
        await engine.refresh()
        rows = snapshot_lifecycle('entry_cancellation_confirmed; filled shares retained')
        parent = next(r for r in rows if r['role'] == 'entry')
        target = parent['exit_utc_ns']
        clock.jump(max(0,target-clock.now().utc_ns), 'SYNTHETIC jump to exchange-calendar scheduled exit')
        await engine.maintain_orders()
        rows = snapshot_lifecycle('scheduled_limit_exit_acknowledged; no fill inferred')
        exits = [row for row in rows if row['role'] == 'exit']
        if exits:
            clock.jump(SECOND//5, 'SYNTHETIC separate scheduled-exit fill event')
            exit_row = exits[-1]
            await engine.update(broker.fill(exit_row['client_id'],exit_row['payload']['qty'],
                execution_id='SYNTHETIC_EXEC_EXIT'),clock.now())
            # Duplicate updates must remain idempotent in the real durable ledger.
            await engine.refresh()
            snapshot_lifecycle('exit_fill_confirmed_and_reconciled')
    final_portfolio = await engine.refresh()
    report = {'version': VERSION, 'mode': 'SYNTHETIC', 'scenario': scenario, 'run_id': journal.run_id,
        'journal_path': str(journal.path), 'ledger_path': str(state_path),
        'real_processing_started_utc': iso_utc(actual_started.utc_ns), 'real_processing_finished_utc': iso_utc(actual_finished.utc_ns),
        'entry_measured_monotonic_duration_ns': actual_finished.mono_ns-actual_started.mono_ns,
        'entry_clock_elapsed_from_receipt_ns': entry_phase_clock.mono_ns-receipt.mono_ns,
        'injected_entry_delay_ns': sum(j['injected_elapsed_ns'] for j in phase_jumps),
        'fictional_publication_utc': PUBLICATION_UTC, 'fictional_receipt_utc': iso_utc(receipt.utc_ns),
        'fictional_receipt_display_pacific': local_time(receipt.utc_ns),
        'original_five_second_ceiling_ns': DEADLINE_NS, 'analysis': analysis, 'entry_result': result,
        'decision_callbacks': decisions, 'clock_jumps': clock.jumps, 'lifecycle': lifecycle,
        'final_orders': ledger.orders(), 'final_portfolio': final_portfolio,
        'remaining_shares': str(broker._shares), 'synthetic_dispatches': broker.dispatches,
        'synthetic_cancel_requests': broker.cancellations, 'simulated_order_submissions': len(broker.dispatches),
        'broker_network_requests': 0, 'actual_broker_order_submissions': 0, 'live_order_submission_enabled': False,
        'api_called': use_api and len(budget.starts)>previous_api_attempts,
        'api_attempts': len(budget.starts)-previous_api_attempts if use_api else 0,
        'offline_selector_calls': len(selector_calls), 'selector_mode': 'real_api_fabricated_input' if use_api else 'SYNTHETIC_OFFLINE_ID_SELECTOR',
        'lifecycle_completed': bool(broker.dispatches) and len(broker.dispatches) == 2 and broker._shares == 0
            and final_portfolio is not None and final_portfolio['reconciled'],
        'limitations': ['All alerts, publication/delivery times, companies, expectations, quotes, accounts, orders and fills are fabricated.',
            'Only processing duration and optional OpenAI API responses are observed; no market or broker transport is exercised.',
            'Positive entry timing uses measured monotonic time; fault and lifecycle jumps are explicitly injected, never waiting for market time.',
            'Simulated acknowledgments are distinct from separate fabricated fill/cancel events; an intent does not create shares.',
            'This exercises current integration and durability, not prospective latency, source accuracy, fill quality or profitability.']}
    tagged.emit('synthetic_paper_demo_report', report)
    return report


def write_paper_demo_report(report, json_path, html_path=None):
    atomic_text(json_path,json.dumps(report,indent=2)+'\n')
    if html_path:
        esc = lambda value: html.escape(str(value))
        rows = ''.join('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in (
            row['phase'],row['fictional_utc'],row['remaining_shares'],
            '; '.join(r['role']+': '+r['state']+' '+r['filled_qty']+'/'+r['payload']['qty'] for r in row['orders'])))+'</tr>' for row in report['lifecycle'])
        atomic_text(html_path,'<!doctype html><meta charset="utf-8"><title>SYNTHETIC paper lifecycle proof</title>'
            '<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:20px}td,th{padding:10px;border-bottom:1px solid #bbb;text-align:left}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
            '<h1>SYNTHETIC paper lifecycle proof</h1><p>Fabricated market and broker; zero actual broker requests or orders.</p>'
            '<p>Selector: '+esc(report['selector_mode'])+'. Entry decision: '+esc(report['entry_result']['decision'])+'. Measured entry processing: '
            +esc(round(report['entry_measured_monotonic_duration_ns']/1e6,3))+' ms. Injected entry delay: '
            +esc(report['injected_entry_delay_ns']/1e6)+' ms.</p><p>Simulation complete: '+esc(report['lifecycle_completed'])
            +'. Remaining fabricated shares: '+esc(report['remaining_shares'])+'.</p>'
            '<table><tr><th>Phase</th><th>Fictional UTC</th><th>Shares</th><th>Order states / cumulative fills</th></tr>'+rows+'</table>'
            '<ul>'+''.join('<li>'+esc(item)+'</li>' for item in report['limitations'])+'</ul>'
            '<details><summary>Full proof record</summary><pre>'+esc(json.dumps(report,indent=2))+'</pre></details>')


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',required=True); parser.add_argument('--state',required=True)
    parser.add_argument('--report',required=True); parser.add_argument('--html')
    parser.add_argument('--api',action='store_true'); parser.add_argument('--model',default=DEFAULT_MODEL)
    parser.add_argument('--scenario',choices=sorted(SCENARIOS),default='complete')
    args = parser.parse_args()
    if Path(args.db).exists():
        parser.error('Demo requires a new journal database')
    journal = Journal(args.db,{'mode':'SYNTHETIC','purpose':VERSION,'broker_network_requests':0})
    try:
        report = asyncio.run(run_paper_demo(journal,args.state,use_api=args.api,model=args.model,scenario=args.scenario))
        write_paper_demo_report(report,args.report,args.html)
        journal.emit('run_end',{'mode':'SYNTHETIC','reason':'synthetic_paper_demo_complete','broker_network_requests':0})
    finally:
        journal.close()
    print(json.dumps({key:report[key] for key in ('mode','selector_mode','lifecycle_completed','remaining_shares',
        'entry_measured_monotonic_duration_ns','injected_entry_delay_ns','simulated_order_submissions','actual_broker_order_submissions','api_attempts')}))


if __name__ == '__main__':
    main()
