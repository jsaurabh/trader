"""Independent regressions: fabricated accounts/orders only, with no network writes."""
import asyncio
import copy
from dataclasses import replace
from decimal import Decimal
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import UUID

import httpx

from recorder.analysis_demo import context_for
from recorder.execution import PaperExecution
from recorder.paper_broker import EntryPermit, PaperBroker, PaperError
from recorder.portfolio import PaperLedger
from recorder.timebase import DEADLINE_NS, MarketCalendar, SECOND, Stamp, parse_rfc3339
from test_decision import inputs_for
from test_paper import Clock, CLIENT, intent, order, payload, snapshot


def broker_order(clock, source_payload=None, *, filled='0', status='new', account_tag='A'):
    value = order(clock, source_payload, filled=filled, status=status)
    value['id'] = str(UUID(hashlib.sha256((account_tag+value['client_order_id']).encode()).hexdigest()[:32]))
    return value


class MemoryJournal:
    run_id = 'SYNTHETIC_REVIEW_RUN'
    def __init__(self):
        self.records = []
    def emit(self, kind, data, stamp=None):
        self.records.append((kind, copy.deepcopy(data), stamp))


class FabricatedBroker:
    """In-process broker fixture. It has no HTTP client and cannot place an order."""
    simulation = True
    allow_orders = True
    stream_ready = True
    def __init__(self, clock):
        self.clock = clock
        self.book = {}
        self.current_snapshot = snapshot(clock)
        self.lookups, self.posts, self.cancels, self.config_writes = [], [], [], []
        self.account_authorized = lambda: False
        self.killed = lambda: True
        self.reject_status = None
        self.before_dispatch = None
    async def ensure_long_only_config(self):
        return self.current_snapshot['config']
    async def order(self, client_id):
        self.lookups.append(client_id)
        return copy.deepcopy(self.book.get(client_id))
    async def snapshot(self):
        result = copy.deepcopy(self.current_snapshot)
        result.update(started=self.clock.now(), received=self.clock.now())
        return result
    async def submit(self, source_payload, *, permit=None, exit_check=None, on_dispatch=None, reservation_check=None, on_abort=None):
        if not self.account_authorized():
            raise PaperError('paper_account_identity_not_verified')
        if self.before_dispatch:
            self.before_dispatch(source_payload)
        if reservation_check:
            reservation_check()
        if permit:
            # The fixture exercises the production clock/freshness guard but does not
            # grant research input any authority to use a real transport.
            replace(permit, mode='OBSERVED').check(self.clock.now(), self.killed)
        elif exit_check:
            exit_check(self.clock.now())
        if on_dispatch:
            on_dispatch(self.clock.now())
        self.posts.append(copy.deepcopy(source_payload))
        if self.reject_status:
            raise PaperError('paper_order_http_error', http_status=self.reject_status, ambiguous=False)
        result = broker_order(self.clock, source_payload)
        self.book[source_payload['client_order_id']] = result
        return result
    async def cancel(self, broker_id):
        if not self.account_authorized():
            raise PaperError('paper_account_identity_not_verified')
        self.cancels.append(broker_id)
        return 204  # Acceptance, not a terminal cancellation/fill assertion.


class MutableMarket:
    """Captured quote stays valid-looking while the current stream state changes."""
    def __init__(self, delegate, *, halt_after_capture=False):
        self.delegate, self.halt_after_capture = delegate, halt_after_capture
        self.reasons = []
    async def __call__(self, symbol):
        captured = await self.delegate(symbol)
        if self.halt_after_capture:
            self.halt()
        return captured
    def halt(self):
        self.reasons = ['current_trading_action_not_tradable']
    def validate_dispatch(self, ticker, captured_quote, now, *, side='buy', quantity=None):
        return list(self.reasons)


class CoordinatorReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.clock = Clock()
        self.ledger = PaperLedger(self.path/'synthetic-ledger.sqlite', mode='SYNTHETIC', clock=self.clock.now)
        self.ledger.reconcile(snapshot(self.clock))
        self.broker = FabricatedBroker(self.clock)
        self.journal = MemoryJournal()
        async def market(symbol):
            q = inputs_for(self.clock.now(), self.clock.now().utc_ns-4*SECOND)['quote']
            q.update(ticker=symbol, bid_size_shares='200')
            return q
        self.engine = PaperExecution(self.ledger, self.broker, self.journal, MarketCalendar(self.clock.now().utc_ns),
                                    market, kill_path=self.path/'kill', clock=self.clock.now)

    async def asyncTearDown(self):
        if self.engine.account_lock:
            self.engine.account_lock.close()
        self.tmp.cleanup()

    def reserve(self, client=CLIENT, symbol='ZZTEST', event='SYNTHETIC_EVENT', canonical='SYNTHETIC_PERIOD'):
        p = self.ledger.reserve_entry({'event_id': event, 'ticker': symbol, 'mode': 'SYNTHETIC'},
            intent(self.clock, client=client, symbol=symbol), self.clock.now(), canonical, self.journal.run_id)
        return p

    def prepared_and_analysis(self):
        published = self.clock.now().utc_ns-4*SECOND
        prepared = context_for(published)
        prepared.update({k: inputs_for(self.clock.now(), published)[k] for k in ('company', 'liquidity', 'reference')})
        prepared['announcement_key'] = 'SYNTHETIC_CANONICAL_ANNOUNCEMENT'
        prepared['company']['canonical_company_id'] = 'SYNTHETIC_COMPANY'
        analysis = {'event_id': 'SYNTHETIC_ADMISSION_EVENT', 'ticker': 'ZZTEST', 'mode': 'SYNTHETIC',
            'strategy_version': 'research-defaults-v0', 'facts_pass_source_validation': True, 'reasons': [],
            'source_sha256': 'f'*64,
            'issuer_identity': {'verified': True, 'analysis_source_sha256': 'f'*64, 'canonical_company_id': 'SYNTHETIC_COMPANY'},
            'checks': {'eps_exceeds_consensus': True, 'revenue_beats_consensus_by_two_pct': True,
                       'revenue_guidance_midpoint_increases': True}}
        return prepared, analysis, published

    async def test_wrong_account_cannot_rewrite_ledger_or_submit_exit(self):
        p = self.reserve()
        self.ledger.apply_order(broker_order(self.clock, p, filled='4', status='canceled'))
        target = parse_rfc3339('2026-11-03T15:00:00Z')[0]
        self.clock.advance(target-self.clock.now().utc_ns)
        self.broker.current_snapshot = snapshot(self.clock, cash='99940', positions=[
            {'symbol': 'ZZTEST', 'qty': '6', 'qty_available': '6', 'market_value': '60', 'side': 'long'}])
        self.broker.current_snapshot['account']['id'] = 'SYNTHETIC_DIFFERENT_ACCOUNT_B'
        b_order = broker_order(self.clock, p, filled='6', status='canceled', account_tag='B')
        self.broker.book[CLIENT] = b_order
        try:
            await self.engine.refresh()
        except PaperError:
            pass  # Either an explicit refusal or an unreconciled view is acceptable.
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '4')
        await self.engine.update({'event': 'partial_fill', 'execution_id': 'SYNTHETIC_B_EXEC', 'order': b_order})
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '4')
        await self.engine.exit_position(self.ledger.orders()[0], Decimal('4'), 1)
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.broker.cancels, [])
        self.assertEqual(self.broker.config_writes, [])

    async def test_missed_terminal_fill_is_recovered_from_broker(self):
        p = self.reserve()
        self.ledger.apply_order(broker_order(self.clock, p, filled='4', status='canceled'))
        self.clock.advance(SECOND)
        self.broker.book[CLIENT] = broker_order(self.clock, p, filled='6', status='canceled')
        self.broker.current_snapshot = snapshot(self.clock, cash='99940', positions=[
            {'symbol': 'ZZTEST', 'qty': '6', 'market_value': '60', 'side': 'long'}])
        view = await self.engine.refresh()
        self.assertTrue(view['reconciled'])
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '6')
        self.assertEqual(view['positions'][0]['qty'], '6')

    async def test_admission_waits_for_required_reconnect_sweep(self):
        clients = []
        for i in range(3):
            client = 'research-'+str(i)*32
            p = self.reserve(client, 'ZZ'+str(i), 'E'+str(i), 'C'+str(i))
            terminal = broker_order(self.clock, p, status='canceled')
            self.ledger.apply_order(terminal)
            self.broker.book[client] = terminal
            clients.append(client)
        for _ in range(5):
            await self.engine.refresh()
            recovered = set(self.broker.lookups) & set(clients)
            if len(recovered) < len(clients):
                self.assertTrue(self.engine.killed(), 'New admission must not race an unfinished required recovery sweep')
            else:
                self.assertFalse(self.engine.killed())
                break
        self.assertEqual(set(self.broker.lookups), set(clients))

    async def test_mismatch_recovery_finishes_remaining_sweep_after_first_fix(self):
        clients = []
        for i in range(3):
            client = 'research-'+str(i)*32
            p = self.reserve(client, 'ZZ'+str(i), 'E'+str(i), 'C'+str(i))
            terminal = broker_order(self.clock, p, filled='4' if i == 2 else '0', status='canceled')
            self.ledger.apply_order(terminal)
            self.broker.book[client] = terminal
            clients.append(client)
        self.clock.advance(SECOND)
        self.broker.book[clients[-1]] = broker_order(self.clock, payload(client=clients[-1], symbol='ZZ2'), filled='6', status='canceled')
        self.broker.current_snapshot = snapshot(self.clock, cash='99940', positions=[
            {'symbol': 'ZZ2', 'qty': '6', 'market_value': '60', 'side': 'long'}])
        self.engine.recovery_needed = False  # Simulate steady state before detecting the mismatch.
        for _ in range(5):
            await self.engine.refresh()
            if set(self.broker.lookups) == set(clients):
                break
        self.assertEqual(set(self.broker.lookups), set(clients))
        self.assertEqual(next(r for r in self.ledger.orders() if r['client_id'] == clients[-1])['filled_qty'], '6')

    async def test_order_expiry_uses_monotonic_time_after_utc_rolls_back(self):
        p = self.reserve()
        active = broker_order(self.clock, p)
        self.ledger.apply_order(active)
        self.broker.book[CLIENT] = active
        self.broker.current_snapshot = snapshot(self.clock, orders=[active])
        await self.engine.refresh()
        self.clock.advance(2*SECOND)
        self.clock.value = Stamp(self.clock.now().utc_ns-60*SECOND, self.clock.now().mono_ns)
        await self.engine.maintain_orders()
        self.assertIn(active['id'], self.broker.cancels)
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '0')

    async def test_reservation_invalidated_before_submitting_cannot_send(self):
        prepared, analysis, published = self.prepared_and_analysis()
        original = self.ledger.mark
        def invalidate(client_id, state, expected=None):
            if state == 'submitting':
                original(client_id, 'not_submitted', 'reserved')
            return original(client_id, state, expected)
        decisions = []
        with patch.object(self.ledger, 'mark', invalidate):
            await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda value, stamp: decisions.append(value.copy()))
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')
        self.assertTrue(decisions)
        self.assertEqual(decisions[-1]['decision'], 'skip')

    async def test_reservation_invalidated_at_dispatch_boundary_cannot_send(self):
        prepared, analysis, published = self.prepared_and_analysis()
        self.broker.before_dispatch = lambda p: self.ledger.mark(p['client_order_id'], 'not_submitted')
        await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda *args: None)
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')

    async def test_definite_403_rejection_releases_reservation_without_inventing_fill(self):
        prepared, analysis, published = self.prepared_and_analysis()
        self.broker.reject_status = 403
        await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda *args: None)
        rows = self.ledger.orders()
        self.assertEqual(len(self.broker.posts), 1)
        self.assertEqual(rows[0]['state'], 'rejected')
        self.assertEqual(rows[0]['filled_qty'], '0')
        view = self.ledger.reconcile(snapshot(self.clock))
        self.assertEqual(view['entry_reservations'], [])
        self.assertEqual(view['positions'], [])

    async def test_announcement_invalidated_during_quote_fetch_cannot_reserve_or_send(self):
        prepared, analysis, published = self.prepared_and_analysis()
        allowed = [True]
        original_market = self.engine.market
        async def corrected_market(symbol):
            value = await original_market(symbol)
            allowed[0] = False
            return value
        self.engine.market = corrected_market
        decisions = []
        await self.engine.enter(analysis, self.clock.now(), published, prepared,
            lambda value, stamp: decisions.append(value.copy()), admission_guard=lambda: allowed[0])
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders(), [])
        self.assertIn('announcement_clock_changed_before_dispatch', decisions[-1]['reasons'])

    async def test_announcement_invalidated_during_connection_queue_cannot_send(self):
        prepared, analysis, published = self.prepared_and_analysis()
        allowed = [True]
        self.broker.before_dispatch = lambda p: allowed.__setitem__(0, False)
        decisions = []
        await self.engine.enter(analysis, self.clock.now(), published, prepared,
            lambda value, stamp: decisions.append(value.copy()), admission_guard=lambda: allowed[0])
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')
        self.assertIn('announcement_clock_changed_before_dispatch', decisions[-1]['reasons'])

    async def test_uncommitted_admission_audit_blocks_post_and_releases_reservation(self):
        prepared, analysis, published = self.prepared_and_analysis()
        receipt = self.clock.now()
        deadlines = []
        async def uncommitted(deadline_mono_ns, *, clock):
            deadlines.append(deadline_mono_ns)
            self.assertTrue(any(kind == 'paper_admission_audit' for kind, _, _ in self.journal.records))
            self.assertEqual(self.broker.posts, [])
            return False
        self.journal.wait_durable = uncommitted
        decisions = []
        await self.engine.enter(analysis, receipt, published, prepared,
            lambda value, stamp: decisions.append(value.copy()))
        self.assertEqual(deadlines, [receipt.mono_ns+DEADLINE_NS])
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')
        self.assertIn('paper_admission_audit_not_durable_before_deadline', decisions[-1]['reasons'])

    async def test_admission_commit_cannot_renew_original_five_second_clock(self):
        prepared, analysis, published = self.prepared_and_analysis()
        async def slow_commit(deadline_mono_ns, *, clock):
            self.clock.advance(DEADLINE_NS)
            return True  # Even a faulty/late success cannot bypass transport guard.
        self.journal.wait_durable = slow_commit
        await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda *args: None)
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')

    async def test_correction_during_audit_commit_blocks_physical_dispatch(self):
        prepared, analysis, published = self.prepared_and_analysis()
        current = [True]
        async def correction_during_commit(deadline_mono_ns, *, clock):
            current[0] = False
            return True
        self.journal.wait_durable = correction_during_commit
        decisions = []
        await self.engine.enter(analysis, self.clock.now(), published, prepared,
            lambda value, stamp: decisions.append(value.copy()), admission_guard=lambda: current[0])
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')
        self.assertIn('announcement_clock_changed_before_dispatch', decisions[-1]['reasons'])

    async def test_partial_exit_cancel_reprices_only_remaining_owned_shares(self):
        entry_payload = self.reserve()
        terminal_entry = broker_order(self.clock, entry_payload, filled='4', status='canceled')
        self.ledger.apply_order(terminal_entry)
        self.broker.book[CLIENT] = terminal_entry
        target = parse_rfc3339('2026-11-03T15:00:00Z')[0]
        self.clock.advance(target-self.clock.now().utc_ns)
        self.broker.current_snapshot = snapshot(self.clock, cash='99960', positions=[
            {'symbol': 'ZZTEST', 'qty': '4', 'qty_available': '4', 'market_value': '40', 'side': 'long'}])
        await self.engine.refresh()
        await self.engine.maintain_orders()
        self.assertEqual([p['qty'] for p in self.broker.posts], ['4'])
        await self.engine.maintain_orders()
        self.assertEqual(len(self.broker.posts), 1, 'A still-working exit must not be duplicated')
        first_exit = self.broker.posts[0]
        self.clock.advance(5*SECOND)
        self.broker.book[first_exit['client_order_id']] = broker_order(self.clock, first_exit, filled='2', status='canceled')
        self.broker.current_snapshot = snapshot(self.clock, cash='99980', positions=[
            {'symbol': 'ZZTEST', 'qty': '2', 'qty_available': '2', 'market_value': '20', 'side': 'long'}])
        await self.engine.refresh()
        await self.engine.maintain_orders()
        self.assertEqual([p['qty'] for p in self.broker.posts], ['4', '2'])
        self.assertTrue(all(p['side'] == 'sell' for p in self.broker.posts))
        second_exit = self.broker.posts[1]
        self.clock.advance(SECOND)
        self.broker.book[second_exit['client_order_id']] = broker_order(self.clock, second_exit, filled='2', status='filled')
        self.broker.current_snapshot = snapshot(self.clock)
        await self.engine.refresh()
        await self.engine.maintain_orders()
        self.assertEqual(len(self.broker.posts), 2)
        self.assertEqual(self.ledger.holdings(self.ledger.orders()), {})

    async def test_known_halt_after_quote_capture_blocks_new_entry(self):
        prepared, analysis, published = self.prepared_and_analysis()
        self.engine.market = MutableMarket(self.engine.market, halt_after_capture=True)
        await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda *args: None)
        self.assertEqual(self.broker.posts, [])
        self.assertTrue(all(row['state'] in ('not_submitted', 'rejected') for row in self.ledger.orders()))

    async def test_known_halt_during_connection_queue_blocks_new_entry(self):
        prepared, analysis, published = self.prepared_and_analysis()
        market = MutableMarket(self.engine.market)
        self.engine.market = market
        self.broker.before_dispatch = lambda p: market.halt()
        await self.engine.enter(analysis, self.clock.now(), published, prepared, lambda *args: None)
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.orders()[0]['state'], 'not_submitted')

    async def assert_halted_exit_blocked(self, halt_after_capture):
        entry_payload = self.reserve()
        terminal_entry = broker_order(self.clock, entry_payload, filled='4', status='canceled')
        self.ledger.apply_order(terminal_entry)
        self.broker.book[CLIENT] = terminal_entry
        target = parse_rfc3339('2026-11-03T15:00:00Z')[0]
        self.clock.advance(target-self.clock.now().utc_ns)
        self.broker.current_snapshot = snapshot(self.clock, cash='99960', positions=[
            {'symbol': 'ZZTEST', 'qty': '4', 'qty_available': '4', 'market_value': '40', 'side': 'long'}])
        market = MutableMarket(self.engine.market, halt_after_capture=halt_after_capture)
        self.engine.market = market
        if not halt_after_capture:
            self.broker.before_dispatch = lambda p: market.halt()
        await self.engine.refresh()
        await self.engine.exit_position(self.ledger.orders()[0], Decimal('4'), 1)
        self.assertEqual(self.broker.posts, [])
        self.assertEqual(self.ledger.holdings(self.ledger.orders()), {'ZZTEST': Decimal('4')})

    async def test_known_halt_after_capture_blocks_exit(self):
        await self.assert_halted_exit_blocked(True)

    async def test_known_halt_during_connection_queue_blocks_exit(self):
        await self.assert_halted_exit_blocked(False)


class FinalBoundaryReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_correction_after_candidate_callback_is_rechecked_before_headers(self):
        clock = Clock()
        receipt = clock.now()
        permit = EntryPermit(CLIENT, receipt, receipt.mono_ns, receipt.mono_ns,
                             receipt.utc_ns, receipt.mono_ns, receipt.mono_ns+DEADLINE_NS)
        allowed, sent = [True], []
        def guard():
            if not allowed[0]:
                raise PaperError('announcement_clock_changed_before_dispatch')
        async def handler(request):
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            sent.append(clock.now())
            return httpx.Response(200, json=broker_order(clock))
        with patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC'}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                broker = PaperBroker(client, allow_orders=True, clock=clock.now,
                                     killed=lambda: False, account_authorized=lambda: True)
                with self.assertRaisesRegex(PaperError, 'announcement_clock_changed_before_dispatch'):
                    await broker.submit(payload(), permit=permit, reservation_check=guard,
                                        on_dispatch=lambda stamp: allowed.__setitem__(0, False))
        self.assertEqual(sent, [])

    async def test_kill_activated_during_audit_blocks_headers(self):
        clock = Clock()
        receipt = clock.now()
        permit = EntryPermit(CLIENT, receipt, receipt.mono_ns, receipt.mono_ns,
                             receipt.utc_ns, receipt.mono_ns, receipt.mono_ns+DEADLINE_NS)
        killed, sent = [False], []
        class AuditJournal:
            def emit(self, kind, data, stamp=None):
                if kind == 'paper_order_dispatch':
                    clock.advance(100_000_000)
                    killed[0] = True
        async def handler(request):
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            sent.append(clock.now())
            return httpx.Response(200, json=broker_order(clock))
        with patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC'}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                broker = PaperBroker(client, AuditJournal(), allow_orders=True, clock=clock.now,
                                     killed=lambda: killed[0], account_authorized=lambda: True)
                with self.assertRaises(PaperError):
                    await broker.submit(payload(), permit=permit)
        self.assertEqual(sent, [])


class StreamRecoveryReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_gap_callback_failure_does_not_kill_reconnect_loop(self):
        reconnected = asyncio.Event()
        connects, gap_calls = [], []
        class Socket:
            def __init__(self, number):
                self.number = number
            async def send(self, message):
                pass
            async def recv(self):
                if self.number == 1:
                    raise ConnectionError('SYNTHETIC disconnected socket')
                await asyncio.Event().wait()
        class Connection:
            def __init__(self, number):
                self.number = number
            async def __aenter__(self):
                if self.number > 1:
                    reconnected.set()
                return Socket(self.number)
            async def __aexit__(self, *args):
                return False
        def connect(url, **kwargs):
            self.assertEqual(url, 'wss://paper-api.alpaca.markets/stream')
            connects.append(url)
            return Connection(len(connects))
        async def gap():
            gap_calls.append(True)
            raise RuntimeError('SYNTHETIC transient pause persistence failure')
        async def update(*args):
            self.fail('No trade update is sent by this fixture')
        async def fast_sleep(seconds):
            pass
        with patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC'}), \
                patch('recorder.paper_broker.connect', connect), patch('recorder.paper_broker.asyncio.sleep', fast_sleep):
            broker = PaperBroker(None, MemoryJournal())
            task = asyncio.create_task(broker.trade_updates(update, gap))
            try:
                await asyncio.wait_for(reconnected.wait(), 0.5)
                self.assertFalse(task.done())
                self.assertEqual(len(gap_calls), 1)
                self.assertEqual(len(connects), 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_slow_dispatch_callback_cannot_send_after_deadline(self):
        clock = Clock()
        receipt = clock.now()
        permit = EntryPermit(CLIENT, receipt, receipt.mono_ns, receipt.mono_ns,
                             receipt.utc_ns, receipt.mono_ns, receipt.mono_ns+DEADLINE_NS)
        sent = []
        async def handler(request):
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            sent.append(clock.now())
            return httpx.Response(200, json=broker_order(clock))
        with patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC'}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                broker = PaperBroker(client, allow_orders=True, clock=clock.now,
                                     killed=lambda: False, account_authorized=lambda: True)
                with self.assertRaises(PaperError):
                    await broker.submit(payload(), permit=permit, on_dispatch=lambda stamp: clock.advance(6*SECOND))
        self.assertEqual(sent, [])
