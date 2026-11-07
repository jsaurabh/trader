"""All broker requests are intercepted; every company, fill and account is fabricated."""
import asyncio
import copy
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from recorder.paper_broker import EntryPermit, PaperBroker, PaperError
from recorder.portfolio import PaperLedger
from recorder.timebase import SECOND, Stamp, iso_utc, parse_rfc3339

CLIENT = 'research-'+'a'*32
BROKER_ID = '00000000-0000-4000-8000-000000000001'


class Clock:
    def __init__(self):
        self.value = Stamp(parse_rfc3339('2026-11-02T21:30:04Z')[0], 100*SECOND)
    def now(self):
        return self.value
    def advance(self, ns):
        self.value = Stamp(self.value.utc_ns+ns, self.value.mono_ns+ns)


def payload(client=CLIENT, symbol='ZZTEST', qty='10', side='buy'):
    return {'symbol': symbol, 'side': side, 'qty': qty, 'type': 'limit', 'limit_price': '10',
            'time_in_force': 'day', 'extended_hours': True, 'client_order_id': client}


def intent(clock, **kwargs):
    return payload(**kwargs) | {'expires_utc': iso_utc(clock.now().utc_ns+SECOND),
           'scheduled_exit_utc': '2026-11-03T15:00:00Z'}


def order(clock, p=None, *, filled='0', status='new', average='10'):
    return (p or payload()) | {'id': BROKER_ID, 'filled_qty': filled, 'filled_avg_price': average if Decimal(filled) else None,
                              'status': status, 'updated_at': iso_utc(clock.now().utc_ns)}


def snapshot(clock, *, cash='100000', equity='100000', previous='100000', positions=None, orders=None):
    return {'started': clock.now(), 'received': clock.now(), 'account': {'id': 'SYNTHETIC_ACCOUNT', 'status': 'ACTIVE',
            'currency': 'USD', 'trading_blocked': False, 'account_blocked': False,
            'cash': cash, 'equity': equity, 'last_equity': previous}, 'positions': positions or [], 'open_orders': orders or [],
            'config': {'no_shorting': True, 'max_margin_multiplier': '1', 'max_options_trading_level': 0,
                       'disable_overnight_trading': True, 'suspend_trade': False}}


class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC',
                                            'ALPACA_ENDPOINT': 'https://api.alpaca.markets'})
        self.env.start()
        self.clock = Clock()

    async def asyncTearDown(self):
        self.env.stop()

    def permit(self):
        return EntryPermit(CLIENT, self.clock.now(), self.clock.now().mono_ns, self.clock.now().mono_ns,
                           self.clock.now().utc_ns, self.clock.now().mono_ns, self.clock.now().mono_ns+SECOND)

    async def test_host_is_paper_even_when_environment_points_to_live(self):
        requests = []
        async def respond(request):
            requests.append(request)
            return httpx.Response(200, json={'status': 'SYNTHETIC'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            await PaperBroker(client, account_authorized=lambda: True, clock=self.clock.now).read('/v2/account')
        self.assertEqual(requests[0].url.host, 'paper-api.alpaca.markets')

    async def test_orders_disabled_and_research_inputs_never_reach_transport(self):
        def forbidden(request):
            self.fail('Order must not reach transport')
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            broker = PaperBroker(client, account_authorized=lambda: True, clock=self.clock.now, killed=lambda: False)
            with self.assertRaisesRegex(PaperError, 'paper_orders_disabled'):
                await broker.submit(payload(), permit=self.permit())
            broker.allow_orders = True
            permit = self.permit(); permit.mode = 'SYNTHETIC'
            with self.assertRaisesRegex(PaperError, 'research_input_cannot_submit_to_broker'):
                await broker.submit(payload(), permit=permit)

    async def test_pool_or_connection_delay_is_checked_at_headers_boundary(self):
        permit = self.permit()
        async def delayed(request):
            self.clock.advance(5*SECOND)
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            self.fail('Late request headers must not be sent')
        async with httpx.AsyncClient(transport=httpx.MockTransport(delayed)) as client:
            broker = PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: False)
            with self.assertRaisesRegex(PaperError, 'five_second_deadline_expired_before_dispatch'):
                await broker.submit(payload(), permit=permit)

    async def test_quote_can_expire_while_deadline_still_has_time(self):
        permit = self.permit()
        permit.expires_mono_ns += 4*SECOND
        async def delayed(request):
            self.clock.advance(SECOND+1)
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            self.fail('Stale state must block dispatch')
        async with httpx.AsyncClient(transport=httpx.MockTransport(delayed)) as client:
            with self.assertRaisesRegex(PaperError, 'quote_or_portfolio_expired_before_dispatch'):
                await PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: False).submit(payload(), permit=permit)

    async def test_kill_switch_is_rechecked_after_connection_queue(self):
        killed = [False]
        async def delayed(request):
            killed[0] = True
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            self.fail('Killed request must not be sent')
        async with httpx.AsyncClient(transport=httpx.MockTransport(delayed)) as client:
            with self.assertRaisesRegex(PaperError, 'manual_kill_switch'):
                await PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: killed[0]).submit(payload(), permit=self.permit())

    async def test_ambiguous_post_is_not_retried(self):
        calls = []
        async def ambiguous(request):
            calls.append(request)
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            raise httpx.ReadTimeout('SYNTHETIC timeout')
        async with httpx.AsyncClient(transport=httpx.MockTransport(ambiguous)) as client:
            with self.assertRaises(PaperError) as raised:
                await PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: False).submit(payload(), permit=self.permit())
        self.assertTrue(raised.exception.ambiguous)
        self.assertEqual(len(calls), 1)

    async def test_ack_may_arrive_later_without_backdating_dispatch_or_fills(self):
        permit = self.permit()
        async def respond(request):
            await request.extensions['trace']('http11.send_request_headers.started', {'request': request})
            self.clock.advance(6*SECOND)
            return httpx.Response(200, json=order(self.clock))
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: False).submit(payload(), permit=permit)
        self.assertEqual(result['filled_qty'], '0')
        self.assertEqual(self.clock.now().mono_ns-permit.receipt.mono_ns, 6*SECOND)

    async def test_missing_trace_is_ambiguous_and_never_claimed_timely(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=order(self.clock)))) as client:
            with self.assertRaisesRegex(PaperError, 'dispatch_trace_unavailable') as raised:
                await PaperBroker(client, account_authorized=lambda: True, allow_orders=True, clock=self.clock.now, killed=lambda: False).submit(payload(), permit=self.permit())
        self.assertTrue(raised.exception.ambiguous)

    async def test_unsupported_orders_and_position_caps_are_rejected(self):
        for change in ({'type': 'market'}, {'qty': '101'}, {'side': 'short'}, {'time_in_force': 'gtc'}, {'qty': '.5'}):
            with self.subTest(change=change), self.assertRaises(PaperError):
                PaperBroker.validate_payload(payload() | change)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'synthetic-paper.sqlite'
        self.clock = Clock()
        self.ledger = PaperLedger(self.path, mode='SYNTHETIC', clock=self.clock.now)
        self.assertTrue(self.ledger.reconcile(snapshot(self.clock))['reconciled'])

    def tearDown(self):
        self.tmp.cleanup()

    def reserve(self, client=CLIENT, symbol='ZZTEST', event='SYNTHETIC_EVENT', canonical='SYNTHETIC_PERIOD', qty='10'):
        return self.ledger.reserve_entry({'event_id': event, 'ticker': symbol, 'mode': 'SYNTHETIC'},
               intent(self.clock, client=client, symbol=symbol, qty=qty), self.clock.now(), canonical, 'SYNTHETIC_RUN')

    def test_no_intent_becomes_a_fill_and_uncertain_order_holds_cash(self):
        self.reserve()
        self.ledger.mark(CLIENT, 'unknown')
        view = self.ledger.reconcile(snapshot(self.clock))
        self.assertEqual(view['positions'], [])
        self.assertEqual(view['entry_reservations'], [{'ticker': 'ZZTEST', 'notional': '100'}])

    def test_duplicate_and_corrected_new_vendor_id_share_durable_canonical_guard(self):
        self.reserve()
        with self.assertRaises(PaperError):
            self.reserve(client='research-'+'b'*32, event='SYNTHETIC_CORRECTED_VENDOR_ID')
        self.assertEqual(len(self.ledger.orders()), 1)

    def test_atomic_capacity_survives_stale_callers(self):
        for i in range(3):
            self.reserve(client='research-'+str(i)*32, symbol='ZZ'+str(i), event='E'+str(i), canonical='C'+str(i))
        with self.assertRaisesRegex(PaperError, 'atomic_position_capacity'):
            self.reserve(client='research-'+'f'*32, symbol='ZZFOUR', event='E4', canonical='C4')

    def test_deadline_expiry_during_disk_transaction_rolls_back(self):
        receipt = self.clock.now()
        self.clock.advance(5*SECOND)
        with self.assertRaisesRegex(PaperError, 'five_second_deadline_expired_before_reservation'):
            self.ledger.reserve_entry({'event_id': 'E'}, intent(self.clock), receipt, 'C', 'RUN')
        self.assertEqual(self.ledger.orders(), [])

    def test_partial_fill_then_cancel_preserves_filled_position_and_releases_only_remainder(self):
        self.reserve()
        self.ledger.apply_order(order(self.clock, filled='4', status='partially_filled'))
        self.ledger.mark(CLIENT, 'cancel_requested')
        position = [{'symbol': 'ZZTEST', 'qty': '4', 'market_value': '40', 'side': 'long'}]
        view = self.ledger.reconcile(snapshot(self.clock, cash='99960', positions=position))
        self.assertEqual(view['entry_reservations'][0]['notional'], '60')
        self.clock.advance(SECOND)
        self.ledger.apply_order(order(self.clock, filled='4', status='canceled'))
        view = self.ledger.reconcile(snapshot(self.clock, cash='99960', positions=position))
        self.assertTrue(view['reconciled'])
        self.assertEqual(view['positions'][0]['qty'], '4')
        self.assertEqual(view['entry_reservations'], [])

    def test_repeated_fill_and_stale_update_cannot_double_count_cash_or_regress_position(self):
        self.reserve()
        current = order(self.clock, filled='4', status='partially_filled')
        update = {'event': 'partial_fill', 'execution_id': 'SYNTHETIC_EXEC_1', 'qty': '4', 'price': '10', 'order': current}
        self.ledger.trade_update(update); self.ledger.trade_update(update)
        stale = copy.deepcopy(current); stale.update(filled_qty='1', updated_at='2026-11-02T21:30:03Z')
        self.ledger.apply_order(stale)
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '4')
        self.assertEqual(self.ledger.orders()[0]['filled_notional'], '40')

    def test_newer_cumulative_regression_requires_review(self):
        self.reserve(); self.ledger.apply_order(order(self.clock, filled='4', status='partially_filled'))
        self.clock.advance(SECOND)
        self.ledger.apply_order(order(self.clock, filled='1', status='partially_filled'))
        self.assertTrue(self.ledger.flags()['review_required'])
        self.assertEqual(self.ledger.orders()[0]['filled_qty'], '4')

    def test_daily_loss_pause_persists_after_recovery_and_resets_next_day(self):
        self.reserve(qty='100'); self.ledger.apply_order(order(self.clock, payload(qty='100'), filled='100', status='filled'))
        position = [{'symbol': 'ZZTEST', 'qty': '100', 'market_value': '400', 'side': 'long'}]
        self.ledger.reconcile(snapshot(self.clock, cash='99000', equity='99400', positions=position))
        self.assertTrue(self.ledger.flags()['daily_pause'])
        position[0]['market_value'] = '600'
        self.ledger.reconcile(snapshot(self.clock, cash='99000', equity='99600', positions=position))
        self.assertTrue(self.ledger.flags()['daily_pause'])
        self.clock.advance(86400*SECOND)
        self.ledger.reconcile(snapshot(self.clock, cash='99000', equity='99600', previous='99600', positions=position))
        self.assertFalse(self.ledger.flags()['daily_pause'])

    def test_restarts_require_reconciliation_and_cannot_mix_observed_with_synthetic(self):
        self.reserve()
        restored = PaperLedger(self.path, mode='SYNTHETIC', clock=self.clock.now)
        self.assertFalse(restored.flags()['reconciled'])
        self.assertEqual(len(restored.orders()), 1)
        with self.assertRaises(ValueError):
            PaperLedger(self.path, mode='OBSERVED', clock=self.clock.now)

    def test_exits_cannot_oversell_parent_lot_or_reuse_a_closed_old_lot(self):
        self.reserve(); self.ledger.apply_order(order(self.clock, filled='4', status='canceled'))
        parent = self.ledger.orders()[0]
        exit_payload = payload(client='research-'+'c'*32, side='sell', qty='5')
        with self.assertRaisesRegex(PaperError, 'exit_would_exceed'):
            self.ledger.reserve_exit(parent, exit_payload, 'RUN', self.clock.now().utc_ns+SECOND)
        exit_payload['qty'] = '4'
        self.ledger.reserve_exit(parent, exit_payload, 'RUN', self.clock.now().utc_ns+SECOND)
        self.ledger.apply_order(order(self.clock, exit_payload, filled='4', status='filled'))
        self.reserve(client='research-'+'d'*32, event='NEXT_E', canonical='NEXT_C')
        self.ledger.apply_order(order(self.clock, payload(client='research-'+'d'*32), filled='4', status='canceled'))
        exit_payload['client_order_id'] = 'research-'+'e'*32
        with self.assertRaises(PaperError):
            self.ledger.reserve_exit(parent, exit_payload, 'RUN', self.clock.now().utc_ns+SECOND)
