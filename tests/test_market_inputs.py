"""Network-free fabricated quotes/statuses; no market performance evidence."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

import httpx

from recorder.market_inputs import MarketInputs, StatusBook, validate_for_exit, verified_shares
from recorder.timebase import MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339


class Clock:
    def __init__(self, utc='2026-11-02T21:30:04Z'):
        self.value = Stamp(parse_rfc3339(utc)[0], 10_000*SECOND)
    def now(self):
        return self.value
    def advance(self, ns):
        self.value = Stamp(self.value.utc_ns+ns, self.value.mono_ns+ns)


def quote(clock, **changes):
    return {'T': 'q', 'S': 'ZZTEST', 't': iso_utc(clock.now().utc_ns), 'bp': '10', 'ap': '10.02',
            'bs': 80, 'as': 200, 'bx': 'Q', 'ax': 'Q', 'c': ['R'], 'z': 'C'} | changes


def metadata_response(request):
    tape = request.url.params.get('tape')
    return httpx.Response(200, json={'R': 'Regular Two Sided Open' if tape == 'C' else 'Regular Market Maker Open', 'N': 'Non Firm Quote'})


def establish_status(book, clock):
    earlier = Stamp(clock.now().utc_ns-3*3600*SECOND, clock.now().mono_ns-3*3600*SECOND)
    book.connected('sip', earlier)
    book.subscribed({'T': 'subscription', 'quotes': ['ZZTEST'], 'statuses': ['ZZTEST']}, earlier)
    event = {'T': 's', 'S': 'ZZTEST', 'sc': 'T', 'sm': 'Trading Resumption', 'rc': 'R', 't': iso_utc(earlier.utc_ns+SECOND), 'z': 'C'}
    book.ingest(event, Stamp(earlier.utc_ns+SECOND, earlier.mono_ns+SECOND))
    book.ingest(quote(clock), clock.now())


class SizeTests(unittest.TestCase):
    def test_post_transition_rest_sizes_are_shares_without_multiplier(self):
        clock = Clock()
        result = verified_shares(quote(clock), 'sip')
        self.assertTrue(result['size_unit_verified'])
        self.assertEqual(result['ask_size_shares'], '200')
        self.assertEqual(result['bid_size_shares'], '80')
        self.assertEqual(result['size_provenance']['conversion_multiplier'], 1)

    def test_pre_transition_round_lots_are_not_silently_100_shares(self):
        result = verified_shares(quote(Clock('2025-10-31T20:00:00Z')), 'sip')
        self.assertFalse(result['size_unit_verified'])
        self.assertIsNone(result['ask_size_shares'])

    def test_unknown_feed_or_tape_and_invalid_sizes_stay_unknown(self):
        for changes, feed in (({'z': 'O'}, 'sip'), ({}, 'boats'), ({'as': True}, 'sip'), ({'as': '200'}, 'sip'), ({'as': -1}, 'sip')):
            with self.subTest(changes=changes, feed=feed):
                self.assertFalse(verified_shares(quote(Clock(), **changes), feed)['size_unit_verified'])

    def test_sip_websocket_uses_explicit_dated_change_policy_iex_stream_stays_unknown(self):
        result = verified_shares(quote(Clock()), 'sip', observation_method='websocket_stream')
        self.assertTrue(result['size_unit_verified'])
        self.assertEqual(result['ask_size_shares'], '200')
        self.assertEqual(result['size_provenance']['policy'], 'alpaca-cta-utp-post-2025-11-03-v1')
        self.assertIn('supersedes', result['size_provenance']['documentation_conflict'])
        self.assertFalse(verified_shares(quote(Clock()), 'iex', observation_method='websocket_stream')['size_unit_verified'])
        self.assertFalse(verified_shares(quote(Clock('2025-10-31T20:00:00Z')), 'sip', observation_method='websocket_stream')['size_unit_verified'])


class StatusTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.book = Clock(), StatusBook()

    def test_open_quote_without_trading_action_never_establishes_tradable(self):
        self.book.connected('sip', self.clock.now())
        self.book.subscribed({'quotes': ['ZZTEST'], 'statuses': ['ZZTEST']}, self.clock.now())
        self.book.ingest(quote(self.clock), self.clock.now())
        self.assertEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'unknown')

    def test_old_transition_plus_current_continuous_stream_is_not_a_stale_status(self):
        establish_status(self.book, self.clock)
        result = self.book.snapshot('ZZTEST', self.clock.now(), 'sip')
        self.assertEqual(result['market_status'], 'tradable')
        self.assertGreater(result['status_provenance']['transition_age_ns'], 3600*SECOND)
        self.assertEqual(result['market_status_available_utc'], iso_utc(self.clock.now().utc_ns))

    def test_disconnect_invalidates_baseline_and_replayed_old_transition_cannot_restore_it(self):
        establish_status(self.book, self.clock)
        self.book.disconnected()
        self.book.connected('sip', self.clock.now())
        self.book.subscribed({'quotes': ['ZZTEST'], 'statuses': ['ZZTEST']}, self.clock.now())
        self.book.ingest({'T': 's', 'S': 'ZZTEST', 'sc': 'T', 'z': 'C', 't': iso_utc(self.clock.now().utc_ns-SECOND)}, self.clock.now())
        self.book.ingest(quote(self.clock), self.clock.now())
        self.assertEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'unknown')

    def test_halt_overrides_later_regular_quotes_until_explicit_resumption(self):
        establish_status(self.book, self.clock)
        self.clock.advance(1)
        self.book.ingest({'T': 's', 'S': 'ZZTEST', 'sc': 'H', 'z': 'C', 't': iso_utc(self.clock.now().utc_ns)}, self.clock.now())
        self.clock.advance(1)
        self.book.ingest(quote(self.clock), self.clock.now())
        self.assertEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'halted')
        self.book.ingest({'T': 's', 'S': 'ZZTEST', 'sc': 'Q', 'z': 'C', 't': iso_utc(self.clock.now().utc_ns)}, self.clock.now())
        self.assertEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'quotation_only')
        self.clock.advance(1)
        self.book.ingest({'T': 's', 'S': 'ZZTEST', 'sc': 'T', 'z': 'C', 't': iso_utc(self.clock.now().utc_ns)}, self.clock.now())
        self.assertEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'tradable')

    def test_status_subscription_and_same_feed_continuity_are_required(self):
        establish_status(self.book, self.clock)
        self.assertNotEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'iex')['market_status'], 'tradable')
        self.clock.advance(SECOND+1)
        self.assertNotEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'tradable')

    def test_conflicting_statuses_at_same_timestamp_fail_closed(self):
        self.book.connected('sip', self.clock.now())
        self.book.subscribed({'quotes': ['ZZTEST'], 'statuses': ['ZZTEST']}, self.clock.now())
        for code in ('T', 'H'):
            self.book.ingest({'T': 's', 'S': 'ZZTEST', 'sc': code, 'z': 'C', 't': iso_utc(self.clock.now().utc_ns)}, self.clock.now())
        self.assertNotEqual(self.book.snapshot('ZZTEST', self.clock.now(), 'sip')['market_status'], 'tradable')


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.calendar = MarketCalendar(self.clock.now().utc_ns)
        self.env = patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC'})
        self.env.start()
    async def asyncTearDown(self):
        self.env.stop()

    async def test_current_sip_quote_has_exact_fields_after_prepared_metadata_and_status(self):
        async def respond(request):
            if '/meta/conditions/' in request.url.path:
                return metadata_response(request)
            return httpx.Response(200, json={'symbol': 'ZZTEST', 'currency': 'USD', 'quote': quote(self.clock)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider.prewarm(); establish_status(provider.status, self.clock)
            result = await provider('ZZTEST')
        self.assertEqual(result['reason_codes'], [])
        self.assertTrue(result['eligible_for_execution_checks'])
        self.assertEqual(result['ask_size_shares'], '200')
        self.assertEqual(validate_for_exit(result, self.clock.now()), [])

    async def test_dispatch_uses_current_halt_and_connection_not_captured_tradable(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(metadata_response)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider.prewarm()
            establish_status(provider.status, self.clock)
            captured = provider.normalize('ZZTEST', {'symbol': 'ZZTEST', 'quote': quote(self.clock)}, self.clock.now())
            self.assertEqual(provider.validate_dispatch('ZZTEST', captured, self.clock.now(), quantity='50'), [])
            self.clock.advance(1)
            provider.status.ingest({'T':'s','S':'ZZTEST','sc':'H','z':'C','t':iso_utc(self.clock.now().utc_ns)}, self.clock.now())
            self.assertIn('current_trading_action_not_tradable', provider.validate_dispatch('ZZTEST', captured, self.clock.now()))
            provider.status.disconnected()
            establish_status(provider.status, self.clock)
            self.assertIn('market_connection_changed_since_quote', provider.validate_dispatch('ZZTEST', captured, self.clock.now()))

    async def test_known_new_quote_can_invalidate_spread_size_and_conditions_before_send(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(metadata_response)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider.prewarm()
            establish_status(provider.status, self.clock)
            captured = provider.normalize('ZZTEST', {'symbol':'ZZTEST','quote':quote(self.clock)}, self.clock.now())
            self.clock.advance(1)
            provider.status.ingest(quote(self.clock, ap='10.20', **{'as':10}), self.clock.now())
            reasons = provider.validate_dispatch('ZZTEST', captured, self.clock.now(), quantity='50')
            self.assertIn('current_quote_spread_above_entry_limit', reasons)
            self.assertIn('current_displayed_size_below_order_quantity', reasons)
            self.clock.advance(1)
            provider.status.ingest(quote(self.clock, c=['N']), self.clock.now())
            self.assertTrue(any('quote_conditions' in r for r in provider.validate_dispatch('ZZTEST', captured, self.clock.now())))
            self.clock.advance(1)
            provider.status.ingest(quote(self.clock, bp='4.98', ap='4.99'), self.clock.now())
            self.assertIn('current_ask_below_minimum_entry_price', provider.validate_dispatch('ZZTEST', captured, self.clock.now()))

    async def test_denied_sip_never_falls_back_or_echoes_error_body(self):
        requests = []
        async def respond(request):
            requests.append(request)
            return httpx.Response(403, text='SYNTHETIC_SENSITIVE_ERROR_BODY')
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await MarketInputs(client, calendar=self.calendar, clock=self.clock.now)('ZZTEST')
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.params['feed'], 'sip')
        self.assertEqual(result['status'], 'access_denied')
        self.assertNotIn('SYNTHETIC_SENSITIVE', json.dumps(result))

    async def test_expired_deadline_prevents_request_and_slow_request_is_cancelled(self):
        entered, cancelled = asyncio.Event(), asyncio.Event()
        async def respond(request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            result = await provider('ZZTEST', deadline_mono_ns=self.clock.now().mono_ns)
            self.assertFalse(entered.is_set())
            self.assertIn('five_second_deadline_expired_before_market_request', result['reason_codes'])
            result = await provider('ZZTEST', deadline_mono_ns=self.clock.now().mono_ns+20_000_000)
            self.assertTrue(cancelled.is_set())
            self.assertEqual(result['status'], 'timed_out')

    async def test_source_staleness_is_not_refreshed_by_a_new_http_response(self):
        stale = quote(self.clock, t=iso_utc(self.clock.now().utc_ns-10*SECOND))
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={'symbol': 'ZZTEST', 'quote': stale}))) as client:
            result = await MarketInputs(client, calendar=self.calendar, clock=self.clock.now)('ZZTEST')
        self.assertIn('quote_not_fresh', result['reason_codes'])
        self.assertEqual(result['source_utc'], stale['t'])

    async def test_regular_quote_alone_exposes_open_asof_without_claiming_tradable(self):
        async def respond(request):
            return metadata_response(request) if '/meta/' in request.url.path else httpx.Response(200, json={'symbol': 'ZZTEST', 'quote': quote(self.clock)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider.prewarm()
            result = await provider('ZZTEST')
        self.assertTrue(result['open_quote_asof_timestamp'])
        self.assertEqual(result['market_status'], 'open_quote_asof_timestamp')
        self.assertEqual(result['halt_state'], 'unknown')

    async def test_wrong_currency_or_venue_cannot_look_verified(self):
        for currency, change in (('EUR', {}), ('USD', {'ax': ' '})):
            async def respond(request):
                return metadata_response(request) if '/meta/' in request.url.path else httpx.Response(200, json={'symbol': 'ZZTEST', 'currency': currency, 'quote': quote(self.clock, **change)})
            async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
                await provider.prewarm(); establish_status(provider.status, self.clock)
                result = await provider('ZZTEST')
            self.assertFalse(result['coverage_verified'])
            self.assertTrue(result['reason_codes'])

    async def test_nonfirm_conditions_and_iex_partial_coverage_are_rejected(self):
        async def respond(request):
            return metadata_response(request) if '/meta/' in request.url.path else httpx.Response(200, json={'symbol': 'ZZTEST', 'quote': quote(self.clock, c=['N'])})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now, feed='iex')
            await provider.prewarm()
            result = await provider('ZZTEST')
        self.assertFalse(result['conditions_eligible'])
        self.assertFalse(result['coverage_verified'])

    async def test_rate_limit_cooldown_does_not_queue_or_retry(self):
        requests = []
        async def respond(request):
            requests.append(request)
            return httpx.Response(429, headers={'retry-after': '30'})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider('ZZTEST')
            result = await provider('ZZTEST')
        self.assertEqual(len(requests), 1)
        self.assertIn('market_rate_limit_cooldown', result['reason_codes'])

    async def test_response_completing_after_original_deadline_cannot_look_eligible(self):
        async def respond(request):
            self.clock.advance(5*SECOND)
            return httpx.Response(200, json={'symbol': 'ZZTEST', 'quote': quote(self.clock)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await MarketInputs(client, calendar=self.calendar, clock=self.clock.now)('ZZTEST', deadline_mono_ns=self.clock.now().mono_ns+5*SECOND)
        self.assertEqual(result['status'], 'deadline_expired')
        self.assertFalse(result['coverage_verified'])
        self.assertFalse(result['eligible_for_execution_checks'])
        self.assertIn('five_second_deadline_expired_during_market_request', result['reason_codes'])

    async def test_changed_condition_dictionary_is_not_accepted_by_code_alone(self):
        async def respond(request):
            if '/meta/' in request.url.path:
                return httpx.Response(200, json={'R': 'SYNTHETIC unexpected meaning'})
            return httpx.Response(200, json={'symbol': 'ZZTEST', 'quote': quote(self.clock)})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            provider = MarketInputs(client, calendar=self.calendar, clock=self.clock.now)
            await provider.prewarm(); establish_status(provider.status, self.clock)
            result = await provider('ZZTEST')
        self.assertFalse(result['conditions_eligible'])
        self.assertIn('quote_condition_metadata_not_prepared_or_changed', result['reason_codes'])

    def test_early_close_extended_window_not_silently_assumed_normal(self):
        clock = Clock('2026-11-27T19:00:00Z')
        provider = MarketInputs(None, calendar=self.calendar, clock=clock.now)
        session, supported, reason = provider.broker_session(clock.now())
        self.assertEqual(session, 'after_hours')
        self.assertFalse(supported)
        self.assertEqual(reason, 'early_close_extended_broker_window_not_verified')
