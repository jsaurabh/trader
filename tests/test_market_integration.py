"""Fabricated sockets/HTTP prove one-connection market integration and startup order."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from recorder.collectors import Recorder, run_metadata
from recorder.journal import Journal, read_rows
from recorder.market_inputs import MarketInputs, StatusBook
from recorder.timebase import SECOND, Stamp, iso_utc


class PreparedAdapter:
    def __init__(self, results=None, failure=False):
        self.status = StatusBook()
        self.results = results or [{'tape': t, 'status': 'prepared'} for t in 'ABC']
        self.failure, self.calls = failure, 0
    async def prewarm(self):
        self.calls += 1
        if self.failure:
            raise RuntimeError('SYNTHETIC_SENSITIVE_FAILURE_TEXT')
        return self.results


class MarketIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'synthetic-transport-observation.sqlite'
        self.journal = Journal(self.path, run_metadata(['ZZTEST'], 'sip'))
        self.env = patch.dict('os.environ', {'ALPACA_API_KEY': 'SYNTHETIC', 'ALPACA_API_SECRET': 'SYNTHETIC', 'RTPR_API_KEY': 'SYNTHETIC'})
        self.env.start()
    async def asyncTearDown(self):
        self.journal.close()
        self.env.stop()
        self.tmp.cleanup()

    async def test_run_uses_shared_http_and_prewarms_before_both_existing_sockets(self):
        recorder = Recorder(self.journal, ['ZZTEST'], feed='sip')
        calls, clients, sent = [], [], []
        stock_ready, wire_ready = asyncio.Event(), asyncio.Event()
        real_client = httpx.AsyncClient
        async def http_response(request):
            self.assertIn('/meta/conditions/quote', request.url.path)
            tape = request.url.params['tape']
            calls.append('metadata-'+tape)
            return httpx.Response(200, json={'R': 'Regular Two Sided Open' if tape == 'C' else 'Regular Market Maker Open'})
        def client_factory(*args, **kwargs):
            client = real_client(*args, **kwargs, transport=httpx.MockTransport(http_response))
            clients.append(client)
            return client
        class Socket:
            def __init__(self, stock):
                self.stock, self.index = stock, 0
            async def send(self, raw):
                sent.append((self.stock, json.loads(raw)))
            async def recv(self):
                self.index += 1
                now = Stamp.now()
                if not self.stock:
                    if self.index == 1:
                        wire_ready.set()
                        return '{"type":"connected"}'
                    await asyncio.Event().wait()
                if self.index == 1:
                    return '[{"T":"success","msg":"authenticated"}]'
                if self.index == 2:
                    return '[{"T":"subscription","quotes":["ZZTEST"],"trades":["ZZTEST"],"statuses":["ZZTEST"]}]'
                if self.index == 3:
                    return json.dumps([{'T': 's', 'S': 'ZZTEST', 'sc': 'T', 'z': 'C', 't': iso_utc(now.utc_ns)}])
                if self.index == 4:
                    return json.dumps([{'T': 'q', 'S': 'ZZTEST', 'bp': 10, 'ap': 10.02, 'bs': 80, 'as': 200,
                                        'bx': 'Q', 'ax': 'Q', 'c': ['R'], 'z': 'C', 't': iso_utc(now.utc_ns)}])
                stock_ready.set()
                await asyncio.Event().wait()
        @asynccontextmanager
        async def connection(url, **kwargs):
            self.assertEqual(set(recorder.market_inputs.conditions), set('ABC'))
            stock = url.startswith('wss://stream.data.alpaca.markets/')
            calls.append('stock_socket' if stock else 'wire_socket')
            yield Socket(stock)
        with patch('recorder.collectors.httpx.AsyncClient', client_factory), patch('recorder.collectors.connect', connection):
            running = asyncio.create_task(recorder.run(None))
            try:
                await asyncio.wait_for(asyncio.gather(stock_ready.wait(), wire_ready.wait()), 1)
                self.assertIs(recorder.market_inputs.client, clients[0])
                self.assertEqual(recorder.market_inputs.status.snapshot('ZZTEST', Stamp.now(), 'sip')['market_status'], 'tradable')
            finally:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
        self.assertEqual(calls[:3], ['metadata-A', 'metadata-B', 'metadata-C'])
        self.assertEqual(calls.count('stock_socket'), 1)
        self.assertEqual(calls.count('wire_socket'), 1)
        subscribe = [body for stock, body in sent if stock and body.get('action') == 'subscribe'][0]
        self.assertEqual(subscribe['statuses'], ['ZZTEST'])
        self.assertIsNone(recorder.market_inputs.status.connected_at)
        self.journal.close()
        quotes = list(read_rows(self.path, kinds=('quote',)))
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0]['data']['raw']['ap'], '10.02')

    async def test_prewarm_failure_releases_wire_with_gap_without_logging_failure_text(self):
        adapter = PreparedAdapter(failure=True)
        recorder = Recorder(self.journal, ['ZZTEST'], feed='sip', market_inputs=adapter)
        recorder.market_metadata_ready.clear()
        connected = asyncio.Event()
        class Socket:
            async def recv(self):
                connected.set()
                await asyncio.Event().wait()
        @asynccontextmanager
        async def connection(*args, **kwargs):
            yield Socket()
        with patch('recorder.collectors.connect', connection):
            running = asyncio.create_task(recorder.wire(None))
            try:
                await asyncio.sleep(0)
                self.assertFalse(connected.is_set())
                await recorder.prepare_market_inputs()
                await asyncio.wait_for(connected.wait(), 1)
            finally:
                running.cancel(); await asyncio.gather(running, return_exceptions=True)
        self.journal.close()
        records = list(read_rows(self.path))
        self.assertTrue(any(r['data'].get('reason') == 'condition_metadata_prewarm_failed' for r in records))
        self.assertNotIn('SYNTHETIC_SENSITIVE_FAILURE_TEXT', json.dumps([r['data'] for r in records]))

    async def test_partial_metadata_is_recorded_as_unprepared_not_verified(self):
        adapter = PreparedAdapter([{'tape': 'A', 'status': 'prepared'}, {'tape': 'B', 'status': 'http_error', 'http_status': 403}, {'tape': 'C', 'status': 'prepared'}])
        recorder = Recorder(self.journal, ['ZZTEST'], market_inputs=adapter)
        recorder.market_metadata_ready.clear()
        await recorder.prepare_market_inputs()
        self.assertTrue(recorder.market_metadata_ready.is_set())
        self.journal.close()
        gaps = [r['data'] for r in read_rows(self.path, kinds=('gap',))]
        self.assertEqual(gaps[0]['missing_tapes'], ['B'])
        prewarm = list(read_rows(self.path, kinds=('market_input_prewarm',)))[0]['data']
        self.assertFalse(prewarm['complete'])
        self.assertFalse(prewarm['status_baseline_established'])

    async def test_market_capture_preserves_receipt_and_routes_subscription_status_quote(self):
        recorder = Recorder(self.journal, ['ZZTEST'], feed='sip')
        provider = MarketInputs(None, calendar=recorder.calendar, feed='sip')
        recorder.market_inputs = provider
        first = Stamp.now()
        provider.status.connected('sip', first)
        recorder.handle_market('[{"T":"subscription","quotes":["ZZTEST"],"trades":["ZZTEST"],"statuses":["ZZTEST"]}]', first)
        source = Stamp(first.utc_ns+1, first.mono_ns+1)
        raw = json.dumps([{'T': 's', 'S': 'ZZTEST', 'sc': 'T', 'z': 'C', 't': iso_utc(source.utc_ns)},
                          {'T': 'q', 'S': 'ZZTEST', 'bp': '10', 'ap': '10.02', 'bs': 80, 'as': 200,
                           'bx': 'Q', 'ax': 'Q', 'c': ['R'], 'z': 'C', 't': iso_utc(source.utc_ns)}])
        recorder.handle_market(raw, source)
        self.assertEqual(provider.status.snapshot('ZZTEST', source, 'sip')['market_status'], 'tradable')
        recorder.handle_market('SYNTHETIC INVALID FRAME', source)
        self.assertIsNone(provider.status.connected_at)
        self.journal.close()
        recorded = list(read_rows(self.path, kinds=('quote', 'market_status')))
        self.assertEqual([r['kind'] for r in recorded], ['market_status', 'quote'])
        self.assertTrue(all(r['stamp'] == source for r in recorded))
        self.assertEqual(recorded[-1]['data']['raw']['ap'], '10.02')

    async def test_missing_status_ack_keeps_quote_capture_but_records_partial_capability(self):
        adapter = PreparedAdapter()
        recorder = Recorder(self.journal, ['ZZTEST'], feed='sip', market_inputs=adapter)
        sent = []
        class Socket:
            def __init__(self):
                self.index = 0
            async def send(self, raw):
                sent.append(json.loads(raw))
            async def recv(self):
                self.index += 1
                if self.index == 1:
                    return '[{"T":"success","msg":"authenticated"}]'
                if self.index == 2:
                    return '[{"T":"subscription","quotes":["ZZTEST"],"trades":["ZZTEST"],"statuses":[]}]'
                raise ConnectionError('SYNTHETIC socket end')
        @asynccontextmanager
        async def connection(*args, **kwargs):
            yield Socket()
        async def stop_after_gap(delay):
            raise asyncio.CancelledError
        with patch('recorder.collectors.connect', connection), patch('recorder.collectors.backoff', stop_after_gap):
            with self.assertRaises(asyncio.CancelledError):
                await recorder.market()
        self.journal.close()
        transport = [r['data'] for r in read_rows(self.path, kinds=('transport',)) if r['data'].get('state') == 'partial_subscription']
        self.assertEqual(len(transport), 1)
        self.assertTrue(transport[0]['quote_trade_subscription_confirmed'])
        self.assertFalse(transport[0]['status_subscription_confirmed'])
        self.assertTrue(any(r['data'].get('reason') == 'trading_action_subscription_not_confirmed' for r in read_rows(self.path, kinds=('gap',))))
        self.assertIsNone(adapter.status.connected_at)
