import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from recorder.collectors import run_metadata
from recorder.journal import Journal, read_rows
from recorder.report import quote_at
from recorder.snapshots import QuoteSnapshots
from recorder.timebase import Stamp, iso_utc


class Calendar:
    def session(self, _):
        return "after_hours"


class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)/"snapshot.sqlite"
        self.journal = Journal(self.path, run_metadata(["ZZTEST"], "iex"))
        self.auth = patch("recorder.collectors.alpaca_headers", return_value={"APCA-API-KEY-ID": "SYNTHETIC", "APCA-API-SECRET-KEY": "SYNTHETIC"})
        self.auth.start()

    async def asyncTearDown(self):
        self.auth.stop(); self.journal.close(); self.temp.cleanup()

    async def collect(self, responses, budget=120):
        requests = []
        async def respond(request):
            requests.append(request)
            return responses[len(requests)-1]
        receipt = Stamp.now()
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            collector = QuoteSnapshots(self.journal, client, "iex", Calendar(), requests_per_minute=budget)
            collector.enqueue("event", "ZZTEST", receipt)
            worker = asyncio.create_task(collector.worker())
            await collector.pending.join()
            worker.cancel(); await asyncio.gather(worker, return_exceptions=True)
        self.journal.close()
        return receipt, requests, list(read_rows(self.path))

    async def test_snapshot_is_available_only_after_its_actual_response(self):
        quote = {"t": iso_utc(Stamp.now().utc_ns), "bp": 10.0, "ap": 10.02, "bs": 2, "as": 3}
        receipt, requests, rows = await self.collect([httpx.Response(200, json={"quotes": {"ZZTEST": quote}})])
        q = [r for r in rows if r["kind"] == "quote"][0]
        self.assertEqual(q["data"]["observation_method"], "rest_latest_snapshot")
        self.assertEqual(quote_at([q], receipt, "iex", False)["status"], "missing")
        after = quote_at([q], q["stamp"], "iex", False)
        self.assertEqual(after["ask"], "10.02")
        self.assertFalse(after["eligible_for_execution_research"])
        self.assertEqual(requests[0].method, "GET")
        self.assertEqual(requests[0].url.params["feed"], "iex")

    async def test_denied_snapshot_does_not_fallback_or_log_error_body(self):
        _, requests, rows = await self.collect([httpx.Response(403, text="echo SECRET must not be logged")])
        self.assertEqual(len(requests), 1)
        self.assertFalse(any(r["kind"] == "quote" for r in rows))
        self.assertNotIn("SECRET", json.dumps([r["data"] for r in rows]))

    async def test_exhausted_request_budget_is_visible_without_requesting(self):
        _, requests, rows = await self.collect([], budget=0)
        self.assertEqual(requests, [])
        self.assertEqual([r for r in rows if r["kind"] == "quote_snapshot_result"][0]["data"]["status"], "request_budget_exhausted")

    async def test_missing_quote_stays_missing(self):
        _, _, rows = await self.collect([httpx.Response(200, json={"quotes": {}})])
        self.assertFalse(any(r["kind"] == "quote" for r in rows))
        self.assertEqual([r for r in rows if r["kind"] == "quote_snapshot_result"][0]["data"]["status"], "missing_quote")

    async def test_rate_limit_cooldown_blocks_following_requests(self):
        requests = []
        async def respond(request):
            requests.append(request)
            return httpx.Response(429, headers={"retry-after": "60"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            collector = QuoteSnapshots(self.journal, client, "iex", Calendar())
            collector.enqueue("one", "ZZTEST", Stamp.now())
            collector.enqueue("two", "ZZTEST", Stamp.now())
            worker = asyncio.create_task(collector.worker())
            await collector.pending.join()
            worker.cancel(); await asyncio.gather(worker, return_exceptions=True)
        self.journal.close()
        self.assertEqual(len(requests), 1)
        rows = [r["data"] for r in read_rows(self.path) if r["kind"] == "quote_snapshot_result"]
        self.assertEqual(rows[1]["status"], "rate_limit_cooldown")
