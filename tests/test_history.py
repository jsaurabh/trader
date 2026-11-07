import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from recorder.collectors import run_metadata
from recorder.history import collect_history, source_quote_at, validate_window
from recorder.journal import Journal, read_rows
from recorder.report import build_report, write_report
from recorder.timebase import SECOND, parse_rfc3339

START = "2026-08-03T20:00:03.123456789Z"
ANCHOR = "2026-08-03T20:00:04.123456789Z"
END = "2026-08-03T20:00:10.123456789Z"
NOW = parse_rfc3339("2026-08-03T21:00:00Z")[0]


def quote(timestamp, ask="10.02"):
    return {"t": timestamp, "bp": "10.00", "ap": ask, "bs": 3, "as": 4, "bx": "Q", "ax": "Q", "c": ["R"]}


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "history.sqlite"
        self.j = Journal(self.db, {**run_metadata(["AAPL"], "sip"), "mode": "HISTORICAL"})
        self.auth = patch("recorder.history.alpaca_headers", return_value={"APCA-API-KEY-ID": "SYNTHETIC", "APCA-API-SECRET-KEY": "SYNTHETIC"})
        self.auth.start()

    async def asyncTearDown(self):
        self.auth.stop()
        self.j.close()
        self.temp.cleanup()

    async def call(self, responses, max_pages=5):
        requests = []
        async def respond(request):
            requests.append(request)
            return responses[len(requests)-1]
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await collect_history(self.j, ["AAPL"], START, END, ANCHOR, client=client, max_pages=max_pages)
        self.j.close()
        return result, requests, build_report(self.db)

    async def test_pagination_preserves_exact_time_and_never_creates_live_observations(self):
        responses = [httpx.Response(200, json={"quotes": {"AAPL": [quote("2026-08-03T20:00:04.000000001Z")]}, "next_page_token": "opaque_cursor"}),
                     httpx.Response(200, json={"quotes": {"AAPL": [quote("2026-08-03T20:00:09.123456788Z", "10.12"),
                                                                           quote("2026-08-03T20:00:09.123456790Z", "99.99")]}, "next_page_token": None})]
        result, requests, report = await self.call(responses)
        self.assertTrue(result["complete"])
        self.assertEqual(requests[0].url.params["start"], START)
        self.assertEqual(requests[0].url.params["asof"], "2026-08-03")
        self.assertEqual(requests[1].url.params["page_token"], "opaque_cursor")
        self.assertTrue(all(r.method == "GET" and r.url.host == "data.alpaca.markets" for r in requests))
        self.assertEqual(report["events"], [])
        self.assertNotIn("quote", report["record_counts"])
        history = report["historical_quotes"][0]
        snapshots = history["snapshots"]["AAPL"]
        self.assertEqual(snapshots["at_anchor"]["ask"], "10.02")
        self.assertEqual(snapshots["at_anchor_plus_five_seconds"]["ask"], "10.12")
        self.assertFalse(snapshots["at_anchor"]["known_to_system_at_target"])
        self.assertFalse(snapshots["at_anchor"]["live_execution_eligible"])
        self.assertEqual(snapshots["at_anchor"]["source_precision_ns"], 1)

    async def test_empty_page_with_cursor_does_not_end_download(self):
        result, requests, _ = await self.call([
            httpx.Response(200, json={"quotes": {}, "next_page_token": "next"}),
            httpx.Response(200, json={"quotes": {"AAPL": [quote(ANCHOR)]}, "next_page_token": None})])
        self.assertEqual(len(requests), 2)
        self.assertEqual(result["quote_count"], 1)
        self.assertTrue(result["complete"])

    async def test_page_budget_reports_partial_download(self):
        result, requests, report = await self.call([
            httpx.Response(200, json={"quotes": {"AAPL": [quote(ANCHOR)]}, "next_page_token": "more"})], max_pages=1)
        self.assertEqual(result["status"], "page_budget_exhausted")
        self.assertFalse(result["complete"])
        self.assertTrue(result["more_pages"])
        self.assertFalse(report["historical_quotes"][0]["snapshots"]["AAPL"]["at_anchor"]["source_pages_complete"])

    async def test_access_denial_does_not_fallback_to_another_feed(self):
        result, requests, report = await self.call([httpx.Response(403, text="denied: do not log key=SECRET")])
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.params["feed"], "sip")
        self.assertEqual(result["status"], "access_denied")
        self.assertFalse(result["complete"])
        self.assertNotIn("SECRET", json.dumps(report))

    async def test_repeated_cursor_stops_without_claiming_completeness(self):
        page = httpx.Response(200, json={"quotes": {}, "next_page_token": "same"})
        result, requests, _ = await self.call([page, page])
        self.assertEqual(len(requests), 2)
        self.assertEqual(result["status"], "invalid_or_repeated_page_token")
        self.assertFalse(result["complete"])

    async def test_unexpected_schema_is_preserved_and_flagged(self):
        result, requests, report = await self.call([httpx.Response(200, json={"quotes": []})])
        self.assertEqual(result["status"], "unexpected_schema")
        raw = [r["data"]["raw"] for r in read_rows(self.db) if r["kind"] == "historical_page"]
        self.assertEqual(json.loads(raw[0]), {"quotes": []})

    async def test_missing_pagination_field_cannot_prove_completeness(self):
        result, _, _ = await self.call([httpx.Response(200, json={"quotes": {"AAPL": []}})])
        self.assertEqual(result["status"], "unexpected_schema")
        self.assertFalse(result["complete"])

    async def test_malformed_success_body_is_preserved_without_logging(self):
        result, _, _ = await self.call([httpx.Response(200, text="not JSON")])
        self.assertEqual(result["status"], "unexpected_schema")
        page = [r for r in read_rows(self.db) if r["kind"] == "historical_page"][0]
        self.assertEqual(page["data"]["raw"], "not JSON")

    async def test_history_report_clearly_labels_retrieval_time_domain(self):
        _, _, report = await self.call([httpx.Response(200, json={"quotes": {"AAPL": [quote(ANCHOR)]}, "next_page_token": None})])
        out, html = Path(self.temp.name)/"history.json", Path(self.temp.name)/"history.html"
        write_report(report, out, html)
        self.assertIn("HISTORICAL", html.read_text())
        self.assertIn("Known to system at past anchor", html.read_text())
        self.assertIn("Actual retrieval began", html.read_text())


class WindowTests(unittest.TestCase):
    def test_recent_history_is_rejected(self):
        end_ns, _ = parse_rfc3339(END)
        with self.assertRaisesRegex(ValueError, "sixteen minutes"):
            validate_window(["AAPL"], START, END, ANCHOR, end_ns + 15*60*SECOND)

    def test_window_must_cover_anchor_plus_five_seconds(self):
        with self.assertRaisesRegex(ValueError, "anchor plus five"):
            validate_window(["AAPL"], START, ANCHOR, ANCHOR, NOW)

    def test_bounded_window_and_symbol_budget(self):
        with self.assertRaisesRegex(ValueError, "fifteen minutes"):
            validate_window(["AAPL"], "2026-08-03T19:00:00Z", END, ANCHOR, NOW)
        with self.assertRaisesRegex(ValueError, "one to ten"):
            validate_window(["AAPL"]*11, START, END, ANCHOR, NOW)

    def test_no_future_or_prior_window_quotes_can_fill_missing_snapshot(self):
        target, _ = parse_rfc3339(ANCHOR)
        start, _ = parse_rfc3339(START)
        result = source_quote_at([quote("2026-08-03T20:00:04.123456790Z"), quote("2026-08-03T20:00:03.123456788Z")], target, start, True)
        self.assertEqual(result["status"], "missing")
        self.assertFalse(result["known_to_system_at_target"])

    def test_invalid_sizes_and_stale_quotes_are_visible(self):
        target, _ = parse_rfc3339(ANCHOR)
        start, _ = parse_rfc3339(START)
        q = {**quote(START), "bs": 0}
        result = source_quote_at([q], target+SECOND, start, True)
        self.assertEqual(result["status"], "invalid_quote")
        self.assertTrue(result["stale_over_one_second"])

    def test_conflicting_same_timestamp_quotes_are_visible(self):
        target, _ = parse_rfc3339(ANCHOR)
        start, _ = parse_rfc3339(START)
        result = source_quote_at([quote(ANCHOR), quote(ANCHOR, "10.10")], target, start, True)
        self.assertTrue(result["quote_state_ambiguous"])
        self.assertEqual(result["observations_at_same_source_timestamp"], 2)
        self.assertFalse(result["live_execution_eligible"])


if __name__ == "__main__":
    unittest.main()
