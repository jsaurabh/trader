import asyncio
import base64
from contextlib import asynccontextmanager
import json
from pathlib import Path
import queue
import tempfile
import threading
import unittest
from unittest.mock import patch

import httpx

from recorder.collectors import Recorder, normalize_alert, run_metadata, safe_error
from recorder.documents import DocumentCollector, article_id, inspect_document
from recorder.journal import Journal, read_rows
from recorder.report import build_report, quote_at, write_report
from recorder.synthetic import make_demo
from recorder.timebase import DEADLINE_NS, MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339


class TimeTests(unittest.TestCase):
    def test_nanoseconds_and_precision_survive_offset_conversion(self):
        ns, precision = parse_rfc3339("2026-11-06T13:00:02.015070376-08:00")
        self.assertEqual(iso_utc(ns), "2026-11-06T21:00:02.015070376Z")
        self.assertEqual(precision, 1)
        self.assertEqual(parse_rfc3339("2026-11-06T21:00:02Z")[1], SECOND)

    def test_naive_timestamp_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_rfc3339("2026-11-06T13:00:02")

    def test_holidays_early_close_and_dst(self):
        cal = MarketCalendar(parse_rfc3339("2026-11-06T21:00:00Z")[0])
        cases = {"2026-07-03T16:00:00Z": "closed_non_trading_day",
                 "2026-11-27T17:59:59Z": "regular", "2026-11-27T18:00:00Z": "after_hours",
                 "2026-03-09T13:30:00Z": "regular", "2026-11-02T14:29:59Z": "pre_market"}
        for timestamp, expected in cases.items():
            with self.subTest(timestamp=timestamp):
                self.assertEqual(cal.session(parse_rfc3339(timestamp)[0]), expected)


class DocumentTests(unittest.TestCase):
    def test_opaque_id_does_not_change_with_signature(self):
        self.assertEqual(article_id("https://rtpr.io/a/arbitrarily-long_20261106?sig=one&exp=1"),
                         article_id("https://rtpr.io/a/arbitrarily-long_20261106?exp=2&sig=two"))

    def test_article_links_cannot_select_other_hosts_or_ports(self):
        for url in ["https://evil.example/a/id", "http://rtpr.io/a/id", "https://rtpr.io:444/a/id", "https://secret@rtpr.io/a/id"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                article_id(url)

    def test_lossless_tables_are_candidates_not_verified_full_text(self):
        body = b'<article><script>ignore_me()</script><table><tr><th>Revenue</th><td>$123</td></tr></table></article>'
        result = inspect_document(body, "text/html")
        self.assertEqual(base64.b64decode(result["raw_body_base64"]), body)
        self.assertTrue(result["contains_html_tables"])
        self.assertIn("Revenue\t$123", result["candidate_article_text"])
        self.assertNotIn("ignore_me", result["candidate_article_text"])
        self.assertFalse(result["full_text_verified"])

    def test_shell_html_does_not_become_a_release(self):
        result = inspect_document(b'<main>Sign in to view the release</main>', "text/html")
        self.assertIsNone(result["candidate_article_text"])
        self.assertFalse(result["full_text_verified"])

    def test_exception_text_is_not_logged(self):
        result = safe_error(ValueError("https://ws.rtpr.io/?apiKey=SECRET"))
        self.assertNotIn("SECRET", json.dumps(result))


class ReportingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "synthetic.sqlite"
        make_demo(self.path)
        self.report = build_report(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_synthetic_data_cannot_be_mistaken_for_observed(self):
        self.assertEqual(self.report["runs"][0]["mode"], "SYNTHETIC")
        self.assertEqual(self.report["events"][0]["mode"], "SYNTHETIC")
        self.assertFalse(self.report["orders_enabled"])

    def test_receipt_and_five_seconds_exclude_future_quote(self):
        event = self.report["events"][0]
        self.assertEqual(event["publication_to_receipt_ns"], 4 * SECOND)
        self.assertEqual(event["quote_at_receipt"]["ask"], "10.02")
        self.assertEqual(event["quote_at_five_seconds"]["ask"], "10.12")
        self.assertEqual(event["five_second_target_utc"], "2026-08-03T20:00:09.000000000Z")
        self.assertFalse(event["quote_at_five_seconds"]["eligible_for_execution_research"])

    def test_duplicates_and_corrections_preserve_first_receipt(self):
        event = self.report["events"][0]
        self.assertEqual(event["repeat_alert_count"], 2)
        self.assertEqual(event["exact_duplicate_count"], 1)
        self.assertEqual(event["observed_document_versions"], 2)
        self.assertEqual(event["document_content_changes"], 1)
        self.assertEqual(event["receipt_utc"], "2026-08-03T20:00:04.000000000Z")
        self.assertIsNone(event["full_text_available_utc"])
        self.assertEqual(event["explicit_correction_claims"], [{"correction": True, "version": 2}])

    def test_stale_and_crossed_quotes_are_visible(self):
        rows = [r for r in read_rows(self.path) if r["kind"] == "quote"]
        target = Stamp(rows[0]["stamp"].utc_ns + 2*SECOND, rows[0]["stamp"].mono_ns + 2*SECOND)
        quote = quote_at(rows, target, "iex", True)
        self.assertEqual(quote["status"], "stale")
        rows[0]["data"]["raw"]["bp"] = "20"
        quote = quote_at(rows[:1], rows[0]["stamp"], "iex", True)
        self.assertIn("invalid_crossed_or_empty_quote", quote["issues"])

    def test_missing_unsubscribed_quotes_stay_missing(self):
        quote = quote_at([], Stamp(1, 1), "sip", False)
        self.assertEqual(quote["status"], "missing")
        self.assertEqual(quote["reason"], "not_subscribed_before_alert")

    def test_unfinished_window_is_not_reported_as_observed(self):
        path = Path(self.temp.name) / "incomplete.sqlite"
        j = Journal(path, run_metadata(["AAPL"], "iex"))
        j.emit("alert", normalize_alert({"type": "alert", "ticker": "AAPL", "article_url": "https://rtpr.io/a/incomplete"}))
        j.close()
        event = build_report(path)["events"][0]
        self.assertFalse(event["five_second_window_observed"])
        self.assertEqual(event["quote_at_five_seconds"]["status"], "window_not_observed")
        self.assertNotIn("end_utc", build_report(path)["runs"][0])

    def test_export_contains_no_signed_urls_and_escapes_html(self):
        self.report["events"][0]["source_wire"] = '<script>alert("untrusted")</script>'
        output, html = Path(self.temp.name)/"report.json", Path(self.temp.name)/"report.html"
        write_report(self.report, output, html)
        self.assertNotIn("<script>", html.read_text())
        self.assertIn("&lt;script&gt;", html.read_text())
        self.assertNotIn("article_url", output.read_text())


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "observed.sqlite"
        self.j = Journal(self.path, run_metadata(["AAPL"], "iex"))

    async def asyncTearDown(self):
        self.j.close()
        self.temp.cleanup()

    async def test_redirect_to_login_is_unavailable_without_forwarding_secrets(self):
        requests = []
        async def respond(request):
            requests.append(request)
            return httpx.Response(307, headers={"location": "/sign-in?redirect_url=signed-secret"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            collector = DocumentCollector(self.j, client)
            collector.enqueue("opaque", "https://rtpr.io/a/opaque?sig=SIGNED", Stamp.now())
            worker = asyncio.create_task(collector.worker())
            await collector.pending.join()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self.j.close()
        doc = [r for r in read_rows(self.path) if r["kind"] == "document"][0]["data"]
        self.assertEqual(doc["status"], "authentication_required")
        self.assertFalse(doc["full_text_verified"])
        self.assertEqual(len(requests), 1)
        self.assertNotIn("authorization", requests[0].headers)
        self.assertEqual(requests[0].url.query, b"sig=SIGNED")
        self.assertNotIn("signed-secret", json.dumps(doc))

    async def test_slow_documents_do_not_block_wire_or_reset_duplicate_deadline(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def respond(request):
            entered.set()
            await release.wait()
            return httpx.Response(200, text="<article>SYNTHETIC TEST</article>", headers={"content-type": "text/html"})
        r = Recorder(self.j, ["AAPL"])
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            documents = DocumentCollector(self.j, client)
            worker = asyncio.create_task(documents.worker())
            msg = {"type": "alert", "ticker": "AAPL", "article_url": "https://rtpr.io/a/same", "rules": []}
            first = Stamp.now()
            r.handle_wire(json.dumps(msg), first, documents)
            await entered.wait()
            later = Stamp(first.utc_ns+6*SECOND, first.mono_ns+6*SECOND)
            r.handle_wire(json.dumps(msg), later, documents)
            self.assertEqual(r.handle_wire('{"type":"ping"}', later, documents), "ping")
            self.assertEqual(len(r.checkpoints), 1)
            release.set()
            await documents.pending.join()
            worker.cancel()
            for task in r.checkpoints:
                task.cancel()
            await asyncio.gather(worker, *r.checkpoints, return_exceptions=True)
        self.j.close()
        alerts = [x["data"] for x in read_rows(self.path) if x["kind"] == "alert"]
        self.assertEqual(alerts[0]["deadline_mono_ns"], first.mono_ns + DEADLINE_NS)
        self.assertEqual(alerts[1]["deadline_mono_ns"], alerts[0]["deadline_mono_ns"])
        self.assertTrue(alerts[1]["research_only_repeat"])

    async def test_restart_does_not_assign_a_fresh_entry_deadline(self):
        alert = normalize_alert({"type": "alert", "ticker": "AAPL", "article_url": "https://rtpr.io/a/prior"})
        first = Stamp.now()
        self.j.emit("alert", alert, first)
        self.j.close()
        self.j = Journal(self.path, run_metadata(["AAPL"], "iex"))
        r = Recorder(self.j, ["AAPL"])
        class Docs:
            def enqueue(self, *args, **kwargs):
                self.comparable = kwargs["deadline_comparable"]
        documents = Docs()
        r.handle_wire(json.dumps({"type": "alert", "ticker": "AAPL", "article_url": "https://rtpr.io/a/prior?sig=NEW"}), Stamp.now(), documents)
        self.j.close()
        alerts = [x["data"] for x in read_rows(self.path) if x["kind"] == "alert"]
        self.assertTrue(alerts[-1]["research_only_repeat"])
        self.assertIsNone(alerts[-1]["deadline_mono_ns"])
        self.assertEqual(alerts[-1]["first_deadline_utc_ns"], first.utc_ns + DEADLINE_NS)
        self.assertFalse(documents.comparable)
        self.assertEqual(len(r.checkpoints), 0)

    async def test_full_document_queue_records_loss(self):
        async with httpx.AsyncClient() as client:
            d = DocumentCollector(self.j, client, capacity=1)
            d.enqueue("first", "https://rtpr.io/a/first", Stamp.now())
            d.enqueue("second", "https://rtpr.io/a/second", Stamp.now())
        self.j.close()
        docs = [r["data"] for r in read_rows(self.path) if r["kind"] == "document"]
        self.assertEqual(docs[0]["status"], "queue_overflow")

    async def test_socket_heartbeat_and_disconnect_use_pong_and_backoff(self):
        r = Recorder(self.j, ["AAPL"])
        sent, delays = [], []
        class Socket:
            frames = iter(['{"type":"connected"}', '{"type":"ping"}'])
            async def recv(self):
                try:
                    return next(self.frames)
                except StopIteration:
                    raise ConnectionError("do not log apiKey=SYNTHETIC_SECRET")
            async def send(self, text):
                sent.append(text)
        @asynccontextmanager
        async def connection(*args, **kwargs):
            yield Socket()
        async def end_after_retry_logged(delay):
            delays.append(delay)
            raise asyncio.CancelledError
        with patch.dict("os.environ", {"RTPR_API_KEY": "SYNTHETIC_SECRET"}), \
                patch("recorder.collectors.connect", connection), \
                patch("recorder.collectors.backoff", end_after_retry_logged):
            with self.assertRaises(asyncio.CancelledError):
                await r.wire(None)
        self.j.close()
        rows = list(read_rows(self.path))
        self.assertEqual(sent, ['{"type":"pong"}'])
        self.assertGreaterEqual(delays[0], 2)
        self.assertTrue(any(x["kind"] == "gap" and x["data"]["reason"] == "disconnected_no_replay_guarantee" for x in rows))
        self.assertNotIn("SYNTHETIC_SECRET", json.dumps([x["data"] for x in rows]))


class JournalTests(unittest.TestCase):
    def test_private_database_and_lossless_raw_frame(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"capture.sqlite"
            j = Journal(path, {"mode": "OBSERVED"})
            raw = '{ "type": "alert", "article_url": "https://rtpr.io/a/id?sig=SENSITIVE" }'
            j.emit("wire_frame", {"raw": raw}, Stamp(10, 20))
            j.close()
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            row = [r for r in read_rows(path) if r["kind"] == "wire_frame"][0]
            self.assertEqual(row["data"]["raw"], raw)
            self.assertEqual(row["stamp"], Stamp(10, 20))

    def test_producer_fails_visibly_on_backpressure(self):
        # Simulate a stalled disk writer without relying on scheduler races.
        instance = object.__new__(Journal)
        instance.failure = None
        instance._barriers, instance._barriers_lock = {}, threading.Lock()
        instance.queue = queue.Queue(maxsize=1)
        instance.emit("one", {})
        with self.assertRaisesRegex(RuntimeError, "overflow"):
            instance.emit("two", {})
        with self.assertRaisesRegex(RuntimeError, "journal failed"):
            instance.emit("three", {})


if __name__ == "__main__":
    unittest.main()
