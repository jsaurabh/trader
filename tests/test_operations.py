import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from recorder.analysis import ModelRequestBudget, ModelResponseError
from recorder.collectors import Recorder, run_metadata
from recorder.journal import Journal, quote_window, read_rows, record_counts
from recorder.locking import CaptureAlreadyRunning, CaptureLock
from recorder.report import build_report
from recorder.timebase import SECOND, Stamp, iso_utc


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)/"capture.sqlite"

    def tearDown(self):
        self.temp.cleanup()

    def test_capture_lock_prevents_duplicate_collectors_and_releases(self):
        first = CaptureLock(self.path)
        try:
            with self.assertRaises(CaptureAlreadyRunning):
                CaptureLock(self.path)
        finally:
            first.close()
        second = CaptureLock(self.path)
        second.close()

    def test_resume_records_gap_and_does_not_claim_complete_replay(self):
        old = Journal(self.path, run_metadata(["ZZTEST"], "iex"))
        old.emit("run_end", {"reason": "controlled_stop"})
        old.close()
        journal = Journal(self.path, run_metadata(["ZZTEST"], "iex"))
        try:
            Recorder(journal, ["ZZTEST"])
        finally:
            journal.close()
        gaps = [r["data"] for r in read_rows(self.path, kinds=("gap",))]
        self.assertEqual(len(gaps), 1)
        self.assertTrue(gaps[0]["previous_shutdown_clean"])
        self.assertFalse(gaps[0]["replay_completeness_verified"])

    def test_model_request_cap_survives_restart(self):
        journal = Journal(self.path, run_metadata([], "iex"))
        budget = ModelRequestBudget(journal, limit=2)
        budget.claim("test-model"); budget.claim("test-model")
        with self.assertRaises(ModelResponseError):
            budget.claim("test-model")
        journal.close()
        journal = Journal(self.path, run_metadata([], "iex"))
        try:
            restored = ModelRequestBudget(journal, limit=2)
            with self.assertRaises(ModelResponseError):
                restored.claim("test-model")
        finally:
            journal.close()
        self.assertEqual(record_counts(self.path)["model_request_started"], 2)

    def test_request_budget_expires_only_after_rolling_day(self):
        journal = Journal(self.path, run_metadata([], "iex"))
        now = Stamp.now()
        clock = [now]
        try:
            budget = ModelRequestBudget(journal, limit=1, clock=lambda: clock[0])
            budget.claim("test-model")
            clock[0] = Stamp(now.utc_ns+86_400*SECOND+1, now.mono_ns+86_400*SECOND+1)
            budget.claim("test-model")
        finally:
            journal.close()

    def test_report_quote_query_keeps_stale_baseline_and_excludes_future(self):
        journal = Journal(self.path, run_metadata(["ZZTEST"], "iex"))
        receipt = Stamp.now()
        run = journal.run_id
        def emit(symbol, offset):
            stamp = Stamp(receipt.utc_ns+offset, receipt.mono_ns+offset)
            journal.emit("quote", {"feed": "iex", "raw": {"S": symbol, "t": iso_utc(stamp.utc_ns),
                                                         "bp": "10", "ap": "10.02", "bs": 1, "as": 1}}, stamp)
        emit("ZZTEST", -3600*SECOND)
        emit("OTHER", 0)
        emit("ZZTEST", 5*SECOND+1)
        journal.close()
        selected = quote_window(self.path, run, "ZZTEST", receipt.mono_ns, receipt.mono_ns+5*SECOND)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["stamp"].mono_ns, receipt.mono_ns-3600*SECOND)
        self.assertEqual(build_report(self.path)["record_counts"]["quote"], 3)


class ServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_unbounded_recorder_publishes_and_stops_cleanly_on_cancel(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"capture.sqlite"
            journal = Journal(path, run_metadata(["ZZTEST"], "iex"))
            recorder = Recorder(journal, ["ZZTEST"])
            published = asyncio.Event()
            loop = asyncio.get_running_loop()
            async def idle(*args):
                await asyncio.Event().wait()
            def publish():
                loop.call_soon_threadsafe(published.set)
            with patch.dict("os.environ", {"RTPR_API_KEY": "SYNTHETIC", "ALPACA_API_KEY": "SYNTHETIC", "ALPACA_API_SECRET": "SYNTHETIC"}), \
                    patch.object(recorder, "wire", idle), patch.object(recorder, "market", idle):
                task = asyncio.create_task(recorder.run(None, report_callback=publish))
                await asyncio.wait_for(published.wait(), 1)
                self.assertFalse(task.done())
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            journal.close()
            self.assertTrue(any(r["kind"] == "run_end" for r in read_rows(path)))
