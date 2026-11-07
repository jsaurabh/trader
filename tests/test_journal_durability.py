"""Network-free tests for immutable observations and actual commit acknowledgements."""
import asyncio
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from recorder.journal import Journal, read_rows
from recorder.timebase import SECOND, Stamp


class WriterGate:
    """Hold one writer operation without delaying the event loop or reader DB."""
    def __init__(self, *, phase="commit", kind="_durability_barrier", fail=False):
        self.phase, self.kind, self.fail = phase, kind, fail
        self.entered, self.release = threading.Event(), threading.Event()
        self.fired = False
        self.original_connect = sqlite3.connect

    def connect(self, *args, **kwargs):
        return GatedConnection(self.original_connect(*args, **kwargs), self)

    def wait(self):
        self.fired = True
        self.entered.set()
        if not self.release.wait(5):
            raise RuntimeError("fabricated writer gate was not released")
        if self.fail:
            raise sqlite3.OperationalError("fabricated persistence failure")


class GatedConnection:
    def __init__(self, inner, gate):
        self.inner, self.gate, self.pending_target = inner, gate, False

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def executemany(self, sql, rows):
        rows = list(rows)
        target = any(row[1] == self.gate.kind for row in rows)
        if target and self.gate.phase == "write" and not self.gate.fired:
            self.gate.wait()
        self.pending_target = self.pending_target or target
        return self.inner.executemany(sql, rows)

    def commit(self):
        if self.pending_target and self.gate.phase == "commit" and not self.gate.fired:
            self.gate.wait()
        result = self.inner.commit()
        self.pending_target = False
        return result


class JournalDurabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/"synthetic-journal.sqlite"
        self.journal, self.gate = None, None

    async def asyncTearDown(self):
        if self.gate is not None:
            self.gate.release.set()
        if self.journal is not None:
            try:
                await asyncio.to_thread(self.journal.close)
            except RuntimeError:
                # Failure tests assert the same visible failure explicitly.
                pass
        self.tmp.cleanup()

    def create_gated(self, **kwargs):
        self.gate = WriterGate(**kwargs)
        with patch("recorder.journal.sqlite3.connect", self.gate.connect):
            self.journal = Journal(self.path, {"mode": "SYNTHETIC", "no_network": True})

    async def writer_is_held(self):
        self.assertTrue(await asyncio.to_thread(self.gate.entered.wait, 1))

    async def test_emission_freezes_nested_producer_data_before_queued_write(self):
        self.create_gated(phase="write", kind="hold_writer")
        self.journal.emit("hold_writer", {})
        await self.writer_is_held()
        bundle = {"analysis": {"eps": "1.23", "evidence": ["original passage"]}, "decision": "entry"}
        self.journal.emit("paper_admission_audit", bundle)
        bundle["analysis"]["eps"] = "999"
        bundle["analysis"]["evidence"].append("later mutation")
        bundle["decision"] = "changed"
        self.gate.release.set()
        self.assertTrue(await self.journal.wait_durable(Stamp.now().mono_ns+SECOND))
        stored = list(read_rows(self.path, kinds=("paper_admission_audit",)))[0]["data"]
        self.assertEqual(stored, {"analysis": {"eps": "1.23", "evidence": ["original passage"]}, "decision": "entry"})

    async def test_barrier_acknowledges_only_after_prior_bundle_commits(self):
        self.create_gated()
        self.journal.emit("paper_admission_audit", {"source_hash": "synthetic", "quote": {"ask": "50"}})
        task = asyncio.create_task(self.journal.wait_durable(Stamp.now().mono_ns+2*SECOND))
        await self.writer_is_held()
        self.assertFalse(task.done())
        # Earlier batches may already be committed. The barrier's own transaction
        # is still uncommitted, so it cannot yet acknowledge the FIFO prefix.
        self.assertEqual(list(read_rows(self.path, kinds=("_durability_barrier",))), [])
        self.gate.release.set()
        self.assertTrue(await task)
        rows = list(read_rows(self.path))
        self.assertIn("paper_admission_audit", [row["kind"] for row in rows])
        self.assertEqual(rows[-1]["kind"], "_durability_barrier")

    async def test_stalled_writer_cannot_acknowledge_after_original_deadline(self):
        self.create_gated()
        self.journal.emit("paper_admission_audit", {"fabricated": True})
        task = asyncio.create_task(self.journal.wait_durable(Stamp.now().mono_ns+SECOND//20))
        await self.writer_is_held()
        self.assertFalse(await task)
        self.assertEqual(self.journal._barriers, {})
        self.gate.release.set()

    async def test_late_commit_on_caller_clock_does_not_renew_deadline(self):
        self.create_gated()
        current = [Stamp.now()]
        original_deadline = current[0].mono_ns+5*SECOND
        task = asyncio.create_task(self.journal.wait_durable(original_deadline, clock=lambda: current[0]))
        await self.writer_is_held()
        current[0] = Stamp(current[0].utc_ns+6*SECOND, current[0].mono_ns+6*SECOND)
        self.gate.release.set()
        self.assertFalse(await task)

    async def test_writer_failure_wakes_waiter_and_fails_closed(self):
        self.create_gated(fail=True)
        task = asyncio.create_task(self.journal.wait_durable(Stamp.now().mono_ns+5*SECOND))
        await self.writer_is_held()
        self.gate.release.set()
        with self.assertRaisesRegex(RuntimeError, "journal failed"):
            await asyncio.wait_for(task, .5)
        self.assertEqual(self.journal._barriers, {})
        with self.assertRaisesRegex(RuntimeError, "journal failed"):
            await asyncio.to_thread(self.journal.close)

    async def test_cancellation_cleans_registered_barrier(self):
        self.create_gated()
        task = asyncio.create_task(self.journal.wait_durable(Stamp.now().mono_ns+5*SECOND))
        await self.writer_is_held()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.journal._barriers, {})


if __name__ == "__main__":
    unittest.main()
