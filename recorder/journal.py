"""Append-only SQLite journal with bounded producers and FIFO commit barriers."""
import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import queue
import sqlite3
import threading
import uuid

from .timebase import SECOND, Stamp


_DURABILITY_BARRIER = "_durability_barrier"


class Journal:
    def __init__(self, path: str | Path, metadata: dict, capacity: int = 50_000, started: Stamp | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Open private before SQLite creates its WAL. The CLI also sets umask 077.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        self.run_id = str(uuid.uuid4())
        self.mode = metadata.get("mode", "UNKNOWN")
        self.queue = queue.Queue(maxsize=capacity)
        self.failure = None
        self._barriers = {}
        self._barriers_lock = threading.Lock()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._write, name="journal-writer", daemon=True)
        self.thread.start()
        self.ready.wait()
        self.check()
        self.emit("run_start", metadata, started)

    def check(self):
        if self.failure is not None:
            raise RuntimeError("journal failed; recording stopped") from None

    def emit(self, kind: str, data: dict, stamp: Stamp | None = None):
        self.check()
        stamp = stamp or Stamp.now()
        # Queue the raw frame, before parsing or document retrieval.
        # Producers retain mutable analysis/decision dictionaries. Freeze them here,
        # rather than allowing a later mutation to rewrite an earlier observation.
        try:
            self.queue.put_nowait((kind, stamp.utc_ns, stamp.mono_ns, deepcopy(data)))
        except queue.Full:
            self._fail("journal_queue_overflow")
            raise RuntimeError("journal queue overflow; recording stopped") from None

    def _fail(self, reason):
        # Wake all barriers after publishing failure, so no waiter mistakes an
        # error wakeup for a successful FULL commit.
        with self._barriers_lock:
            self.failure = reason
            for barrier in self._barriers.values():
                barrier.set()

    async def wait_durable(self, deadline_mono_ns, *, clock=Stamp.now) -> bool:
        """Commit all preceding emissions within the caller's original deadline.

        The marker is ordinary FIFO journal data. Its event is acknowledged only
        after the SQLite FULL commit containing it, never when it is dequeued.
        A writer error raises through check(); an expired deadline returns False.
        """
        self.check()
        if clock().mono_ns >= deadline_mono_ns or not self.thread.is_alive():
            return False
        token, barrier = str(uuid.uuid4()), threading.Event()
        with self._barriers_lock:
            self.check()
            self._barriers[token] = barrier
        try:
            self.emit(_DURABILITY_BARRIER, {"token": token, "mode": self.mode}, clock())
            self.check()
            remaining = (deadline_mono_ns-clock().mono_ns)/SECOND
            if remaining <= 0:
                return False
            try:
                # asyncio's timeout also counts time awaiting a worker slot.
                async with asyncio.timeout(remaining):
                    committed = await asyncio.to_thread(barrier.wait, remaining)
            except TimeoutError:
                self.check()
                return False
            self.check()
            return committed and clock().mono_ns < deadline_mono_ns
        finally:
            with self._barriers_lock:
                self._barriers.pop(token, None)
            # A canceled/timed-out asyncio waiter must not leave an executor
            # worker sleeping until its original timeout after registry cleanup.
            barrier.set()

    def _write(self):
        connection = None
        try:
            connection = sqlite3.connect(self.path)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("""CREATE TABLE IF NOT EXISTS journal (
                seq INTEGER PRIMARY KEY, run_id TEXT NOT NULL, kind TEXT NOT NULL,
                utc_ns INTEGER NOT NULL, mono_ns INTEGER NOT NULL, data TEXT NOT NULL)""")
            connection.execute("CREATE INDEX IF NOT EXISTS journal_run ON journal(run_id,seq)")
            connection.execute("CREATE INDEX IF NOT EXISTS journal_kind ON journal(kind)")
            connection.execute("""CREATE INDEX IF NOT EXISTS quote_symbol_time ON journal(
                run_id,json_extract(data,'$.raw.S'),mono_ns) WHERE kind='quote'""")
            connection.commit()
            self.ready.set()
            while True:
                item = self.queue.get()
                if item is None:
                    break
                batch = [item]
                stopping = False
                for _ in range(255):
                    try:
                        next_item = self.queue.get_nowait()
                    except queue.Empty:
                        break
                    if next_item is None:
                        stopping = True
                        break
                    batch.append(next_item)
                connection.executemany(
                    "INSERT INTO journal(run_id,kind,utc_ns,mono_ns,data) VALUES(?,?,?,?,?)",
                    [(self.run_id, k, u, m, json.dumps(d, separators=(",", ":"), allow_nan=False)) for k, u, m, d in batch],
                )
                connection.commit()
                with self._barriers_lock:
                    for kind, _, _, data in batch:
                        if kind == _DURABILITY_BARRIER:
                            barrier = self._barriers.get(data["token"])
                            if barrier is not None:
                                barrier.set()
                if stopping:
                    break
        except Exception as exc:
            self._fail(type(exc).__name__)
        finally:
            self.ready.set()
            if connection:
                connection.close()

    def close(self):
        if self.thread.is_alive():
            # Shutdown may wait for persistence; network ingestion has already ended.
            while self.thread.is_alive():
                try:
                    self.queue.put(None, timeout=0.1)
                    break
                except queue.Full:
                    pass
            self.thread.join()
        self.check()


def row_record(row):
    seq, run_id, kind, utc, mono, data = row
    return {"seq": seq, "run_id": run_id, "kind": kind, "stamp": Stamp(utc, mono), "data": json.loads(data)}


def read_rows(path: str | Path, kinds=None):
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        sql, params = "SELECT * FROM journal", ()
        if kinds:
            sql += " WHERE kind IN (" + ",".join("?" for _ in kinds) + ")"
            params = tuple(kinds)
        for row in connection.execute(sql + " ORDER BY seq", params):
            yield row_record(row)


def record_counts(path):
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        return dict(connection.execute("SELECT kind,COUNT(*) FROM journal GROUP BY kind"))


def previous_run_boundary(path, current_run_id):
    with sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro", uri=True) as connection:
        row = connection.execute("SELECT run_id,kind,utc_ns FROM journal WHERE run_id<>? ORDER BY seq DESC LIMIT 1",
                                 (current_run_id,)).fetchone()
    return {"run_id": row[0], "kind": row[1], "utc_ns": row[2]} if row else None


def quote_window(path, run_id, ticker, receipt_mono_ns, end_mono_ns):
    """One stale baseline plus relevant quotes; avoid loading a day of ticks."""
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        baseline = list(connection.execute("""SELECT * FROM journal WHERE kind='quote' AND run_id=?
            AND json_extract(data,'$.raw.S')=? AND mono_ns<=? ORDER BY mono_ns DESC,seq DESC LIMIT 1""",
            (run_id, ticker, receipt_mono_ns)))
        current = connection.execute("""SELECT * FROM journal WHERE kind='quote' AND run_id=?
            AND json_extract(data,'$.raw.S')=? AND mono_ns BETWEEN ? AND ? ORDER BY seq""",
            (run_id, ticker, receipt_mono_ns, end_mono_ns))
        return list({r[0]: row_record(r) for r in [*baseline, *current]}.values())
