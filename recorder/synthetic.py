"""Explicitly fabricated observations for checking recorder/report behavior."""
import json

from .collectors import normalize_alert, run_metadata
from .documents import inspect_document
from .journal import Journal
from .timebase import SECOND, Stamp, parse_rfc3339


def make_demo(path):
    base, _ = parse_rfc3339("2026-08-03T20:00:04Z")
    mono = 10_000 * SECOND
    j = Journal(path, run_metadata(["ZZTEST"], "iex", synthetic=True), started=Stamp(base-2*SECOND, mono-2*SECOND))
    def stamp(offset):
        return Stamp(base + offset, mono + offset)
    def emit(kind, data, offset):
        j.emit(kind, data, stamp(offset))
    try:
        emit("transport", {"stream": "alpaca", "state": "ready", "quotes": ["ZZTEST"], "trades": ["ZZTEST"]}, -SECOND)
        emit("transport", {"stream": "rtpr", "state": "ready"}, -SECOND)
        def quote(bid, ask, offset):
            from .timebase import iso_utc
            emit("quote", {"feed": "iex", "calendar_session_at_receipt": "after_hours", "raw": {
                "T": "q", "S": "ZZTEST", "bp": bid, "ap": ask, "bs": 2, "as": 3,
                "bx": "V", "ax": "V", "c": [], "t": iso_utc(base + offset - 1_000_000)}}, offset)
        quote("10.00", "10.02", -100_000_000)
        msg = {"type": "alert", "ticker": "ZZTEST", "rules": [{"rule_name": "SYNTHETIC earnings"}],
               "article_published_at": "2026-08-03T20:00:00Z", "article_url": "https://rtpr.io/a/SYNTHETIC-only-20260803"}
        alert = normalize_alert(msg)
        emit("wire_frame", {"raw": json.dumps(msg)}, 0)
        emit("alert", alert, 0)
        emit("alert", alert, 100_000_000)
        body = b'<article><h1>SYNTHETIC ONLY</h1><table><tr><td>Revenue</td><td>100</td></tr></table></article>'
        emit("document", {"event_id": alert["event_id"], **inspect_document(body, "text/html"),
                          "finished_within_five_seconds": True}, SECOND)
        quote("10.10", "10.12", 4_900_000_000)
        # This quote must never appear in the five-second snapshot.
        quote("90.00", "90.02", 5_000_000_001)
        emit("observation_window", {"event_id": alert["event_id"], "receipt_mono_ns": mono,
                                   "scheduler_lateness_ns": 20_000_000}, 5_020_000_000)
        corrected = {**msg, "correction": True, "version": 2}
        emit("alert", normalize_alert(corrected), 6 * SECOND)
        emit("document", {"event_id": alert["event_id"], **inspect_document(body.replace(b"100", b"101"), "text/html"),
                          "finished_within_five_seconds": False}, 7 * SECOND)
        emit("transport", {"stream": "rtpr", "state": "disconnected"}, 8 * SECOND)
        emit("gap", {"stream": "rtpr", "reason": "SYNTHETIC_disconnect"}, 8 * SECOND)
        emit("run_end", {"reason": "SYNTHETIC_demo_complete"}, 9 * SECOND)
    finally:
        j.close()
