"""Explicitly timed REST quote observations for each alert, separate from streams."""
import asyncio
from collections import deque
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from .timebase import Stamp


class QuoteSnapshots:
    def __init__(self, journal, client, feed, calendar, *, capacity=100, requests_per_minute=120):
        self.journal, self.client, self.feed, self.calendar = journal, client, feed, calendar
        self.pending = asyncio.Queue(maxsize=capacity)
        self.request_starts = deque()
        self.requests_per_minute = requests_per_minute
        self.next_request_ns = 0

    def enqueue(self, event_id, ticker, receipt, target_offset_ns=0):
        data = {"event_id": event_id, "ticker": ticker, "receipt_mono_ns": receipt.mono_ns,
                "target_offset_ns": target_offset_ns, "feed": self.feed}
        try:
            self.pending.put_nowait((data, receipt))
        except asyncio.QueueFull:
            self.journal.emit("quote_snapshot_result", {**data, "status": "queue_overflow"})

    async def worker(self):
        from .collectors import ALPACA_DATA, alpaca_headers
        while True:
            data, receipt = await self.pending.get()
            try:
                now = time.monotonic_ns()
                if now < self.next_request_ns:
                    self.journal.emit("quote_snapshot_result", {**data, "status": "rate_limit_cooldown"})
                    continue
                while self.request_starts and self.request_starts[0] <= now - 60_000_000_000:
                    self.request_starts.popleft()
                if len(self.request_starts) >= self.requests_per_minute:
                    self.journal.emit("quote_snapshot_result", {**data, "status": "request_budget_exhausted"})
                    continue
                self.request_starts.append(now)
                started = Stamp.now()
                response = await self.client.get(ALPACA_DATA + "/v2/stocks/quotes/latest", headers=alpaca_headers(),
                                                 params={"symbols": data["ticker"], "feed": self.feed}, timeout=2.5,
                                                 follow_redirects=False)
                received = Stamp.now()
                result = {**data, "http_status": response.status_code, "request_started_mono_ns": started.mono_ns,
                          "available_after_receipt_ns": received.mono_ns-receipt.mono_ns,
                          "available_after_target_ns": received.mono_ns-receipt.mono_ns-data["target_offset_ns"]}
                if response.status_code == 200:
                    body = json.loads(response.text, parse_float=str)
                    quote = body.get("quotes", {}).get(data["ticker"])
                    result.update(status="captured" if isinstance(quote, dict) else "missing_quote" if quote is None else "unexpected_quote_schema",
                                  raw_response=response.text)
                    if isinstance(quote, dict):
                        self.journal.emit("quote", {"feed": self.feed, "event_id": data["event_id"],
                                                    "raw": {**quote, "T": "q", "S": data["ticker"]},
                                                    "observation_method": "rest_latest_snapshot",
                                                    "target_offset_ns": data["target_offset_ns"],
                                                    "calendar_session_at_receipt": self.calendar.session(received.utc_ns),
                                                    "exchange_halt_status": "unknown"}, received)
                else:
                    result["status"] = "http_error"
                    if response.status_code == 429:
                        retry = response.headers.get("retry-after", "60")
                        try:
                            seconds = float(retry)
                            if not 0 <= seconds < float("inf"):
                                seconds = 60
                        except ValueError:
                            try:
                                seconds = max(0, (parsedate_to_datetime(retry)-datetime.now(timezone.utc)).total_seconds())
                            except (ValueError, TypeError, OverflowError):
                                seconds = 60
                        self.next_request_ns = time.monotonic_ns() + int(seconds * 1e9)
                        result["retry_after_seconds"] = seconds
                self.journal.emit("quote_snapshot_result", result, received)
            except asyncio.CancelledError:
                self.journal.emit("quote_snapshot_result", {**data, "status": "cancelled_at_shutdown"})
                raise
            except Exception as exc:
                self.journal.emit("quote_snapshot_result", {**data, "status": "request_error", "error_class": type(exc).__name__})
            finally:
                self.pending.task_done()
