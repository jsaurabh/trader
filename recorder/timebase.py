"""Integer timestamps; monotonic time governs the five-second research window."""
from dataclasses import dataclass
from datetime import datetime, timezone
import calendar
import re
import time
from zoneinfo import ZoneInfo

SECOND = 1_000_000_000
DEADLINE_NS = 5 * SECOND
UTC = timezone.utc
PACIFIC = ZoneInfo("America/Los_Angeles")
RFC3339 = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$")


@dataclass(frozen=True)
class Stamp:
    utc_ns: int
    mono_ns: int

    @classmethod
    def now(cls):
        # This boundary is application dequeue, not NIC arrival or vendor send time.
        return cls(time.time_ns(), time.monotonic_ns())


def parse_rfc3339(value: str) -> tuple[int, int]:
    """Return UTC ns and represented precision ns, preserving nanoseconds."""
    match = RFC3339.fullmatch(value)
    if not match:
        raise ValueError("timezone-aware RFC3339 timestamp required")
    base, fraction, offset = match.groups()
    dt = datetime.fromisoformat(base + ("+00:00" if offset == "Z" else offset))
    seconds = calendar.timegm(dt.utctimetuple())
    digits = fraction or ""
    return seconds * SECOND + int(digits.ljust(9, "0")), 10 ** (9 - len(digits))


def iso_utc(ns: int) -> str:
    seconds, fraction = divmod(ns, SECOND)
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction:09d}Z"


def local_time(ns: int) -> str:
    return datetime.fromtimestamp(ns // SECOND, UTC).astimezone(PACIFIC).isoformat()


class MarketCalendar:
    def __init__(self, reference_ns: int):
        import exchange_calendars as xcals
        year = datetime.fromtimestamp(reference_ns // SECOND, UTC).year
        self.calendar = xcals.get_calendar("XNYS", start=f"{year-1}-01-01", end=f"{year+1}-12-31")

    def session(self, utc_ns: int) -> str:
        dt = datetime.fromtimestamp(utc_ns // SECOND, UTC).astimezone(ZoneInfo("America/New_York"))
        date = dt.date().isoformat()
        try:
            if not self.calendar.is_session(date):
                return "closed_non_trading_day"
            opening = self.calendar.session_open(date).value
            closing = self.calendar.session_close(date).value
        except (ValueError, KeyError):
            return "unknown_calendar_range"
        if opening <= utc_ns < closing:
            return "regular"
        minute = dt.hour * 60 + dt.minute
        if 240 <= minute and utc_ns < opening:
            return "pre_market"
        if utc_ns >= closing and minute < 1200:
            return "after_hours"
        return "overnight_not_covered_by_iex_or_sip"
