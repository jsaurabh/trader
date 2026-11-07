"""Durable first-relevant clocks across opaque vendor IDs and identity upgrades.

All intake/classification methods are nonblocking journal producers. Before paper
admission, wait_durable() confirms the binding is committed inside the same original
deadline. A prior run's monotonic value is evidence only, never a new time window.
"""
import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from uuid import uuid4
from zoneinfo import ZoneInfo

from .journal import read_rows
from .timebase import SECOND, Stamp

VERSION = "first-relevant-announcement-clock-v1"
_TICKER = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
KINDS = ("alert", "announcement_clock_alert", "announcement_clock_classification", "announcement_clock_binding")


def ny_release_day(utc_ns):
    """Conservative development window: 04:00 ET through the next 04:00 ET."""
    local = datetime.fromtimestamp(utc_ns//SECOND, ZoneInfo("America/New_York"))
    return (local.date()-timedelta(days=local.hour < 4)).isoformat()


@dataclass
class _Alert:
    event_id: str
    ticker: str
    receipt: Stamp
    run_id: str
    publication_ns: int | None
    arrival_order: int
    windows: set = field(default_factory=set)
    tickers: set = field(default_factory=set)
    relevant: bool | None = None
    periods: set = field(default_factory=set)
    prepared_keys: set = field(default_factory=set)
    relevance_conflict: bool = False
    company_conflict: bool = False
    source_missing: bool = False
    first_identity_key: str | None = None


class AnnouncementClockRegistry:
    def __init__(self, journal, *, clock=Stamp.now, window_key=ny_release_day):
        self.journal, self.clock, self.window_key = journal, clock, window_key
        self.instance = uuid4().hex
        self.alerts, self.groups, self.event_group, self.alias_group = {}, {}, {}, {}
        self.arrival_counter, self.group_counter, self.generation = 0, 0, 0
        self._cache, self._by_company_window = {}, {}
        for row in read_rows(journal.path, kinds=KINDS):
            data, kind = row["data"], row["kind"]
            if kind == "alert":
                self._observe(data.get("event_id"), data.get("ticker"), data.get("published_ns"), row["stamp"], row["run_id"])
            elif kind == "announcement_clock_alert":
                self._observe(data.get("event_id"), data.get("ticker"), data.get("published_ns"), row["stamp"], row["run_id"],
                              arrival_order=data.get("first_arrival_order"))
            elif kind == "announcement_clock_classification":
                if data.get("event_id") in self.alerts:
                    self._classify(data["event_id"], data.get("relevant"), data.get("period_end"),
                                   data.get("period_kind", "QUARTER"), data.get("announcement_key"))
            elif kind == "announcement_clock_binding":
                self._restore_pin(data)

    @staticmethod
    def _validate_alert(event_id, ticker, publication_ns, receipt):
        return (isinstance(event_id, str) and 0 < len(event_id) <= 2048 and isinstance(ticker, str) and _TICKER.fullmatch(ticker)
                and isinstance(receipt, Stamp) and type(receipt.utc_ns) is int and type(receipt.mono_ns) is int
                and (publication_ns is None or type(publication_ns) is int))

    def _observe(self, event_id, ticker, publication_ns, receipt, run_id, arrival_order=None):
        if not self._validate_alert(event_id, ticker, publication_ns, receipt):
            return None
        try:
            windows = {self.window_key(receipt.utc_ns)}
            if publication_ns is not None:
                windows.add(self.window_key(publication_ns))
        except (ValueError, OverflowError, OSError, TypeError):
            return None
        event = self.alerts.get(event_id)
        if event is None:
            self.arrival_counter += 1
            event = _Alert(event_id, ticker, receipt, run_id, publication_ns, self.arrival_counter)
            self.alerts[event_id] = event
        if type(arrival_order) is int and arrival_order > 0:
            event.arrival_order = arrival_order
            self.arrival_counter = max(self.arrival_counter, arrival_order)
        event.tickers.add(ticker)
        event.company_conflict = len(event.tickers) > 1
        event.windows.update(windows)
        for company in event.tickers:
            for window in event.windows:
                self._by_company_window.setdefault((company, window), set()).add(event_id)
        return event

    def observe_alert(self, event_id, ticker, published_ns, receipt):
        """Call for every alert, including duplicates and alerts later skipped."""
        if not self._validate_alert(event_id, ticker, published_ns, receipt):
            raise ValueError("invalid_clock_alert")
        event = self._observe(event_id, ticker, published_ns, receipt, self.journal.run_id)
        if event is None:
            raise ValueError("invalid_clock_time_window")
        self.journal.emit("announcement_clock_alert", {"registry_version": VERSION, "event_id": event_id,
            "ticker": ticker, "published_ns": published_ns, "first_arrival_order": event.arrival_order}, receipt)
        return self.binding(event_id)

    def _new_group(self, event):
        self.group_counter += 1
        key = "announcement-clock:"+hashlib.sha256((event.run_id+":"+event.ticker+":"+event.event_id).encode()).hexdigest()
        self.groups[key] = {"clock_key": key, "created": self.group_counter, "members": set(), "aliases": set(), "pinned": False}
        return key

    def _join(self, event_id, aliases):
        event = self.alerts[event_id]
        roots = {self.alias_group[a] for a in aliases if a in self.alias_group}
        if event_id in self.event_group:
            roots.add(self.event_group[event_id])
        root = min(roots, key=lambda r: self.groups[r]["created"]) if roots else self._new_group(event)
        group = self.groups[root]
        for other in roots-{root}:
            merged = self.groups.pop(other)
            group["members"].update(merged["members"])
            group["aliases"].update(merged["aliases"])
            group["aliases"].add("clock:"+other)
            group["pinned"] = group["pinned"] or merged["pinned"]
        group["members"].add(event_id)
        group["aliases"].update(aliases)
        for member in group["members"]:
            self.event_group[member] = root
        for alias in group["aliases"]:
            self.alias_group[alias] = root
        return group

    def _classify(self, event_id, relevant, period_end, period_kind, announcement_key):
        event = self.alerts[event_id]
        if relevant is not None and type(relevant) is not bool:
            raise ValueError("relevance_must_be_boolean_or_unknown")
        if period_end is not None:
            period_end = date.fromisoformat(period_end).isoformat()
            if period_kind not in ("QUARTER", "FISCAL_YEAR", "OTHER"):
                raise ValueError("invalid_clock_period_kind")
        if announcement_key is not None and (not isinstance(announcement_key, str) or not announcement_key.strip() or len(announcement_key) > 2048):
            raise ValueError("invalid_prepared_announcement_key")
        if event.relevant is True and relevant is False:
            # Once relevant, later reclassification cannot move or erase its clock.
            event.relevance_conflict = True
        elif relevant is not None:
            event.relevant = relevant
        if event.relevant is not True:
            return
        aliases = {"event:"+event_id}
        if period_end is not None:
            event.periods.add((period_kind, period_end))
            aliases.add("period:"+event.ticker+":"+period_kind+":"+period_end)
        if announcement_key is not None:
            event.prepared_keys.add(announcement_key)
            aliases.add("prepared:"+announcement_key)
        if event.first_identity_key is None:
            event.first_identity_key = ("prepared:"+announcement_key if announcement_key is not None else
                "period:"+event.ticker+":"+period_kind+":"+period_end if period_end is not None else "event:"+event_id)
        self._join(event_id, aliases)

    def classify(self, event_id, relevant, period_end=None, period_kind="QUARTER", announcement_key=None):
        """False means definitively non-relevant, not extraction failure/missing text.

        Record True before later eligibility/latency rejection. Prepared identity and
        ticker/period aliases merge monotonically, including across release days.
        """
        if event_id not in self.alerts:
            raise ValueError("classify_requires_observed_alert")
        self._classify(event_id, relevant, period_end, period_kind, announcement_key)
        self.journal.emit("announcement_clock_classification", {"registry_version": VERSION, "event_id": event_id,
            "relevant": relevant, "period_end": period_end, "period_kind": period_kind, "announcement_key": announcement_key})
        return self.binding(event_id)

    def _restore_pin(self, data):
        """Restore binding evidence too, so a lost/partial classification cannot extend it."""
        if data.get("relevant") is not True:
            return
        event_id, first_id = data.get("event_id"), data.get("first_event_id")
        if not isinstance(event_id, str) or not isinstance(first_id, str):
            return
        first = self.alerts.get(first_id)
        if first is None:
            try:
                receipt = Stamp(data["first_receipt_utc_ns"], data["first_receipt_mono_ns"])
                first = self._observe(first_id, data["first_ticker"], data.get("publication_ns"), receipt, data["first_run_id"])
                if first is None:
                    return
                first.arrival_order = data["first_arrival_order"]
                self.arrival_counter = max(self.arrival_counter, first.arrival_order)
                first.source_missing = True
            except (KeyError, TypeError, ValueError):
                return
        if event_id not in self.alerts:
            # A binding is enough to preserve a conservative prior clock even
            # when its alert row is missing. It can never authorize admission.
            event = self._observe(event_id, data.get("ticker"), first.publication_ns, first.receipt, first.run_id)
            if event is None:
                return
            event.source_missing = True
        if first.relevant is False:
            first.relevance_conflict = True
        first.relevant = True
        self.alerts[event_id].relevant = True
        if first.first_identity_key is None and isinstance(data.get("first_key"), str):
            first.first_identity_key = data["first_key"]
        saved_aliases = data.get("identity_aliases", [])
        if not isinstance(saved_aliases, list):
            first.relevance_conflict = True
            saved_aliases = []
        aliases = set(a for a in saved_aliases if isinstance(a, str))
        group = self._join(first_id, aliases | {"event:"+event_id, "event:"+first_id})
        self._join(event_id, group["aliases"])
        root = self.event_group[event_id]
        group = self.groups[root]
        pinned_key = data.get("clock_key")
        if isinstance(pinned_key, str) and pinned_key != root and not group["pinned"]:
            self.groups[pinned_key] = self.groups.pop(root)
            group["clock_key"] = pinned_key
            for member in group["members"]:
                self.event_group[member] = pinned_key
            for alias in group["aliases"]:
                self.alias_group[alias] = pinned_key
        group["pinned"] = True
        # If the journal contains contradictory receipt pins, preserve the observed
        # receipt and block; do not choose a convenient later time.
        if ((first.receipt.utc_ns, first.receipt.mono_ns, first.run_id) !=
                (data.get("first_receipt_utc_ns"), data.get("first_receipt_mono_ns"), data.get("first_run_id"))):
            first.relevance_conflict = True

    def _compute(self, event_id):
        event = self.alerts[event_id]
        group = self.groups.get(self.event_group.get(event_id))
        members = [self.alerts[e] for e in group["members"]] if group else [event]
        first = min(members, key=lambda e: e.arrival_order)
        reasons, blockers = [], []
        if event.relevant is None:
            reasons.append("announcement_relevance_not_classified")
        elif event.relevant is False:
            reasons.append("announcement_classified_non_relevant")
        if any(e.company_conflict for e in members) or len({e.ticker for e in members}) > 1:
            reasons.append("event_id_company_conflict")
        periods = set().union(*(e.periods for e in members))
        if any(e.relevance_conflict for e in members) or len(periods) > 1:
            reasons.append("announcement_classification_or_period_conflict")
        if any(e.source_missing for e in members):
            reasons.append("restored_clock_source_alert_missing")
        comparable = first.run_id == self.journal.run_id
        if not comparable:
            reasons.append("first_relevant_receipt_from_prior_run_research_only")
        if first.publication_ns is None:
            reasons.append("missing_original_publication_time")
        if event.relevant is True:
            company_windows = {(company, window) for e in members for company in e.tickers for window in e.windows}
            candidates = set().union(*(self._by_company_window.get(pair, set()) for pair in company_windows)) if company_windows else set()
            for other_id in candidates:
                other = self.alerts[other_id]
                if other_id in self.event_group and group and self.event_group[other_id] == group["clock_key"]:
                    continue
                if other.arrival_order >= first.arrival_order or other.relevant is False:
                    continue
                unresolved = other.relevant is None or (other.relevant is True and not other.periods and not other.prepared_keys)
                if unresolved or other.company_conflict:
                    blockers.append(other_id)
            if blockers:
                reasons.append("earlier_same_company_release_unclassified_or_unresolved")
        first_key = first.first_identity_key or "event:"+first.event_id
        result = {"registry_version": VERSION, "event_id": event_id, "ticker": event.ticker,
            "relevant": event.relevant, "effective_receipt": first.receipt, "first_event_id": first.event_id,
            "first_ticker": first.ticker, "first_key": first_key, "clock_key": group["clock_key"] if group else None,
            "first_run_id": first.run_id, "publication_ns": first.publication_ns,
            "first_arrival_order": first.arrival_order, "comparable_in_current_run": comparable,
            "block_reason": reasons[0] if reasons else None, "block_reasons": reasons,
            "blocking_event_ids": sorted(blockers, key=lambda e: self.alerts[e].arrival_order),
            "identity_aliases": sorted(group["aliases"]) if group else [],
            "release_window_policy": "NY 04:00-to-04:00 publication/receipt buckets" if self.window_key is ny_release_day else "custom_window_key"}
        return result

    @staticmethod
    def _serialized(binding):
        return {k: v for k, v in binding.items() if k not in ("effective_receipt", "durability_token", "binding_signature")} | {
            "first_receipt_utc_ns": binding["effective_receipt"].utc_ns,
            "first_receipt_mono_ns": binding["effective_receipt"].mono_ns}

    def binding(self, event_id):
        if event_id not in self.alerts:
            raise ValueError("unknown_clock_alert")
        result = self._compute(event_id)
        serialized = self._serialized(result)
        signature = hashlib.sha256(json.dumps(serialized, sort_keys=True).encode()).hexdigest()
        cached = self._cache.get(event_id)
        if cached and cached["binding_signature"] == signature:
            return deepcopy(cached)
        self.generation += 1
        token = {"registry_instance": self.instance, "generation": self.generation, "run_id": self.journal.run_id}
        result.update(binding_signature=signature, durability_token=token)
        self.journal.emit("announcement_clock_binding", serialized | token | {"binding_signature": signature})
        self._cache[event_id] = result
        return deepcopy(result)

    def is_current(self, binding):
        event_id = binding.get("event_id")
        if event_id not in self.alerts:
            return False
        signature = hashlib.sha256(json.dumps(self._serialized(self._compute(event_id)), sort_keys=True).encode()).hexdigest()
        return signature == binding.get("binding_signature")

    def _committed_generation(self, token):
        uri = Path(self.journal.path).resolve().as_uri()+"?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=0.05) as c:
            row = c.execute("SELECT json_extract(data,'$.generation') FROM journal WHERE run_id=? AND kind='announcement_clock_binding' AND json_extract(data,'$.registry_instance')=? ORDER BY seq DESC LIMIT 1",
                            (token["run_id"], token["registry_instance"])).fetchone()
        return int(row[0]) if row else 0

    async def wait_durable(self, binding, deadline_mono_ns):
        """Durability only; caller must also require no block and is_current at send."""
        token = binding.get("durability_token")
        if not isinstance(token, dict) or token.get("registry_instance") != self.instance:
            return False
        while self.clock().mono_ns < deadline_mono_ns:
            if not self.is_current(binding):
                return False
            remaining = (deadline_mono_ns-self.clock().mono_ns)/SECOND
            try:
                async with asyncio.timeout(remaining):
                    committed = await asyncio.to_thread(self._committed_generation, token)
            except (TimeoutError, sqlite3.Error):
                return False
            if committed >= token["generation"]:
                return self.is_current(binding) and self.clock().mono_ns < deadline_mono_ns
            await asyncio.sleep(min(0.005, max(0, (deadline_mono_ns-self.clock().mono_ns)/SECOND)))
        return False
