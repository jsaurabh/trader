"""Chronological research replay with explicit latency and qualification provenance.

No broker or network calls exist in this module. A historical source timestamp never
becomes a claimed observed receipt or observed fill.
"""
import argparse
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, ROUND_FLOOR
import hashlib
import heapq
import html
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from .decision import next_exit
from .execution_model import ConservativeExecutionModel, ExecutionAssumptions, ModelOrder, QuoteObservation, amount, IMPLEMENTATION_SOURCE as EXECUTION_IMPLEMENTATION
from .journal import read_rows
from .report import atomic_text
from .timebase import DEADLINE_NS, SECOND, MarketCalendar, iso_utc, local_time, parse_rfc3339

VERSION = "chronological-qualified-limit-replay-v1"
IMPLEMENTATION_SOURCE = Path(__file__).read_text()
REQUIRED_EVIDENCE = ("source_facts_validated", "expectations_point_in_time", "announcement_version_point_in_time",
                     "corporate_actions_verified", "complete_market_window", "price_tick_verified", "readiness_checks_completed")


@dataclass(frozen=True)
class QuoteCapabilities:
    """Per-quote verified metadata supplied by a documented market-data adapter.

    No default share multiplier is assumed. A lot-size multiplier may depend on the
    instrument and date; the caller must provide the appropriate verified conversion.
    """
    shares_per_vendor_unit: str | None = None
    share_size_evidence: str | None = None
    conditions_eligible: bool = False
    continuity_verified: bool = False
    session_supported: bool = False
    market_tradable: bool = False
    evidence_refs: tuple[str, ...] = ()


def historical_quotes_from_journal(path, symbol, *, assumed_quote_delivery_delay_ns, capabilities_for_quote=None):
    """Load exact source-time records; later retrieval remains separate and visible."""
    if type(assumed_quote_delivery_delay_ns) is not int or assumed_quote_delivery_delay_ns < 0:
        raise ValueError("explicit nonnegative quote delivery assumption required")
    rows = list(read_rows(path, kinds=("run_start", "historical_page", "historical_result")))
    modes = {r["run_id"]: r["data"].get("mode") for r in rows if r["kind"] == "run_start"}
    if not modes or any(mode not in ("HISTORICAL", "HISTORICAL_DEVELOPMENT") for mode in modes.values()):
        raise ValueError("historical quotes require separately labelled source journals")
    complete = {}
    for row in rows:
        if row["kind"] == "historical_result":
            complete[row["run_id"]] = complete.get(row["run_id"], True) and row["data"].get("complete") is True
    observations, invalid = [], 0
    for row in rows:
        if row["kind"] != "historical_page" or row["data"].get("schema_valid") is not True:
            continue
        try:
            raw_quotes = json.loads(row["data"]["raw"], parse_float=str)["quotes"].get(symbol, [])
        except (ValueError, KeyError, TypeError):
            invalid += 1
            continue
        for raw in raw_quotes:
            try:
                source_ns, precision_ns = parse_rfc3339(raw["t"])
                caps = capabilities_for_quote(raw, source_ns) if capabilities_for_quote else QuoteCapabilities()
                verified_size = caps.shares_per_vendor_unit is not None and bool(caps.share_size_evidence) and bool(caps.evidence_refs)
                multiplier = amount(caps.shares_per_vendor_unit) if verified_size else None
                if multiplier is not None and multiplier <= 0:
                    raise ValueError
                bid_size = str(Decimal(str(raw["bs"]))*multiplier) if verified_size else None
                ask_size = str(Decimal(str(raw["as"]))*multiplier) if verified_size else None
                observations.append(QuoteObservation(symbol, source_ns, source_ns+assumed_quote_delivery_delay_ns,
                    str(raw["bp"]), str(raw["ap"]), bid_size, ask_size, "HISTORICAL", "assumed_from_source_plus_delay",
                    row["run_id"]+":"+str(row["seq"])+":"+str(len(observations)), row["data"].get("feed", "sip"),
                    size_unit_verified=verified_size, conditions_eligible=caps.conditions_eligible,
                    continuity_verified=caps.continuity_verified and complete.get(row["run_id"], False),
                    session_supported=caps.session_supported, market_tradable=caps.market_tradable,
                    retrieved_at_utc_ns=row["stamp"].utc_ns, conditions=tuple(str(value) for value in raw.get("c", [])),
                    verification_refs=caps.evidence_refs, source_precision_ns=precision_ns, source_timestamp_raw=raw["t"]))
            except (ValueError, TypeError, KeyError, ArithmeticError, AttributeError):
                invalid += 1
    return observations, {"source": str(path), "mode": "HISTORICAL", "quote_count": len(observations), "invalid_rows": invalid,
        "assumed_quote_delivery_delay_ns": assumed_quote_delivery_delay_ns, "retrieval_is_not_original_receipt": True,
        "all_source_runs_complete": bool(complete) and all(complete.values()), "default_size_conversion": None}


def digest(value):
    encoded = value if isinstance(value, str) else json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True)
class ReplayCase:
    case_id: str
    announcement_key: str
    symbol: str
    publication_utc_ns: int
    processing_time_ns: int
    processing_scope: str
    qualified: bool = False
    rejection_reasons: tuple[str, ...] = ()
    observed_receipt_utc_ns: int | None = None
    assumed_feed_delay_ns: int | None = None
    entry_qty: int | None = None
    entry_limit_price: str | None = None
    price_tick: str | None = None
    evidence: dict = field(default_factory=dict)

    def __post_init__(self):
        if type(self.qualified) is not bool or not isinstance(self.evidence, dict):
            raise ValueError("qualification and evidence require explicit types")
        if not isinstance(self.rejection_reasons, (tuple, list)) or any(not isinstance(r, str) for r in self.rejection_reasons):
            raise ValueError("rejection reasons must be strings")
        object.__setattr__(self, "rejection_reasons", tuple(self.rejection_reasons))

    def receipt(self):
        if (self.observed_receipt_utc_ns is None) == (self.assumed_feed_delay_ns is None):
            raise ValueError("exactly one observed receipt or explicit feed-delay assumption required")
        if self.assumed_feed_delay_ns is not None:
            if type(self.assumed_feed_delay_ns) is not int or self.assumed_feed_delay_ns < 0:
                raise ValueError("nonnegative feed delay required")
            return self.publication_utc_ns+self.assumed_feed_delay_ns, "assumed_from_publication_anchor"
        if self.observed_receipt_utc_ns < self.publication_utc_ns:
            raise ValueError("publication/receipt clocks are not ordered; cannot model a negative delay")
        return self.observed_receipt_utc_ns, "observed_first_wire_receipt"


@dataclass(frozen=True)
class ReplayPlan:
    mode: str
    strategy_version: str
    split: str
    windows: dict
    order_transport_ns: int = 250_000_000
    cancellation_transport_ns: int = 250_000_000
    entry_lifetime_ns: int = SECOND
    exit_lifetime_ns: int = SECOND
    exit_retry_interval_ns: int = 5*SECOND
    maximum_exit_attempts: int = 12
    selection_cutoff_utc_ns: int | None = None
    locked_at_utc_ns: int | None = None

    def __post_init__(self):
        if self.mode not in ("SYNTHETIC", "HISTORICAL", "OBSERVED_REPLAY") or self.split not in ("development", "validation", "evaluation"):
            raise ValueError("explicit mode and chronological split required")
        if not self.strategy_version or self.split not in self.windows:
            raise ValueError("strategy version and selected window required")
        ordered = []
        for key in ("development", "validation", "evaluation"):
            if key in self.windows:
                start, end = self.windows[key]
                if not isinstance(start, int) or not isinstance(end, int) or start >= end:
                    raise ValueError("invalid chronological interval")
                ordered.append((key, start, end))
        if any(previous[2] > current[1] for previous, current in zip(ordered, ordered[1:])):
            raise ValueError("development/validation/evaluation intervals overlap or are not chronological")
        if self.split == "evaluation":
            start = self.windows["evaluation"][0]
            if (set(self.windows) != {"development", "validation", "evaluation"}
                    or self.locked_at_utc_ns is None or self.selection_cutoff_utc_ns is None
                    or not self.selection_cutoff_utc_ns <= self.locked_at_utc_ns < start):
                raise ValueError("untouched evaluation requires an earlier recorded strategy lock and selection cutoff")
        if (min(self.order_transport_ns, self.cancellation_transport_ns) < 0
                or min(self.entry_lifetime_ns, self.exit_lifetime_ns, self.exit_retry_interval_ns) <= 0
                or not 1 <= self.maximum_exit_attempts <= 120):
            raise ValueError("invalid latency, expiry or bounded exit-retry assumptions")


class EvaluationRegistry:
    """An evaluation event cannot be reused to select a different configuration."""
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd); os.chmod(self.path, 0o600)
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS event_use(event_key TEXT PRIMARY KEY,configuration_hash TEXT NOT NULL,dataset_hash TEXT NOT NULL)")

    def claim(self, cases, configuration_hash, dataset_hash, *, reproduction=False):
        keys = [digest({"symbol": case.symbol, "announcement_key": case.announcement_key}) for case in cases]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate canonical announcements in evaluation")
        with sqlite3.connect(self.path) as c:
            c.execute("BEGIN IMMEDIATE")
            used = [c.execute("SELECT configuration_hash,dataset_hash FROM event_use WHERE event_key=?", (key,)).fetchone() for key in keys]
            if any(row for row in used):
                if not reproduction or not all(row == (configuration_hash, dataset_hash) for row in used):
                    raise ValueError("evaluation event already inspected; cannot call a revised configuration untouched")
                return "exact_reproduction_of_registered_evaluation"
            for key in keys:
                c.execute("INSERT INTO event_use VALUES(?,?,?)", (key, configuration_hash, dataset_hash))
        return "first_registered_evaluation"


class ReplayRiskState:
    """Fresh bid marks govern new entries; pauses never change established exits."""
    def __init__(self, initial_cash="100000"):
        self.initial = amount(initial_cash)
        self.peak = self.initial
        self.daily_pauses, self.closing_equity, self.events = set(), {}, []
        self.review_required = False

    @staticmethod
    def day(utc_ns):
        return datetime.fromtimestamp(utc_ns//SECOND, ZoneInfo("America/New_York")).date().isoformat()

    def baseline(self, utc_ns):
        prior = [day for day in self.closing_equity if day < self.day(utc_ns)]
        # At model inception the portfolio is explicitly all cash, not an inferred
        # historical brokerage balance. An in-window missing close remains unknown.
        return self.closing_equity[max(prior)] if prior else self.initial

    def observe(self, utc_ns, equity):
        if equity is None:
            return
        self.peak = max(self.peak, equity)
        baseline = self.baseline(utc_ns)
        day = self.day(utc_ns)
        if baseline is not None and equity-baseline <= -500 and day not in self.daily_pauses:
            self.daily_pauses.add(day)
            self.events.append({"kind": "model_daily_loss_pause", "day": day, "equity": str(equity), "baseline": str(baseline)})
        if self.peak > 0 and 100*(self.peak-equity)/self.peak >= 5 and not self.review_required:
            self.review_required = True
            self.events.append({"kind": "model_drawdown_review", "equity": str(equity), "peak": str(self.peak)})

    def close(self, utc_ns, equity):
        self.observe(utc_ns, equity)
        self.closing_equity[self.day(utc_ns)] = equity

    def rejection_reasons(self, utc_ns, equity):
        self.observe(utc_ns, equity)
        reasons = []
        if equity is None:
            reasons.append("missing_fresh_bid_marks_for_open_positions")
        if self.baseline(utc_ns) is None:
            reasons.append("missing_previous_close_portfolio_baseline")
        if self.day(utc_ns) in self.daily_pauses:
            reasons.append("daily_loss_pause_active")
        if self.review_required:
            reasons.append("portfolio_drawdown_review_required")
        return reasons

    def summary(self):
        return {"marking": "cash plus fresh eligible bid marks for every open position",
                "initial_previous_close_assumption": str(self.initial), "peak_equity": str(self.peak),
                "daily_loss_limit": "500", "drawdown_review_pct": "5", "daily_pause_dates": sorted(self.daily_pauses),
                "review_required": self.review_required,
                "closing_equity": {key: str(value) if value is not None else None for key, value in self.closing_equity.items()},
                "events": self.events}


def _ambiguity_checked(quotes):
    states = {}
    for quote in quotes:
        state = (quote.bid, quote.ask, quote.bid_size_shares, quote.ask_size_shares, quote.feed, quote.conditions)
        states.setdefault((quote.symbol, quote.source_utc_ns), set()).add(state)
    return [replace(q, ambiguous=True) if len(states[(q.symbol, q.source_utc_ns)]) > 1 else q for q in quotes]


def replay_cases(cases, quotes, plan, *, assumptions=None, evaluation_registry=None, reproduction=False):
    """Replay already qualified strict decisions. Missing evidence always means skip.

    `processing_time_ns` must cover receipt through all quote/risk readiness checks,
    not just an LLM response. Historical feed/quote delays are scenario assumptions.
    """
    cases, quotes = list(cases), _ambiguity_checked(list(quotes))
    if len({case.case_id for case in cases}) != len(cases):
        raise ValueError("duplicate case IDs")
    if any(quote.mode != plan.mode for quote in quotes):
        raise ValueError("observed, retrospective and synthetic quote records cannot mix")
    assumptions = assumptions or ExecutionAssumptions()
    configuration_hash = digest({"version": VERSION, "plan": asdict(plan), "assumptions": asdict(assumptions),
                                 "replay_implementation_sha256": digest(IMPLEMENTATION_SOURCE),
                                 "execution_implementation_sha256": digest(EXECUTION_IMPLEMENTATION)})
    dataset_hash = digest({"cases": [asdict(c) for c in cases], "quotes": [asdict(q) for q in quotes]})
    evaluation_status = "development_or_validation_not_untouched_evaluation"
    if plan.split == "evaluation":
        if evaluation_registry is None or plan.mode == "SYNTHETIC":
            raise ValueError("actual untouched evaluation requires a persistent event-use registry")
        evaluation_status = evaluation_registry.claim(cases, configuration_hash, dataset_hash, reproduction=reproduction)
    start, end = plan.windows[plan.split]
    model = ConservativeExecutionModel(assumptions)
    risk = ReplayRiskState(assumptions.initial_cash)
    queue, sequence, latest, reports = [], 0, {}, {}

    def quote_issues(quote, now):
        return quote.issues(now, max_age_ns=assumptions.max_quote_age_ns, max_source_precision_ns=assumptions.max_source_precision_ns)

    def equity_at(now):
        total = model.cash
        for symbol, qty in model.holdings().items():
            quote = latest.get(symbol)
            if quote is None or quote_issues(quote, now):
                return None
            total += qty*amount(quote.bid)
        return total

    def observe_adverse(quote, now):
        if quote_issues(quote, now):
            return
        for report in reports.values():
            parent = model.orders.get(report["entry_order_id"])
            if parent and parent.symbol == quote.symbol and parent.filled_qty:
                sold = sum(order.filled_qty for order in model.orders.values() if order.parent_order_id == parent.order_id)
                if sold < parent.filled_qty:
                    bid = amount(quote.bid)
                    previous = report.get("lowest_observed_bid_after_entry")
                    report["lowest_observed_bid_after_entry"] = str(min(bid, amount(previous))) if previous else str(bid)

    def schedule(at, priority, kind, payload):
        nonlocal sequence
        if at < end:
            heapq.heappush(queue, (at, priority, sequence, kind, payload)); sequence += 1

    for quote in quotes:
        if start <= quote.available_utc_ns < end:
            schedule(quote.available_utc_ns, 0, "quote", quote)
    for case in cases:
        reasons = list(case.rejection_reasons)
        receipt, receipt_kind, ready = None, None, None
        if not start <= case.publication_utc_ns < end:
            reasons.append("event_outside_selected_chronological_split")
        try:
            receipt, receipt_kind = case.receipt()
            if type(case.processing_time_ns) is not int or case.processing_time_ns < 0:
                raise ValueError
            ready = receipt+case.processing_time_ns
            if case.processing_time_ns >= DEADLINE_NS:
                reasons.append("five_second_deadline_expired")
        except (ValueError, TypeError):
            reasons.append("missing_or_invalid_receipt_processing_provenance")
        if case.processing_scope != "end_to_end_receipt_to_risk_ready":
            reasons.append("processing_measurement_excludes_required_work")
        if not case.qualified:
            reasons.append("strategy_qualification_not_established")
        reasons.extend("missing_evidence:"+key for key in REQUIRED_EVIDENCE if case.evidence.get(key) is not True)
        if not case.announcement_key:
            reasons.append("missing_canonical_announcement_identity")
        calendar = MarketCalendar(case.publication_utc_ns)
        exit_ns = next_exit(calendar, case.publication_utc_ns)
        if exit_ns >= end:
            reasons.append("outcome_crosses_split_boundary")
        try:
            tick, limit = amount(case.price_tick), amount(case.entry_limit_price)
            if tick <= 0 or limit <= 0 or limit % tick or type(case.entry_qty) is not int or case.entry_qty <= 0:
                raise ValueError
        except (ValueError, TypeError, ArithmeticError):
            reasons.append("missing_or_invalid_entry_intent")
        report = {"case_id": case.case_id, "announcement_key": case.announcement_key, "symbol": case.symbol,
                  "mode": plan.mode, "split": plan.split, "strategy_version": plan.strategy_version,
                  "publication_utc": iso_utc(case.publication_utc_ns), "receipt_utc": iso_utc(receipt) if receipt else None,
                  "receipt_kind": receipt_kind, "processing_time_ns": case.processing_time_ns, "processing_scope": case.processing_scope,
                  "decision_ready_utc": iso_utc(ready) if ready else None, "scheduled_exit_utc": iso_utc(exit_ns),
                  "scheduled_exit_los_angeles": local_time(exit_ns), "decision": "skip" if reasons else "model_entry",
                  "reasons": list(dict.fromkeys(reasons)), "qualification_established": not reasons,
                  "entry_order_id": None, "exit_attempts": 0, "exit_status": "not_entered"}
        reports[case.case_id] = report
        if not reasons:
            schedule(ready, 1, "entry", (case, exit_ns))
    by_id = {case.case_id: case for case in cases}
    closes = set()
    for case in cases:
        closes.update(int(value.value) for value in MarketCalendar(case.publication_utc_ns).calendar.schedule["close"]
                      if start <= value.value < end)
    for close in sorted(closes):
        schedule(close, 3, "regular_close", None)
    while queue:
        now, _, _, kind, data = heapq.heappop(queue)
        model.advance(now)
        if kind == "quote":
            latest[data.symbol] = data
            observe_adverse(data, now)  # Include a quote that completes the final exit.
            model.on_quote(data)
            risk.observe(now, equity_at(now))
            observe_adverse(data, now)  # Include the first quote that fills a new entry.
            continue
        if kind == "regular_close":
            risk.close(now, equity_at(now))
            continue
        if kind == "entry":
            case, exit_ns = data
            report = reports[case.case_id]
            reasons = risk.rejection_reasons(now, equity_at(now))
            if any(latest.get(symbol) is not None and not quote_issues(latest[symbol], now)
                   and qty*amount(latest[symbol].bid) > amount(assumptions.maximum_position_dollars)
                   for symbol, qty in model.holdings().items()):
                reasons.append("existing_position_exposure_over_limit")
            quote = latest.get(case.symbol)
            if quote is None or quote_issues(quote, now):
                reasons.append("missing_fresh_eligible_entry_decision_quote")
            else:
                bid, ask = amount(quote.bid), amount(quote.ask)
                if ask > amount(case.entry_limit_price) or ask < 5 or 100*(ask-bid)/((ask+bid)/2) > Decimal("0.25"):
                    reasons.append("entry_quote_price_or_spread_gate_failed")
            if reasons:
                report.update(decision="skip", qualification_established=False, reasons=report["reasons"]+reasons)
                continue
            order_id = case.case_id+":entry"
            order = ModelOrder(order_id, case.announcement_key, case.symbol, "buy", case.entry_qty, case.entry_limit_price,
                               now, now+plan.order_transport_ns, now+plan.entry_lifetime_ns+plan.cancellation_transport_ns, plan.mode)
            try:
                model.submit(order)
            except ValueError as exc:
                report.update(decision="skip", qualification_established=False, reasons=report["reasons"]+[str(exc)])
                continue
            report.update(entry_order_id=order_id, exit_status="scheduled")
            schedule(order.expires_utc_ns, 0, "entry_expiry", case.case_id)
            schedule(exit_ns, 1, "exit", case.case_id)
        elif kind == "entry_expiry":
            pass  # `advance` already applied effective cancellation/expiry.
        elif kind in ("exit", "exit_retry"):
            case = by_id[data]; report = reports[data]
            parent = model.orders[report["entry_order_id"]]
            remaining = model.lot_available(parent.order_id)
            if remaining <= 0:
                report["exit_status"] = "entry_unfilled" if not parent.filled_qty else "closed"
                continue
            quote = latest.get(case.symbol)
            if report["exit_attempts"] >= plan.maximum_exit_attempts:
                report["exit_status"] = "overdue_unfilled_after_bounded_research_window"
                continue
            report["exit_attempts"] += 1
            if quote is None or quote_issues(quote, now) or MarketCalendar(now).session(now) != "regular":
                report["exit_status"] = "overdue_missing_fresh_eligible_exit_quote"
                schedule(now+plan.exit_retry_interval_ns, 1, "exit_retry", data)
                continue
            tick = amount(case.price_tick)
            limit = (amount(quote.bid)/tick).to_integral_value(rounding=ROUND_FLOOR)*tick
            order = ModelOrder(case.case_id+f":exit:{report['exit_attempts']}", case.announcement_key, case.symbol, "sell", remaining, str(limit),
                               now, now+plan.order_transport_ns, now+plan.exit_lifetime_ns+plan.cancellation_transport_ns,
                               plan.mode, parent_order_id=parent.order_id)
            try:
                model.submit(order)
            except ValueError as exc:
                report["exit_status"] = "overdue_"+str(exc)
                continue
            report["exit_status"] = "model_limit_exit_pending"
            schedule(order.expires_utc_ns, 0, "exit_expiry", (data, now))
        elif kind == "exit_expiry":
            case_id, submitted = data
            parent_id = reports[case_id]["entry_order_id"]
            if model.lot_available(parent_id) > 0:
                schedule(max(now, submitted+plan.exit_retry_interval_ns), 1, "exit_retry", case_id)
            else:
                reports[case_id]["exit_status"] = "closed"
    for report in reports.values():
        parent = model.orders.get(report["entry_order_id"])
        if parent:
            children = [order for order in model.orders.values() if order.parent_order_id == parent.order_id]
            sold = sum(order.filled_qty for order in children)
            net = sum((amount(fill["realized_pnl"]) for fill in model.fills if fill["side"] == "sell" and fill["event_id"] == parent.event_id), Decimal(0))
            report.update(entry_filled_qty=parent.filled_qty, entry_unfilled_qty=parent.remaining_qty, exit_filled_qty=sold,
                          realized_model_pnl=str(net), open_qty=parent.filled_qty-sold)
            completed = [order.last_fill_utc_ns for order in children if order.filled_qty]
            report["last_exit_fill_utc"] = iso_utc(max(completed)) if completed else None
            target = parse_rfc3339(report["scheduled_exit_utc"])[0]
            report["last_exit_fill_delay_ns"] = max(completed)-target if completed else None
            low = report.get("lowest_observed_bid_after_entry")
            report["observed_bid_mark_adverse_pct"] = (str(min(Decimal(0), 100*(amount(low)/(amount(parent.filled_notional)/parent.filled_qty)-1)))
                if low and parent.filled_qty else None)
            report["adverse_mark_scope"] = "Observed fresh bid marks only; uncovered intervals may be worse. Not a continuous maximum-adverse-excursion estimate."
            if parent.filled_qty and sold == parent.filled_qty:
                report["exit_status"] = "closed"
                report["realized_model_return_pct"] = str(100*net/(amount(parent.filled_notional)+amount(parent.costs)))
    return {"replay_version": VERSION, "mode": plan.mode, "strategy_version": plan.strategy_version, "split": plan.split,
            "plan": asdict(plan), "configuration_hash": configuration_hash, "dataset_hash": dataset_hash,
            "input_cases": [asdict(case) for case in cases], "input_quotes": [asdict(quote) for quote in quotes],
            "reproducibility": {"replay_source": IMPLEMENTATION_SOURCE, "replay_source_sha256": digest(IMPLEMENTATION_SOURCE),
                                "execution_source": EXECUTION_IMPLEMENTATION, "execution_source_sha256": digest(EXECUTION_IMPLEMENTATION)},
            "evaluation_status": evaluation_status, "cases": list(reports.values()), "execution_model": model.summary(),
            "portfolio_risk": risk.summary(),
            "qualified_case_count": sum(report["qualification_established"] for report in reports.values()),
            "closed_model_trade_count": sum(report["exit_status"] == "closed" for report in reports.values()),
            "observed_broker_trade_count": 0, "empirical_trading_performance_established": False,
            "monthly_economics": {"strategy_pnl_established": False, "fixed_expense_scenario_only": "500", "reason": "Full-month qualified coverage and actual operating costs are not established"},
            "limitations": ["Every modeled fill is a scenario assumption, separate from broker paper execution",
                "Historical announcement and quote delivery delays must be explicitly assumed when no original receipts exist",
                "Source quote timestamps do not establish what the live system knew at that time",
                "Selected development cases and correlated delay/cost scenarios cannot establish win probability or an edge",
                "Historical model knowledge can be outcome-contaminated; prospective source-fact validation is required",
                "Market/sector benchmarks and uncertainty estimates require suitable additional qualified data"]}


def write_replay_report(report, json_path, html_path=None):
    atomic_text(json_path, json.dumps(report, indent=2)+"\n")
    if html_path:
        esc = lambda value: html.escape(str(value))
        rows = ''.join('<tr>'+''.join('<td>'+esc(case.get(key, 'unavailable'))+'</td>' for key in
            ('case_id', 'decision', 'processing_time_ns', 'entry_filled_qty', 'exit_filled_qty', 'exit_status', 'realized_model_pnl', 'reasons'))+'</tr>' for case in report['cases'])
        atomic_text(html_path, '<!doctype html><meta charset="utf-8"><title>Paper research replay</title>'
            '<style>body{font:16px system-ui;max-width:1200px;margin:35px auto;padding:20px;background:#101923;color:#e7edf6}'
            'table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:10px;border-bottom:1px solid #35465a;text-align:left}'
            'section{overflow:auto;margin:25px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style>'
            f'<p>{esc(report["mode"])} · {esc(report["split"])} · MODEL scenarios, no broker orders</p><h1>Research replay</h1>'
            '<p>Five seconds starts at the explicit first receipt. Missing qualification evidence or deadline expiry means no entry. '
            'Prices and fills below are modeled assumptions; they are not observed trading results.</p>'
            '<section><table><tr><th>Case</th><th>Decision</th><th>Processing ns</th><th>Entry fills</th><th>Exit fills</th><th>Exit status</th><th>Model P&amp;L</th><th>Reasons</th></tr>'+rows+'</table></section>'
            '<h2>Limitations</h2><ul>'+''.join('<li>'+esc(item)+'</li>' for item in report['limitations'])+'</ul>'
            '<details><summary>Complete scenario evidence</summary><pre>'+esc(json.dumps(report, indent=2))+'</pre></details>')


def reproduce_report(report, *, evaluation_registry=None):
    """Re-run captured inputs; evaluation reuse requires the identical frozen config."""
    if (report["reproducibility"]["replay_source_sha256"] != digest(IMPLEMENTATION_SOURCE)
            or report["reproducibility"]["execution_source_sha256"] != digest(EXECUTION_IMPLEMENTATION)
            or report["dataset_hash"] != digest({"cases": report["input_cases"], "quotes": report["input_quotes"]})):
        raise ValueError("captured inputs or implementation changed; restore the recorded version before exact reproduction")
    result = replay_cases([ReplayCase(**item) for item in report["input_cases"]],
                        [QuoteObservation(**item) for item in report["input_quotes"]], ReplayPlan(**report["plan"]),
                        assumptions=ExecutionAssumptions(**report["execution_model"]["assumptions"]),
                        evaluation_registry=evaluation_registry, reproduction=True)
    if result["configuration_hash"] != report["configuration_hash"]:
        raise ValueError("configuration changed; cannot claim exact reproduction")
    return result
