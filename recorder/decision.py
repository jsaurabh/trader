"""Deterministic paper intent checks. This module cannot submit an order."""
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import hashlib

from .timebase import DEADLINE_NS, SECOND, iso_utc, parse_rfc3339
from .context import LIQUIDITY_POLICY, expected_liquidity_end

VERSION = "research-defaults-v0-decision-v3"
POLICY = {"min_price": "5", "min_20_session_dollar_volume": "50000000",
          "max_spread_pct": "0.25", "max_reference_increase_pct": "5",
          "max_position_dollars": "1000", "max_positions": 3,
          "daily_loss_pause_dollars": "500", "drawdown_review_pct": "5",
          "max_quote_age_ns": SECOND, "max_portfolio_age_ns": SECOND,
          "entry_expiry_after_ready_ns": SECOND}
POLICY["liquidity_measurement_policy"] = LIQUIDITY_POLICY


def decimal(value):
    if not isinstance(value, str):
        raise ValueError("exact_decimal_string_required")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("finite_decimal_required")
    return result


def next_exit(calendar, publication_ns):
    """Premarket: today's open+30m; after-close: next trading open+30m."""
    import pandas as pd
    schedule = calendar.calendar.schedule
    openings = schedule["open"]
    candidates = openings[openings > pd.Timestamp(publication_ns, unit="ns", tz="UTC")]
    if candidates.empty:
        raise ValueError("no_future_session_in_calendar")
    return int(candidates.iloc[0].value) + 30 * 60 * SECOND


def evaluate_paper_intent(analysis, inputs, receipt, published_ns, calendar, clock):
    """Every required observation is explicit and timestamped; no defaults grant entry.

    Freshness/expiry limits are development hypotheses. Reservations must include
    pending orders. The caller must serialize risk reservation and repeat these checks
    immediately before any future paper submission. An intent is never a fill.
    """
    now = clock()
    reasons, checks = list(analysis.get("reasons", [])), dict(analysis.get("checks", {}))
    result = {"decision_version": VERSION, "policy": POLICY, "decision": "skip",
              "orders_enabled": False, "checks": checks, "reasons": reasons,
              "inputs": inputs, "paper_intent": None}
    if now.mono_ns >= receipt.mono_ns + DEADLINE_NS:
        reasons.append("five_second_deadline_expired")
    if not isinstance(inputs, dict):
        reasons.append("missing_decision_inputs")
        return result
    missing = [key for key in ("company", "liquidity", "reference", "quote", "portfolio") if not isinstance(inputs.get(key), dict)]
    if missing:
        reasons.extend("missing_decision_input:"+key for key in missing)
        return result
    for key in ("eps_exceeds_consensus", "revenue_beats_consensus_by_two_pct", "revenue_guidance_midpoint_increases"):
        if checks.get(key) is not True:
            reasons.append(key + ":not_passed")
    ticker, event_id = analysis["ticker"], analysis["event_id"]

    def check(name, passed):
        checks[name] = bool(passed)
        if not passed:
            reasons.append(name + ":failed")

    def prepared(record):
        return bool(record.get("source")) and parse_rfc3339(record["available_at"])[0] < published_ns

    try:
        company = inputs["company"]
        check("eligible_common_stock", company["ticker"] == ticker and company["security_type"] == "US_COMMON_STOCK"
              and company["exchange"] in ("NYSE", "NASDAQ", "AMEX", "ARCA", "BATS")
              and company["tradable"] is True and prepared(company))
        liquidity = inputs["liquidity"]
        selected_window_end = expected_liquidity_end(calendar, published_ns)
        liquidity_close = parse_rfc3339(liquidity["last_session_close"])[0]
        check("liquidity_20_completed_sessions", liquidity["session_count"] == 20 and prepared(liquidity)
              and liquidity_close == selected_window_end
              and liquidity["liquidity_policy"] == LIQUIDITY_POLICY
              and parse_rfc3339(liquidity["available_at"])[0] >= liquidity_close
              and decimal(liquidity["average_daily_dollar_volume"]) >= Decimal(POLICY["min_20_session_dollar_volume"]))
        reference = inputs["reference"]
        reference_price = decimal(reference["price"])
        check("reliable_pre_announcement_reference", prepared(reference) and reference_price > 0
              and 0 < published_ns - parse_rfc3339(reference["observed_at"])[0] <= 7*86400*SECOND
              and reference["reliable"] is True)
        check("announcement_outside_regular_hours", calendar.session(published_ns) in ("pre_market", "after_hours"))
        session = calendar.session(now.utc_ns)
        check("current_extended_hours_session", session in ("pre_market", "after_hours"))

        quote = inputs["quote"]
        bid, ask, size = (decimal(quote[key]) for key in ("bid", "ask", "ask_size_shares"))
        source_ns = parse_rfc3339(quote["source_utc"])[0]
        available_ns = parse_rfc3339(quote["available_utc"])[0]
        check("quote_available_and_fresh", quote["ticker"] == ticker
              and 0 <= now.utc_ns-source_ns <= POLICY["max_quote_age_ns"]
              and 0 <= now.utc_ns-available_ns <= POLICY["max_quote_age_ns"]
              and 0 <= now.mono_ns-quote["available_mono_ns"] <= POLICY["max_quote_age_ns"])
        valid_quote = 0 < bid <= ask and size > 0
        check("valid_bid_ask_and_size", valid_quote)
        check("quote_coverage_and_conditions_verified", quote["coverage_verified"] is True
              and quote["conditions_eligible"] is True and quote["size_unit_verified"] is True)
        check("broker_session_supported", quote["broker_session_supported"] is True)
        check("market_status_fresh_and_tradable", quote["market_status"] == "tradable"
              and 0 <= now.utc_ns-parse_rfc3339(quote["market_status_available_utc"])[0] <= POLICY["max_quote_age_ns"])
        spread = 100*(ask-bid)/((ask+bid)/2) if valid_quote else Decimal("Infinity")
        checks["spread_pct_of_mid"] = str(spread)
        check("spread_within_limit", spread <= Decimal(POLICY["max_spread_pct"]))
        check("price_at_least_five", ask >= Decimal(POLICY["min_price"]))
        check("price_within_reference_limit", reference_price > 0 and ask <= reference_price*Decimal("1.05"))

        portfolio = inputs["portfolio"]
        check("paper_portfolio_only", portfolio["paper"] is True)
        check("portfolio_fresh_and_reconciled", portfolio["reconciled"] is True
              and 0 <= now.utc_ns-parse_rfc3339(portfolio["available_utc"])[0] <= POLICY["max_portfolio_age_ns"]
              and 0 <= now.mono_ns-portfolio["available_mono_ns"] <= POLICY["max_portfolio_age_ns"])
        check("kill_switch_clear", portfolio["kill_switch"] is False)
        check("review_clear", portfolio["review_required"] is False)
        equity, peak, cash, pnl = (decimal(portfolio[k]) for k in ("equity", "peak_equity", "cash", "daily_pnl"))
        check("daily_loss_below_pause", pnl > -Decimal(POLICY["daily_loss_pause_dollars"]))
        check("daily_loss_pause_clear", portfolio["daily_pause"] is False)
        check("drawdown_below_review", peak >= equity > 0 and peak > 0 and 100*(peak-equity)/peak < Decimal(POLICY["drawdown_review_pct"]))
        positions, reservations = portfolio["positions"], portfolio["entry_reservations"]
        check("unlevered_usd_portfolio", portfolio["currency"] == "USD" and bool(portfolio["source"])
              and 0 <= cash <= equity and all(decimal(p["qty"]) > 0 and decimal(p["market_value"]) >= 0 for p in positions))
        check("cash_equity_positions_reconcile", abs(cash + sum((decimal(p["market_value"]) for p in positions), Decimal(0)) - equity) <= Decimal("0.01"))
        check("existing_position_exposure_within_cap", all(decimal(p["market_value"]) <= Decimal(POLICY["max_position_dollars"]) for p in positions))
        check("portfolio_capacity", isinstance(positions, list) and isinstance(reservations, list)
              and len({p["ticker"] for p in positions + reservations}) < POLICY["max_positions"])
        check("one_entry_per_company_event", [ticker, event_id] not in portfolio["entered_company_events"]
              and all(p["ticker"] != ticker for p in positions + reservations))
        reserved = sum((decimal(p["notional"]) for p in reservations), Decimal(0))
        check("valid_reservations_within_position_cap", all(0 <= decimal(p["notional"]) <= Decimal(POLICY["max_position_dollars"]) for p in reservations))
        notional = min(Decimal(POLICY["max_position_dollars"]), cash-reserved)
        tick = decimal(company["price_tick"])
        check("verified_price_tick", tick > 0 and bool(company["tick_source"]))
        # A buy limit never rounds above the checked ask/reference/position cap.
        limit = (ask/tick).to_integral_value(rounding=ROUND_FLOOR)*tick if tick > 0 else Decimal(0)
        qty = min(int(notional//limit), int(size)) if notional > 0 and limit > 0 and size > 0 else 0
        checks["quantity_capped_by_visible_ask_size"] = True
        check("whole_share_cash_capacity", qty > 0 and limit*qty <= cash-reserved)
        check("limit_reaches_current_ask", limit >= ask)
        exit_ns = next_exit(calendar, published_ns)
        ready = clock()
        check("within_original_five_second_deadline", ready.mono_ns < receipt.mono_ns + DEADLINE_NS)
        if not reasons:
            result["decision"] = "paper_intent_candidate" if analysis.get("mode") == "OBSERVED" else "research_intent_ready"
            identifier = hashlib.sha256((VERSION+":"+ticker+":"+event_id).encode()).hexdigest()[:32]
            result["paper_intent"] = {"symbol": ticker, "side": "buy", "qty": str(qty), "type": "limit",
                "limit_price": str(limit), "time_in_force": "day", "extended_hours": True,
                "client_order_id": "research-"+identifier, "ready_utc": iso_utc(ready.utc_ns),
                "ready_mono_ns": ready.mono_ns,
                "expires_utc": iso_utc(ready.utc_ns + POLICY["entry_expiry_after_ready_ns"]),
                "expires_mono_ns": ready.mono_ns + POLICY["entry_expiry_after_ready_ns"],
                "cancel_after_ns": POLICY["entry_expiry_after_ready_ns"], "scheduled_exit_utc": iso_utc(exit_ns),
                "notional_cap": POLICY["max_position_dollars"], "paper_only": True,
                "requires_atomic_reservation_and_submission_recheck": True}
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError, ZeroDivisionError):
        reasons.append("missing_or_invalid_required_decision_input")
    result["reasons"] = list(dict.fromkeys(reasons))
    return result
