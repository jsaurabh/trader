"""Bounded retrospective SIP retrieval. Never supply this data to a live decision."""
from decimal import Decimal, InvalidOperation
import json
from zoneinfo import ZoneInfo
from datetime import datetime

import httpx

from .collectors import ALPACA_DATA, SYMBOL, alpaca_headers, safe_error
from .timebase import SECOND, Stamp, iso_utc, local_time, parse_rfc3339

MIN_HISTORY_AGE_NS = 16 * 60 * SECOND
MAX_WINDOW_NS = 15 * 60 * SECOND


def validate_window(symbols, start, end, anchor, now_ns):
    if not symbols or len(symbols) > 10 or any(not SYMBOL.fullmatch(s) for s in symbols):
        raise ValueError("Historical research requires one to ten explicit symbols")
    start_ns, _ = parse_rfc3339(start)
    end_ns, _ = parse_rfc3339(end)
    anchor_ns, _ = parse_rfc3339(anchor)
    if not start_ns <= anchor_ns or not anchor_ns + 5 * SECOND <= end_ns or start_ns >= end_ns:
        raise ValueError("Window must include the anchor and anchor plus five seconds")
    if end_ns - start_ns > MAX_WINDOW_NS:
        raise ValueError("Diagnostic historical windows are limited to fifteen minutes")
    if end_ns > now_ns - MIN_HISTORY_AGE_NS:
        raise ValueError("Historical requests must end at least sixteen minutes ago")
    return start_ns, end_ns, anchor_ns


async def collect_history(journal, symbols, start, end, anchor, *, client=None, max_pages=5):
    """Store exact responses with actual retrieval times and bounded pagination."""
    now = Stamp.now()
    start_ns, end_ns, anchor_ns = validate_window(symbols, start, end, anchor, now.utc_ns)
    if not 1 <= max_pages <= 20:
        raise ValueError("Historical page budget must be between one and twenty")
    asof = datetime.fromtimestamp(anchor_ns // SECOND, ZoneInfo("America/New_York")).date().isoformat()
    request = {"source": "Alpaca historical SIP quotes", "symbols": sorted(set(symbols)),
               "start_utc": iso_utc(start_ns), "end_utc": iso_utc(end_ns), "anchor_utc": iso_utc(anchor_ns),
               "anchor_los_angeles": local_time(anchor_ns), "asof": asof, "feed": "sip",
               "minimum_age_ns": MIN_HISTORY_AGE_NS, "max_pages": max_pages,
               "live_execution_eligible": False,
               "anchor_meaning": "Research source-time anchor; not a measured wire receipt",
               "retrieval_started_utc": iso_utc(now.utc_ns)}
    journal.emit("historical_request", request, now)
    owns_client = client is None
    client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
    pages, quote_count, next_token, seen_tokens = 0, 0, None, set()
    outcome = {"status": "incomplete", "complete": False, "live_execution_eligible": False}
    try:
        for page in range(max_pages):
            params = {"symbols": ",".join(sorted(set(symbols))), "start": request["start_utc"],
                      "end": request["end_utc"], "feed": "sip", "limit": 10000, "sort": "asc", "asof": asof}
            if next_token:
                params["page_token"] = next_token
            response = await client.get(ALPACA_DATA + "/v2/stocks/quotes", headers=alpaca_headers(), params=params)
            received = Stamp.now()
            if response.status_code != 200:
                journal.emit("historical_page", {"page": page + 1, "http_status": response.status_code,
                                                  "feed": "sip", "raw": None}, received)
                outcome.update(status="access_denied" if response.status_code in (401, 403) else "http_error",
                               http_status=response.status_code)
                break
            raw = response.text
            try:
                body = json.loads(raw, parse_float=str)
            except ValueError:
                body = None
            quotes = body.get("quotes") if isinstance(body, dict) else None
            if (not isinstance(quotes, dict) or "next_page_token" not in body
                    or any(not isinstance(v, list) for v in quotes.values())):
                journal.emit("historical_page", {"page": page + 1, "http_status": 200, "feed": "sip", "raw": raw,
                                                  "schema_valid": False}, received)
                outcome["status"] = "unexpected_schema"
                break
            pages += 1
            quote_count += sum(len(v) for v in quotes.values())
            journal.emit("historical_page", {"page": page + 1, "http_status": 200, "feed": "sip", "raw": raw,
                                              "schema_valid": True}, received)
            next_token = body.get("next_page_token")
            if next_token is None:
                outcome.update(status="complete", complete=True)
                break
            if not isinstance(next_token, str) or not next_token or next_token in seen_tokens:
                outcome["status"] = "invalid_or_repeated_page_token"
                break
            seen_tokens.add(next_token)
        else:
            outcome["status"] = "page_budget_exhausted"
    except Exception as exc:
        outcome.update(status="request_error", **safe_error(exc))
    finally:
        if owns_client:
            await client.aclose()
    outcome.update(pages=pages, quote_count=quote_count, more_pages=bool(next_token),
                   finished_utc=iso_utc(Stamp.now().utc_ns))
    journal.emit("historical_result", outcome)
    journal.emit("run_end", {"reason": "historical_diagnostic_complete", "complete": outcome["complete"]})
    return outcome


def source_quote_at(observations, target_ns, requested_start_ns, complete):
    """Source-time reconstruction, explicitly unavailable to the past live system."""
    candidates = []
    rejected = 0
    for index, quote in enumerate(observations):
        try:
            source_ns, precision = parse_rfc3339(quote.get("t", ""))
        except (ValueError, TypeError, AttributeError):
            rejected += 1
            continue
        if requested_start_ns <= source_ns <= target_ns:
            candidates.append((source_ns, index, precision, quote))
    base = {"target_source_utc": iso_utc(target_ns), "known_to_system_at_target": False,
            "live_execution_eligible": False, "source_pages_complete": complete, "invalid_timestamp_count": rejected}
    if not candidates:
        return {**base, "status": "missing", "reason": "no_prior_quote_in_downloaded_window"}
    source_ns, _, precision, q = max(candidates, key=lambda item: (item[0], item[1]))
    simultaneous = [item[3] for item in candidates if item[0] == source_ns]
    variants = len({json.dumps({k: item.get(k) for k in ("bp", "ap", "bs", "as", "bx", "ax")}, sort_keys=True) for item in simultaneous})
    age = target_ns - source_ns
    result = {**base, "status": "historical_source_observation", "source_timestamp_raw": q["t"],
              "source_precision_ns": precision, "source_age_ns": age, "stale_over_one_second": age > SECOND,
              "observations_at_same_source_timestamp": len(simultaneous), "quote_state_ambiguous": variants > 1,
              "bid": q.get("bp"), "ask": q.get("ap"), "bid_size_vendor_units": q.get("bs"), "ask_size_vendor_units": q.get("as"),
              "size_unit": "unverified_for_historical_endpoint; no conversion to shares assumed",
              "bid_exchange": q.get("bx"), "ask_exchange": q.get("ax"), "conditions": q.get("c", [])}
    try:
        bid, ask = Decimal(str(q["bp"])), Decimal(str(q["ap"]))
        bid_size, ask_size = Decimal(str(q["bs"])), Decimal(str(q["as"]))
        if not all(n.is_finite() for n in (bid, ask, bid_size, ask_size)) or bid <= 0 or ask < bid or min(bid_size, ask_size) <= 0:
            raise ValueError("invalid_quote")
        result["spread_pct_of_mid"] = str(100 * (ask-bid) / ((ask+bid)/2))
        result["numerically_valid"] = True
    except (ValueError, InvalidOperation, KeyError, TypeError):
        result.update(status="invalid_quote", numerically_valid=False)
    return result


def historical_reports(rows):
    reports = []
    for row in rows:
        if row["kind"] != "historical_request":
            continue
        run = row["run_id"]
        request = row["data"]
        results = [r["data"] for r in rows if r["run_id"] == run and r["kind"] == "historical_result"]
        result = results[-1] if results else {"status": "interrupted", "complete": False}
        pages = [r for r in rows if r["run_id"] == run and r["kind"] == "historical_page" and r["data"].get("schema_valid")]
        quotes = {s: [] for s in request["symbols"]}
        for page in pages:
            data = json.loads(page["data"]["raw"], parse_float=str)
            for symbol in quotes:
                quotes[symbol].extend(data["quotes"].get(symbol, []))
        start_ns, _ = parse_rfc3339(request["start_utc"])
        anchor_ns, _ = parse_rfc3339(request["anchor_utc"])
        reports.append({"run_id": run, "request": request, "result": result,
                        "retrieval_times_utc": [iso_utc(p["stamp"].utc_ns) for p in pages],
                        "snapshots": {s: {"quote_count": len(qs),
                                          "at_anchor": source_quote_at(qs, anchor_ns, start_ns, result["complete"]),
                                          "at_anchor_plus_five_seconds": source_quote_at(qs, anchor_ns + 5*SECOND, start_ns, result["complete"])}
                                      for s, qs in quotes.items()},
                        "limitations": ["Historical source-time observations were downloaded later; never a live fallback",
                                        "Archive may contain later corrections; original availability/version not established",
                                        "No measured feed delay, simulated fills, or strategy returns"]})
    return reports
