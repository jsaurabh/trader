"""Primary-source development cases, kept separate from prospective observations."""
import asyncio
import base64
from decimal import Decimal
import hashlib
from html.parser import HTMLParser
import html
import json
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .analysis import DEFAULT_MODEL, ModelRequestBudget, OpenAIExtractor, TimedAnalyzer
from .history import collect_history, source_quote_at, validate_window
from .journal import Journal, read_rows
from .report import atomic_text
from .timebase import MarketCalendar, SECOND, Stamp, iso_utc, local_time, parse_rfc3339
from .decision import next_exit


class ExhibitText(HTMLParser):
    """Text/table serialization for an explicitly reviewed SEC exhibit, not a live adapter."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.hidden, self.tables = [], 0, 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1
        if tag in ("p", "div", "tr", "br", "h1", "h2", "h3", "table"):
            self.parts.append("\n")
        if tag in ("td", "th"):
            self.parts.append("\t")
        if tag == "table":
            self.tables += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden-1)

    def handle_data(self, value):
        if not self.hidden:
            self.parts.append(value)

    def text(self):
        return "\n".join(line.rstrip() for line in "".join(self.parts).splitlines() if line.strip())


def parse_exhibit(raw):
    parser = ExhibitText()
    parser.feed(raw.decode("utf-8"))
    parser.close()
    return parser.text(), parser.tables


def quote_rows(path, symbol):
    observations, complete, start_ns, saw_result = [], True, None, False
    for row in read_rows(path, kinds=("historical_request", "historical_result", "historical_page")):
        data = row["data"]
        if row["kind"] == "historical_request":
            start_ns = parse_rfc3339(data["start_utc"])[0]
        elif row["kind"] == "historical_result":
            saw_result = True
            complete = complete and data["complete"]
        elif data.get("schema_valid"):
            observations.extend(json.loads(data["raw"], parse_float=str)["quotes"].get(symbol, []))
    return observations, start_ns, complete and saw_result


def usable(q):
    return (q.get("numerically_valid") is True and q.get("source_pages_complete") is True
            and not q.get("stale_over_one_second", True) and not q.get("quote_state_ambiguous", True))


def conditional_markouts(entry_quotes, exit_quote, publication_ns, start_ns, complete):
    """Ask-to-bid diagnostics only. No fill assumptions or pre-entry gains credited."""
    rows = []
    for feed_ms in (100, 1000, 4000):
        for processing_ms in (1000, 3000, 5000):
            for submission_ms in (250, 1000):
                target = publication_ns+(feed_ms+processing_ms+submission_ms)*1_000_000
                entry = source_quote_at(entry_quotes, target, start_ns, complete)
                row = {"assumed_feed_delay_ms": feed_ms, "assumed_processing_ms": processing_ms,
                       "assumed_order_transport_ms": submission_ms, "hypothetical_entry_utc": iso_utc(target),
                       "entry_source_observation": entry, "status": "insufficient_quotes", "filled": False}
                row["proposed_spread_gate_passed"] = (usable(entry) and Decimal(entry["spread_pct_of_mid"]) <= Decimal("0.25"))
                row["strategy_qualification_established"] = False
                if usable(entry) and usable(exit_quote):
                    ask, bid = Decimal(str(entry["ask"])), Decimal(str(exit_quote["bid"]))
                    raw = 100*(bid/ask-1)
                    row.update(status="conditional_quote_markout_only", ask_to_exit_bid_pct=str(raw),
                               after_additional_round_trip_cost_pct={str(bps)+"_bps": str(raw-Decimal(bps)/100) for bps in (0, 10, 25)})
                rows.append(row)
    return rows


async def run_case(manifest_path, prefix, report_path, html_path=None, *, attempts=3, model=DEFAULT_MODEL):
    """Run a small, explicit development diagnostic; never call a broker order API."""
    manifest = json.loads(Path(manifest_path).read_text())
    url = urlsplit(manifest["primary_source_url"])
    if (url.scheme != "https" or url.hostname != "www.sec.gov" or url.username or url.password
            or url.query or url.fragment or url.port not in (None, 443) or not url.path.startswith("/Archives/edgar/data/")):
        raise ValueError("This reviewed adapter accepts SEC archive exhibits only")
    if manifest["source_reviewed"] is not True or not 1 <= attempts <= 5:
        raise ValueError("Reviewed source and one to five benchmark attempts required")
    published_ns = parse_rfc3339(manifest["publication_anchor_utc"])[0]
    symbol = manifest["ticker"]
    paths = {key: Path(str(prefix)+"."+key+".sqlite") for key in ("analysis", "entry", "exit")}
    if any(path.exists() for path in paths.values()):
        raise ValueError("Use a new development case prefix")
    calendar = MarketCalendar(published_ns)
    exit_ns = next_exit(calendar, published_ns)
    for anchor, before, after in ((published_ns, 2, 20), (exit_ns, 2, 7)):
        validate_window([symbol], iso_utc(anchor-before*SECOND), iso_utc(anchor+after*SECOND), iso_utc(anchor), Stamp.now().utc_ns)
    results = []
    metadata = {"mode": "HISTORICAL_DEVELOPMENT", "orders_enabled": False, "case_id": manifest["case_id"],
                "source": "reviewed_primary_SEC_exhibit", "not_a_wire_benchmark": True}
    journal = Journal(paths["analysis"], metadata)
    try:
        journal.emit("development_case_manifest", manifest)
        async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
            analyzer = TimedAnalyzer(OpenAIExtractor(client, model, ModelRequestBudget(journal, limit=attempts)), journal)
            for attempt in range(attempts):
                async def load():
                    response = await client.get(manifest["primary_source_url"], headers={"User-Agent": "AnnouncementResearch/0.1 paper-only development research"})
                    ready = Stamp.now()
                    raw = response.content
                    sha = hashlib.sha256(raw).hexdigest()
                    verified = response.status_code == 200 and sha == manifest["reviewed_html_sha256"]
                    text, tables = parse_exhibit(raw) if verified else ("", 0)
                    verified = verified and all(fragment in text for fragment in manifest["identity_passages"])
                    journal.emit("development_primary_document", {"http_status": response.status_code,
                        "raw_b64": base64.b64encode(raw).decode(), "html_sha256": sha, "text": text, "tables": tables,
                        "reviewed_hash_matched": sha == manifest["reviewed_html_sha256"], "identity_verified": verified,
                        "actual_download_completed_utc": iso_utc(ready.utc_ns), "attempt": attempt+1}, ready)
                    return {"full_text_verified": verified, "text": text}
                receipt = Stamp.now()
                result = await analyzer.process(manifest["case_id"]+f":attempt-{attempt+1}", symbol, receipt, load,
                        context=None, published_ns=published_ns, mode="HISTORICAL_DEVELOPMENT")
                results.append(result)
        journal.emit("run_end", {"reason": "development_benchmark_complete", "attempts": attempts})
    finally:
        journal.close()
    for key, anchor, before, after in (("entry", published_ns, 2, 20), ("exit", exit_ns, 2, 7)):
        journal = Journal(paths[key], {**metadata, "purpose": "historical_"+key+"_quote_diagnostic"})
        try:
            await collect_history(journal, [symbol], iso_utc(anchor-before*SECOND), iso_utc(anchor+after*SECOND),
                                  iso_utc(anchor), max_pages=20)
        finally:
            journal.close()
    entry_quotes, start_ns, entry_complete = quote_rows(paths["entry"], symbol)
    exit_quotes, exit_start, exit_complete = quote_rows(paths["exit"], symbol)
    exit_quote = source_quote_at(exit_quotes, exit_ns, exit_start, exit_complete)
    report = {"mode": "HISTORICAL_DEVELOPMENT", "manifest": manifest, "model_requested": model,
              "publication_anchor_los_angeles": local_time(published_ns), "scheduled_exit_utc": iso_utc(exit_ns),
              "scheduled_exit_los_angeles": local_time(exit_ns), "benchmark_attempts": results,
              "timely_attempts": sum(r["completed_within_deadline"] for r in results),
              "source_validated_attempts": sum(r.get("facts_pass_source_validation", False) for r in results),
              "paper_order_ready_attempts": 0, "qualifying_strategy_trades_established": 0,
              "entry_quote_count": len(entry_quotes), "exit_quote_count": len(exit_quotes),
              "entry_pages_complete": entry_complete, "exit_pages_complete": exit_complete,
              "pre_publication_quote": source_quote_at(entry_quotes, published_ns-1, start_ns, entry_complete),
              "exit_source_observation": exit_quote,
              "delay_scenarios": conditional_markouts(entry_quotes, exit_quote, published_ns, start_ns, entry_complete),
              "orders_submitted": 0,
              "limitations": ["Development case, selected after the event; not untouched evaluation",
                "Publication anchor is the RTPR published field; earliest public availability is unverified",
                "SEC exhibit retrieved now; not the RTPR version or a historical receipt measurement",
                "Benchmark begins before today's SEC download; no wire alert, current quote checks, or portfolio checks",
                "No point-in-time consensus or previous guidance supplied: strategy qualification cannot be established",
                "Historical quotes downloaded later may include corrections; receipt delays and at-event availability unknown",
                "Corporate actions and original at-event quote conditions require verification before return attribution",
                "Ask-to-bid markouts assume displayed quotes persist until hypothetical execution; fills, sizes, conditions, and liquidity unverified",
                "Costs are explicit sensitivity assumptions, not measured fees/slippage; no strategy returns or profitability established",
                "Historical model output may be contaminated; facts must remain supported by quoted source passages"]}
    atomic_text(report_path, json.dumps(report, indent=2)+"\n")
    if html_path:
        esc = lambda value: html.escape(str(value))
        benchmarks = "".join("<tr>"+"".join(f"<td>{esc(value)}</td>" for value in (
            i+1, Decimal(r["elapsed_from_receipt_ns"])/1_000_000, r["completed_within_deadline"],
            r.get("facts_pass_source_validation", False), len(r["facts"]), "; ".join(r["reasons"])))+"</tr>" for i,r in enumerate(results))
        scenarios = "".join("<tr>"+"".join(f"<td>{esc(value)}</td>" for value in (
            row["assumed_feed_delay_ms"], row["assumed_processing_ms"], row["assumed_order_transport_ms"],
            row["entry_source_observation"].get("ask", "missing"), row["entry_source_observation"].get("spread_pct_of_mid", "missing"),
            row["proposed_spread_gate_passed"], row.get("ask_to_exit_bid_pct", "unavailable"),
            row.get("after_additional_round_trip_cost_pct", {}).get("25_bps", "unavailable")))+"</tr>" for row in report["delay_scenarios"])
        facts = "".join(f"<pre>{esc(json.dumps(r['facts'], indent=2))}</pre>" for r in results if r["facts"])
        atomic_text(html_path, '<!doctype html><meta charset="utf-8"><title>Earnings development case</title>'
            '<style>body{font:16px system-ui;max-width:1150px;margin:40px auto;padding:20px;background:#101923;color:#e7edf6}'
            'td,th{text-align:left;padding:10px;border-bottom:1px solid #35465a}table{border-collapse:collapse;width:100%}'
            'pre{white-space:pre-wrap;overflow-wrap:anywhere}section{overflow:auto;margin:25px 0}a{color:#9fe1cb}</style>'
            f'<p>HISTORICAL DEVELOPMENT · no orders or measured strategy returns</p><h1>{esc(symbol)} earnings case</h1>'
            f'<p>Publication anchor: {esc(local_time(published_ns))}. Scheduled exit: {esc(local_time(exit_ns))}.</p>'
            '<section><h2>Timed primary-source extraction</h2><p>Today’s SEC retrieval and model analysis, measured from an explicit benchmark start. This does not measure historical wire latency or the complete order-ready path.</p>'
            '<table><tr><th>Attempt</th><th>Total ms</th><th>Within 5s</th><th>Source checks pass</th><th>Facts</th><th>Skip reasons</th></tr>'+benchmarks+'</table></section>'
            '<section><h2>Conditional quote markouts</h2><p>Hypothetical entry at the displayed ask; exit at the scheduled displayed bid. Source-time quotes retrieved later. These are diagnostics, not fills or strategy returns.</p>'
            '<table><tr><th>Feed ms</th><th>Processing ms</th><th>Order transport ms</th><th>Entry ask</th><th>Spread %</th><th>Proposed spread gate</th><th>Quote markout %</th><th>After 25bps additional cost %</th></tr>'+scenarios+'</table></section>'
            '<section><h2>Source-backed facts</h2>'+facts+'</section><section><h2>Limitations</h2><ul>'+''.join(f'<li>{esc(item)}</li>' for item in report["limitations"])
            +'</ul></section><details><summary>Complete evidence</summary><pre>'+esc(json.dumps(report, indent=2))+'</pre></details>')
    return report
