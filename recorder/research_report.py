"""Local replay artifacts and honest diagnostics for selected real development cases."""
import argparse
from dataclasses import replace
from decimal import Decimal
import html
import json
import os
from pathlib import Path

from .execution_model import ExecutionAssumptions, QuoteObservation
from .replay import ReplayCase, ReplayPlan, REQUIRED_EVIDENCE, replay_cases, write_replay_report
from .report import atomic_text
from .timebase import SECOND, parse_rfc3339


def synthetic_example(*, processing_addition_ns=0, order_transport_ns=250_000_000, each_side_cost_bps="5"):
    """A deliberately fabricated partial entry, expiry and repriced scheduled exit."""
    publication = parse_rfc3339("2026-11-02T21:30:00Z")[0]
    scheduled_exit = parse_rfc3339("2026-11-03T15:00:00Z")[0]
    case = ReplayCase("SYNTHETIC_END_TO_END", "SYNTHETIC_ZZTEST_QUARTER", "ZZTEST", publication,
                      3*SECOND+processing_addition_ns, "end_to_end_receipt_to_risk_ready", qualified=True,
                      assumed_feed_delay_ns=4*SECOND, entry_qty=99, entry_limit_price="10.02", price_tick="0.01",
                      evidence={key: True for key in REQUIRED_EVIDENCE})
    plan = ReplayPlan("SYNTHETIC", "SYNTHETIC_RESEARCH_DEFAULTS_V0", "development",
                      {"development": (publication-SECOND, scheduled_exit+120*SECOND)}, order_transport_ns=order_transport_ns,
                      exit_retry_interval_ns=SECOND)
    def quote(at, bid="10.00", ask="10.01", size="100"):
        return QuoteObservation("ZZTEST", at, at, bid, ask, size, size, "SYNTHETIC", "assumed_from_source_plus_delay",
            "FABRICATED_QUOTE_"+str(at), "FABRICATED_BOOK", size_unit_verified=True, conditions_eligible=True,
            continuity_verified=True, session_supported=True, market_tradable=True)
    ready = publication+7*SECOND
    quotes = [quote(publication+3*SECOND, "8.99", "9.00", "10000"), quote(ready-100_000_000, ask="10.02"),
              quote(ready+300_000_000), quote(ready+400_000_000),
              quote(ready+500_000_000, size="150"), quote(ready+600_000_000, ask="10.00"),
              quote(ready+1_300_000_000, "9.49", "9.50", "10000"),
              quote(scheduled_exit-100_000_000, "10.20", "10.21", "300"),
              quote(scheduled_exit+100_000_000, "10.20", "10.21", "300"),
              quote(scheduled_exit+300_000_000, "10.18", "10.19", "300"),
              quote(scheduled_exit+1_200_000_000, "10.17", "10.18", "300"),
              quote(scheduled_exit+1_550_000_000, "10.18", "10.19", "300")]
    assumptions = ExecutionAssumptions(each_side_cost_bps=each_side_cost_bps, per_share_cost="0.001", per_filled_order_cost="0.02")
    result = replay_cases([case], quotes, plan, assumptions=assumptions)
    result["fixture_description"] = ("Every company, release, expectation, clock component, quote, size, fee and fill here is fabricated. "
        "This proves accounting/control flow only. A four-second assumed wire delay is separate from three-second assumed full processing. "
        "The initial entry partially fills, unchanged quotes cannot replenish consumed capacity, the remainder expires, "
        "and the next-session scheduled limit exit misses initially and is repriced after expiry.")
    result["synthetic_processing_components_ns"] = {"queueing": 200_000_000, "document_retrieval": 300_000_000,
        "interpretation": 2_000_000_000, "numeric_comparisons": 50_000_000, "quote_checks": 300_000_000,
        "risk_checks_and_durable_readiness": 150_000_000, "additional_assumed_delay": processing_addition_ns}
    return result


def synthetic_sensitivity():
    scenarios = []
    for additional_processing_ns in (0, SECOND, 2*SECOND):
        for order_delay_ns in (250_000_000, SECOND):
            for cost_bps in ("0", "5", "12.5"):
                report = synthetic_example(processing_addition_ns=additional_processing_ns, order_transport_ns=order_delay_ns, each_side_cost_bps=cost_bps)
                case = report["cases"][0]
                scenarios.append({"mode": "SYNTHETIC", "additional_processing_ns": additional_processing_ns,
                    "assumed_order_transport_ns": order_delay_ns, "each_side_cost_bps": cost_bps,
                    "qualified_case_count": report["qualified_case_count"], "closed_model_trade_count": report["closed_model_trade_count"],
                    "entry_filled_qty": case.get("entry_filled_qty", 0), "exit_filled_qty": case.get("exit_filled_qty", 0),
                    "realized_model_pnl": case.get("realized_model_pnl"), "decision": case["decision"], "reasons": case["reasons"]})
    return {"mode": "SYNTHETIC", "scenarios": scenarios,
            "limitation": "Correlated fabricated scenarios validate costs, expiry and the five-second gate; they are not a performance estimate."}


def actual_development_diagnostics(report_paths, *, assumed_feed_delay_ms):
    """Existing real cases remain unqualified; do not turn diagnostic marks into trades."""
    if not isinstance(assumed_feed_delay_ms, int) or assumed_feed_delay_ms < 0:
        raise ValueError("explicit nonnegative feed-delay scenario required")
    cases, summaries = [], []
    earliest, latest = None, None
    for path in report_paths:
        report = json.loads(Path(path).read_text())
        if report.get("mode") != "HISTORICAL_DEVELOPMENT":
            raise ValueError("real development diagnostics require separately labelled historical source reports")
        manifest = report["manifest"]
        publication = parse_rfc3339(manifest["publication_anchor_utc"])[0]
        exit_ns = parse_rfc3339(report["scheduled_exit_utc"])[0]
        earliest = publication if earliest is None else min(earliest, publication)
        latest = exit_ns if latest is None else max(latest, exit_ns)
        latencies = [r["elapsed_from_receipt_ns"] for r in report["benchmark_attempts"]]
        # These are today's SEC download/model durations, not the original full path.
        case = ReplayCase(manifest["case_id"], manifest["case_id"], manifest["ticker"], publication,
            max(latencies) if latencies else 0, "current_primary_download_and_model_only", qualified=False,
            rejection_reasons=("no_point_in_time_consensus", "no_previous_guidance_context", "original_release_version_unverified",
                               "historical_quote_share_units_and_conditions_unverified", "full_holding_window_not_recorded"),
            assumed_feed_delay_ns=assumed_feed_delay_ms*1_000_000, evidence={key: False for key in REQUIRED_EVIDENCE})
        cases.append(case)
        marks = [Decimal(row["ask_to_exit_bid_pct"]) for row in report["delay_scenarios"] if "ask_to_exit_bid_pct" in row]
        spread_passes = sum(Decimal(row["entry_source_observation"]["spread_pct_of_mid"]) <= Decimal("0.25")
            for row in report["delay_scenarios"] if "spread_pct_of_mid" in row["entry_source_observation"])
        summaries.append({"case_id": manifest["case_id"], "symbol": manifest["ticker"], "source_report": str(path),
            "publication_anchor_provenance": manifest["publication_anchor_provenance"], "primary_source_url": manifest["primary_source_url"],
            "retrieved_historical_quote_count_entry": report["entry_quote_count"], "retrieved_historical_quote_count_exit": report["exit_quote_count"],
            "extraction_only_benchmark_ns": latencies,
            "extraction_only_timely_attempts": sum(r["completed_within_deadline"] for r in report["benchmark_attempts"]),
            "extraction_attempts": len(report["benchmark_attempts"]),
            "source_validation_passed_attempts": sum(r.get("facts_pass_source_validation", False) for r in report["benchmark_attempts"]),
            "source_validation_status_not_recorded_attempts": sum("facts_pass_source_validation" not in r for r in report["benchmark_attempts"]),
            "conditional_quote_markout_range_pct": [str(min(marks)), str(max(marks))] if marks else None,
            "scenarios_passing_proposed_spread_gate": spread_passes, "correlated_diagnostic_scenario_count": len(report["delay_scenarios"]),
            "qualifying_strategy_trades_established": 0, "observed_broker_trades": 0,
            "interpretation": "Conditional ask-to-exit-bid marks are neither fills nor proposed-strategy returns; pre-event expectations and executable liquidity are missing."})
    if not cases:
        raise ValueError("at least one reviewed actual development case required")
    plan = ReplayPlan("HISTORICAL", "research-defaults-v0-unqualified-development", "development",
                      {"development": (earliest-SECOND, latest+120*SECOND)})
    result = replay_cases(cases, [], plan)
    result["actual_development_case_diagnostics"] = summaries
    result["qualitative_findings"] = [
        "The existing cases cannot become qualified trades without matching, verifiably pre-event expectations and prior guidance.",
        "A completed download/model request under five seconds does not prove receipt-through-quote-and-risk readiness.",
        "The selected Apple paths include narrow spreads followed by negative scheduled quote marks; spread alone does not establish an advantage.",
        "None of the selected Doximity delay-scenario quotes meet the proposed spread limit, before considering other missing requirements.",
        "There are two selected events, not thirty-six independent trades. No win probability, tail-loss distribution or monthly strategy economics is established."]
    result["assumed_feed_delay_ms_for_no_trade_replay"] = assumed_feed_delay_ms
    return result


def write_diagnostics(report, json_path, html_path):
    write_replay_report(report, json_path, None)
    esc = lambda value: html.escape(str(value))
    sections = []
    for item in report["actual_development_case_diagnostics"]:
        sections.append('<section><h2>'+esc(item['symbol'])+' · actual primary-source development case</h2><p>'+esc(item['interpretation'])+'</p><dl>'+
            ''.join('<dt>'+esc(key)+'</dt><dd>'+esc(item[key])+'</dd>' for key in ('retrieved_historical_quote_count_entry', 'retrieved_historical_quote_count_exit',
                'conditional_quote_markout_range_pct', 'scenarios_passing_proposed_spread_gate', 'correlated_diagnostic_scenario_count', 'qualifying_strategy_trades_established'))+'</dl></section>')
    atomic_text(html_path, '<!doctype html><meta charset="utf-8"><title>Actual earnings development findings</title>'
        '<style>body{font:16px system-ui;max-width:1000px;margin:40px auto;padding:20px;background:#101923;color:#e7edf6}'
        'section{padding:22px;margin:22px 0;background:#192737;border-radius:12px}dl{display:grid;grid-template-columns:1fr 1fr;gap:12px}dd{margin:0}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><p>HISTORICAL DEVELOPMENT · no qualified trades or observed fills</p><h1>Actual earnings findings</h1>'
        +''.join(sections)+'<h2>What these cases establish</h2><ul>'+''.join('<li>'+esc(item)+'</li>' for item in report['qualitative_findings'])+'</ul>'
        +'<details><summary>Complete evidence and limitations</summary><pre>'+esc(json.dumps(report, indent=2))+'</pre></details>')


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Local model/replay diagnostics only; no broker or network calls")
    parser.add_argument('--output-prefix', required=True)
    parser.add_argument('--actual-case', action='append', default=[])
    parser.add_argument('--assumed-feed-delay-ms', type=int, default=4000)
    args = parser.parse_args(argv)
    synthetic = synthetic_example()
    synthetic['synthetic_sensitivity'] = synthetic_sensitivity()
    write_replay_report(synthetic, args.output_prefix+'-synthetic.json', args.output_prefix+'-synthetic.html')
    if args.actual_case:
        actual = actual_development_diagnostics(args.actual_case, assumed_feed_delay_ms=args.assumed_feed_delay_ms)
        write_diagnostics(actual, args.output_prefix+'-development.json', args.output_prefix+'-development.html')
    print(json.dumps({'status': 'research_reports_written', 'synthetic_model_only': True, 'actual_case_count': len(args.actual_case), 'broker_orders': 0}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
