import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

from .collectors import Recorder, run_metadata
from .journal import Journal
from .history import collect_history, validate_window
from .probe import probe
from .report import build_report, write_report
from .synthetic import make_demo
from .timebase import Stamp
from .analysis import DEFAULT_MODEL
from .analysis_demo import run_analysis_demo
from .locking import CaptureLock
from .case_study import run_case
from .context import ContextStore


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Announcement research with a hard five-second admission deadline. Optional paper-only execution; live transport is disabled.")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("record", "serve", "probe", "demo"):
        p = sub.add_parser(command)
        p.add_argument("--db", required=True)
        p.add_argument("--report", required=True)
        p.add_argument("--html")
        if command in ("record", "serve"):
            p.add_argument("--symbols", required=True, help="Comma-separated symbols; subscribe before events, within account limits")
            p.add_argument("--feed", choices=("iex", "sip"), default="iex", help="Explicit feed; no silent fallback")
            p.add_argument("--seconds", type=float, default=None if command == "serve" else 60,
                           help="Seconds; serve continues until stopped when omitted")
            p.add_argument("--analyze", action="store_true", help="Run budgeted source extraction; skips unless all required evidence and paper checks pass")
            p.add_argument("--context", help="Prepared point-in-time company contexts keyed by ticker")
            p.add_argument("--context-db", help="Audited point-in-time context database, prepared before each original publication cutoff")
            p.add_argument("--reference-symbols", default="", help="Up to three subscribed symbols for background prepublication SIP midpoint sampling; requires --context-db")
            p.add_argument("--reference-interval", type=float, default=10, help="At least ten seconds between reference samples; outside the entry path")
            p.add_argument("--extractor", choices=("candidates", "numeric"), default="candidates")
            p.add_argument("--paper", action="store_true", help="Enable gated Alpaca PAPER-only execution and restrictive long-only paper configuration; implies --analyze")
            p.add_argument("--paper-state", default="data/paper-ledger.sqlite")
            p.add_argument("--kill-file", default="data/paper.kill", help="Presence pauses/cancels new entries; established exits continue")
            p.add_argument("--model", default=DEFAULT_MODEL)
    p = sub.add_parser("paper-status", help="Read local paper ledger state; no network or broker mutations")
    p.add_argument("--state", default="data/paper-ledger.sqlite")
    p = sub.add_parser("paper-kill", help="Set or clear the manual entry kill file; existing exit rules continue")
    p.add_argument("--kill-file", default="data/paper.kill")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--on", action="store_true")
    group.add_argument("--off", action="store_true", help="Clear only the manual file; other risk and review pauses remain")
    p = sub.add_parser("report")
    p.add_argument("--db", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--html")
    p = sub.add_parser("analysis-demo", help="Explicitly synthetic extraction and deadline check")
    p.add_argument("--api", action="store_true", help="Call OpenAI using the configured binding; input remains synthetic")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--db", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--html")
    p = sub.add_parser("history", help="Retrospective SIP quotes only; never a live-feed fallback")
    p.add_argument("--symbols", required=True)
    p.add_argument("--start", required=True, help="Timezone-aware RFC3339")
    p.add_argument("--end", required=True, help="At least sixteen minutes ago")
    p.add_argument("--anchor", required=True, help="Source-time research anchor, not a claimed wire receipt")
    p.add_argument("--max-pages", type=int, default=5)
    p.add_argument("--db", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--html")
    p = sub.add_parser("case-study", help="Reviewed primary-source earnings development diagnostic; no orders")
    p.add_argument("--manifest", required=True)
    p.add_argument("--db", required=True, help="New prefix for three separate private development databases")
    p.add_argument("--attempts", type=int, default=3)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--report", required=True)
    p.add_argument("--html")
    args = parser.parse_args(argv)
    journal = None
    capture_lock = None
    exit_code = 0
    try:
        if args.command == "paper-kill":
            path = Path(args.kill_file)
            if args.on:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch(mode=0o600)
            else:
                path.unlink(missing_ok=True)
            print(json.dumps({"manual_entry_kill": path.exists(), "live_orders_enabled": False}))
            return 0
        elif args.command == "paper-status":
            import sqlite3
            from .portfolio import LOCAL_TERMINAL
            if not Path(args.state).is_file():
                print(json.dumps({"status": "not_initialized", "live_orders_enabled": False}))
                return 0
            connection = sqlite3.connect(Path(args.state).resolve().as_uri()+"?mode=ro", uri=True, timeout=0.1)
            try:
                connection.execute("BEGIN")
                meta = {key: json.loads(value) for key,value in connection.execute("SELECT key,value FROM meta")}
                states = [row[0] for row in connection.execute("SELECT state FROM orders")]
            finally:
                connection.close()
            flags = {key: meta.get(key) for key in ("reconciled", "review_required", "daily_pause", "kill_switch", "day")}
            print(json.dumps({"status": "local_ledger", "mode": meta.get("mode"), "flags": flags,
                "orders": len(states), "nonterminal_orders": sum(state not in LOCAL_TERMINAL for state in states),
                "live_orders_enabled": False}))
            return 0
        elif args.command == "case-study":
            result = asyncio.run(run_case(args.manifest, args.db, args.report, args.html, attempts=args.attempts, model=args.model))
            print(json.dumps({"status": "development_report_written", "timely_attempts": result["timely_attempts"],
                              "attempts": args.attempts, "entry_quotes": result["entry_quote_count"],
                              "exit_quotes": result["exit_quote_count"], "orders_submitted": 0}))
            return 0
        elif args.command == "demo":
            if Path(args.db).exists():
                raise ValueError("Use a new, separate database for synthetic data")
            make_demo(args.db)
        elif args.command == "analysis-demo":
            if Path(args.db).exists():
                raise ValueError("Use a new synthetic analysis database")
            journal = Journal(args.db, {**run_metadata(["ZZTEST"], "none", synthetic=True), "analysis_enabled": True,
                                        "input_provenance": "fabricated_earnings_fixture", "api_requested": args.api})
            asyncio.run(run_analysis_demo(journal, use_api=args.api, model=args.model))
        elif args.command == "history":
            symbols = [s.strip().upper() for s in args.symbols.split(",")]
            validate_window(symbols, args.start, args.end, args.anchor, Stamp.now().utc_ns)
            if Path(args.db).exists():
                previous = build_report(args.db)
                if any(run.get("mode") != "HISTORICAL" for run in previous["runs"]):
                    raise ValueError("Historical retrievals require a separate database")
            journal = Journal(args.db, {**run_metadata(symbols, "sip"), "mode": "HISTORICAL",
                                        "purpose": "retrospective_quote_diagnostic", "live_execution_eligible": False})
            result = asyncio.run(collect_history(journal, symbols, args.start, args.end, args.anchor, max_pages=args.max_pages))
            if not result["complete"]:
                exit_code = 2
        elif args.command in ("record", "serve", "probe"):
            is_capture = args.command in ("record", "serve")
            symbols = [s.strip().upper() for s in args.symbols.split(",")] if is_capture else ["AAPL"]
            feed = args.feed if is_capture else "iex"
            if is_capture and args.seconds is not None and not 0 < args.seconds <= 86400:
                raise ValueError("Observation must be bounded to 0 < seconds <= 86400")
            if is_capture:
                capture_lock = CaptureLock(args.db)
            if Path(args.db).exists():
                previous = build_report(args.db)
                if any(run.get("mode") != "OBSERVED" for run in previous["runs"]):
                    raise ValueError("Observed and synthetic recordings require separate databases")
            metadata = run_metadata(symbols, feed)
            if is_capture:
                analysis_enabled = args.analyze or args.paper
                if args.context and args.context_db:
                    raise ValueError("Choose one point-in-time context source")
                metadata.update(analysis_enabled=analysis_enabled, analysis_model=args.model if analysis_enabled else None,
                                extractor_mode=args.extractor if analysis_enabled else None,
                                model_request_limit_per_rolling_24h=100 if analysis_enabled else 0, capture_mode=args.command,
                                paper_enabled_requested=args.paper, live_orders_enabled=False,
                                orders_enabled=args.paper)
                reference_symbols = [s.strip().upper() for s in args.reference_symbols.split(",") if s.strip()]
                metadata.update(reference_symbols=reference_symbols, reference_interval_s=args.reference_interval)
            journal = Journal(args.db, metadata)
            if is_capture:
                contexts = json.loads(Path(args.context).read_text()) if args.context else {}
                if not isinstance(contexts, dict):
                    raise ValueError("Prepared contexts must be keyed by ticker")
                context_store = ContextStore(args.context_db) if args.context_db else None
                recorder = Recorder(journal, symbols, feed, analyze=analysis_enabled, contexts=contexts, model=args.model,
                    context_store=context_store, extractor_mode=args.extractor, paper=args.paper,
                    paper_state=args.paper_state, kill_path=args.kill_file,
                    reference_symbols=reference_symbols, reference_interval=args.reference_interval)
                def publish():
                    write_report(build_report(args.db), args.report, args.html)
                asyncio.run(recorder.run(args.seconds, report_callback=publish))
            else:
                asyncio.run(probe(journal))
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as exc:
        # Class only: external errors and paths may contain secrets. Evidence stays in the private journal.
        print(json.dumps({"status": "stopped", "error_class": type(exc).__name__}), file=sys.stderr)
        exit_code = 1
    finally:
        if journal:
            try:
                journal.close()
            except Exception:
                print('{"status":"journal_failure; recording incomplete"}', file=sys.stderr)
                exit_code = 1
        if capture_lock:
            capture_lock.close()
    if Path(args.db).exists():
        try:
            report = build_report(args.db)
            write_report(report, args.report, args.html)
            print(json.dumps({"status": "report_written", "events": len(report["events"]),
                              "record_counts": report["record_counts"], "historical_windows": len(report["historical_quotes"]),
                              "paper_enabled_requested": report.get("paper_enabled_requested", False), "live_orders_enabled": False}))
        except Exception as exc:
            print(json.dumps({"status": "report_failed", "error_class": type(exc).__name__}), file=sys.stderr)
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
