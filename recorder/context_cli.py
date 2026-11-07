"""Read-only context preparation CLI; never imports or calls an order transport."""
import argparse
import asyncio
import json
import os
from pathlib import Path

import httpx

from .context import ContextStore, fetch_source, prefetch_alpaca, prefetch_sec_identity
from .report import atomic_text
from .timebase import parse_rfc3339


def resolve_evidence(items, documents):
    return [dict(item, document_id=documents[item["document_id"]]) for item in items]


async def import_manifest(store, manifest, *, client=None):
    """Fetch now, then validate selected facts. Imported timestamps cannot grant availability."""
    if manifest.get("mode", store.mode) != store.mode:
        raise ValueError("manifest_mode_mismatch")
    own_client = client is None
    client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
    documents, facts = {}, []
    try:
        for alias, source in manifest["documents"].items():
            identifier, response = await fetch_source(store, client, source["url"], kind=source["source_kind"],
                params=source.get("params"), headers={"User-Agent": "AnnouncementResearch/0.1 paper-only research"})
            documents[alias] = identifier
            if response.status_code != 200:
                raise ValueError("context_source_unavailable")
        for item in manifest["facts"]:
            payload = json.loads(json.dumps(item["payload"]))
            if payload.get("period_mapping", {}).get("calendar_document_id"):
                payload["period_mapping"]["calendar_document_id"] = documents[payload["period_mapping"]["calendar_document_id"]]
            verification = json.loads(json.dumps(item.get("verification", {})))
            if "definition_document_ids" in verification:
                verification["definition_document_ids"] = [documents[d] for d in verification["definition_document_ids"]]
            if "methodology_evidence" in verification:
                verification["methodology_evidence"] = resolve_evidence(verification["methodology_evidence"], documents)
            facts.append(store.add_fact(item["ticker"], item["kind"], payload,
                document_ids=[documents[d] for d in item["document_ids"]], evidence=resolve_evidence(item["evidence"], documents), verification=verification))
    finally:
        if own_client:
            await client.aclose()
    return {"mode": store.mode, "documents": documents, "facts": facts, "availability": "actual current receipt and derivation, never imported publication time"}


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Prepare source-backed point-in-time context. No orders or paid activation.")
    parser.add_argument("--db", required=True)
    parser.add_argument("--mode", choices=("OBSERVED", "SYNTHETIC"), default="OBSERVED")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prefetch")
    p.add_argument("--symbols", required=True)
    p.add_argument("--sec", action="store_true", help="Supplement broker names with SEC cover/fiscal-calendar evidence")
    p.add_argument("--report", required=True)
    p = sub.add_parser("import")
    p.add_argument("--manifest", required=True)
    p.add_argument("--report", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--ticker", required=True)
    p.add_argument("--publication", required=True)
    p.add_argument("--actual-period-end")
    p.add_argument("--guidance-period-end")
    p.add_argument("--guidance-period-kind", choices=("QUARTER", "FISCAL_YEAR"))
    p.add_argument("--report", required=True)
    args = parser.parse_args(argv)
    try:
        store = ContextStore(args.db, mode=args.mode)
        if args.command == "prefetch":
            symbols = [s.strip().upper() for s in args.symbols.split(",")]
            if not 1 <= len(symbols) <= 10:
                raise ValueError("one_to_ten_explicit_symbols_required")
            async def run():
                results=[]
                async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
                    for ticker in symbols:
                        results.append(await prefetch_alpaca(store, ticker, client=client))
                        if args.sec:
                            results.append(await prefetch_sec_identity(store, ticker, client=client))
                return {"mode": args.mode, "results": results, "orders_enabled": False}
            report = asyncio.run(run())
        elif args.command == "import":
            report = asyncio.run(import_manifest(store, json.loads(Path(args.manifest).read_text())))
        else:
            publication_ns = parse_rfc3339(args.publication)[0]
            report = {args.ticker: store.prepare(args.ticker, publication_ns, actual_period_end=args.actual_period_end,
                      guidance_period_end=args.guidance_period_end, guidance_period_kind=args.guidance_period_kind, mode=args.mode)}
        atomic_text(args.report, json.dumps(report, indent=2)+"\n")
        print(json.dumps({"status": "context_report_written", "mode": args.mode, "orders_enabled": False}))
        return 0
    except Exception as exc:
        # External exceptions can contain URLs; only classes are printed.
        print(json.dumps({"status": "context_operation_failed", "error_class": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
