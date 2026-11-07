"""Fabricated financial input for functional and API latency checks only."""
import copy

from .analysis import DEFAULT_MODEL, OpenAIExtractor, TimedAnalyzer
from .timebase import SECOND, Stamp, iso_utc

SOURCE = """SYNTHETIC ENGINEERING FIXTURE — not a real company announcement.
All monetary amounts, including per-share amounts, are in U.S. dollars (USD).
ZZTEST reports results for the quarter ended June 30, 2026.
For the quarter ended June 30, 2026, non-GAAP diluted EPS was $1.25.
GAAP revenue for the quarter ended June 30, 2026 was $1,040 million.
For the fiscal year ending December 31, 2026, GAAP revenue guidance is $4,400 million to $4,600 million.
Every company, result, expectation, and market assumption in this fixture is fabricated.
"""
PASSAGES = SOURCE.splitlines()
PAYLOAD = {"ticker": "ZZTEST", "missing_information": [], "facts": [
    {"name": "DILUTED_EPS", "value": "1.25", "unit": "USD_PER_SHARE", "basis": "NON_GAAP", "period_end": "2026-06-30", "period_kind": "QUARTER", "evidence": [PASSAGES[3], PASSAGES[1]]},
    {"name": "REVENUE", "value": "1040", "unit": "MILLION_USD", "basis": "GAAP", "period_end": "2026-06-30", "period_kind": "QUARTER", "evidence": [PASSAGES[4], PASSAGES[1]]},
    {"name": "REVENUE_GUIDANCE_LOW", "value": "4400", "unit": "MILLION_USD", "basis": "GAAP", "period_end": "2026-12-31", "period_kind": "FISCAL_YEAR", "evidence": [PASSAGES[5], PASSAGES[1]]},
    {"name": "REVENUE_GUIDANCE_HIGH", "value": "4600", "unit": "MILLION_USD", "basis": "GAAP", "period_end": "2026-12-31", "period_kind": "FISCAL_YEAR", "evidence": [PASSAGES[5], PASSAGES[1]]}]}


def context_for(published_ns):
    documents = {"SYNTHETIC_PROVIDER_DEFINITION": "0"*64, "SYNTHETIC_ISSUER_DEFINITION": "1"*64}
    methodology_evidence = [{"document_id": key, "type": "text", "passage": "SYNTHETIC fabricated provider and issuer use the same non-GAAP EPS adjustments; GAAP revenue and guidance are explicitly declared."} for key in documents]
    common = {"available_at": iso_utc(published_ns - 60*SECOND), "source": "SYNTHETIC prepared fixture",
        "comparability_verified": True, "methodology_id": "SYNTHETIC_SHARED_EPS_METHOD_V1",
        "issuer_metric_definition_id": "SYNTHETIC_ISSUER_ADJUSTMENTS_V1", "methodology_evidence": methodology_evidence,
        "source_versions": documents, "evidence": methodology_evidence,
        "verification": {"verified_by": "SYNTHETIC_FUNCTIONAL_FIXTURE", "definition_document_ids": list(documents)}}
    return {"ticker": "ZZTEST", "eps": {**common, "value": "1.20", "unit": "USD_PER_SHARE", "basis": "NON_GAAP", "period_end": "2026-06-30", "period_kind": "QUARTER"},
            "revenue": {**common, "value": "1000", "unit": "MILLION_USD", "basis": "GAAP", "period_end": "2026-06-30", "period_kind": "QUARTER"},
            "prior_guidance": {**common, "low": "4200", "high": "4300", "unit": "MILLION_USD", "basis": "GAAP", "period_end": "2026-12-31", "period_kind": "FISCAL_YEAR"}}


async def run_analysis_demo(journal, *, use_api=False, model=DEFAULT_MODEL):
    import httpx
    async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
        async def offline(source, ticker):
            return {"payload": copy.deepcopy(PAYLOAD), "model": "SYNTHETIC_TEST_EXTRACTOR"}
        # Client construction and synthetic context preparation precede the simulated receipt.
        prepared_at = Stamp.now()
        published_ns = prepared_at.utc_ns - 4*SECOND
        context = context_for(published_ns)
        analyzer = TimedAnalyzer(OpenAIExtractor(client, model) if use_api else offline, journal)
        async def load():
            return {"full_text_verified": True, "text": SOURCE}
        receipt = Stamp.now()
        result = await analyzer.process("SYNTHETIC-analysis-demo", "ZZTEST", receipt, load,
                                        context=context, published_ns=published_ns, mode="SYNTHETIC")
        journal.emit("run_end", {"reason": "SYNTHETIC_analysis_demo_complete", "api_called": use_api})
        return result
