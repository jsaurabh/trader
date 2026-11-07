"""Fabricated pipeline inputs/transport spies; no network, accounts or orders."""
import asyncio
import base64
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from recorder.analysis import TimedAnalyzer, digest
from recorder.analysis_demo import SOURCE, PAYLOAD, context_for
from recorder.collectors import Recorder, run_metadata
from recorder.journal import Journal, read_rows
from recorder.timebase import DEADLINE_NS, SECOND, Stamp, iso_utc


BODY = SOURCE.replace("ZZTEST reports results for the quarter ended June 30, 2026.",
    "Example Research Inc. (NYSE: ZZTEST) today announced financial results for the quarter ended June 30, 2026.")
TEXT = "SYNTHETIC Example earnings fixture\n\n" + BODY
HTML = "<html><article><p>Explicit synthetic original HTML fixture</p></article></html>"


def context(published):
    return {**context_for(published), "actual_period_end": "2026-06-30", "actual_period_kind": "QUARTER",
        "announcement_key": "SYNTHETIC:12345:2026-06-30:Q",
        "company": {"ticker": "ZZTEST", "exchange": "NYSE", "canonical_company_id": "SYNTHETIC:12345",
            "classification_verified": True, "issuer_identity_verified": True, "exchange_ticker_verified": True,
            "legal_issuer_name": "Example Research Inc.", "verified_issuer_aliases": [],
            "source": "SYNTHETIC verified preparation", "available_at": iso_utc(published-60*SECOND)}}


def document(published, **extra):
    return {"full_text_verified": True, "candidate_article_text": TEXT, "article_body_text": BODY,
        "content_type": "text/html", "raw_body_base64": base64.b64encode(HTML.encode()).decode(),
        "document_published_ns": published, "title": "SYNTHETIC quarterly earnings",
        "source_tickers": ["ZZTEST"], **extra}


class Documents:
    def __init__(self): self.items = []
    def enqueue(self, *args, **kwargs): self.items.append((args, kwargs))


class AnalyzerSpy:
    def __init__(self): self.calls = []
    async def process(self, event, ticker, receipt, loader, **kwargs):
        loaded = await loader()
        self.calls.append({"event": event, "ticker": ticker, "receipt": receipt, "document": loaded, **deepcopy(kwargs)})
        return {"event_id": event, "ticker": ticker, "mode": "OBSERVED", "strategy_version": "SYNTHETIC_TEST",
            "decision": "pending_execution_checks" if kwargs.get("defer_execution_checks") else "skip",
            "reasons": [] if kwargs.get("defer_execution_checks") else ["SYNTHETIC_observation_only"],
            "source_sha256": digest(loaded["text"]), "issuer_identity": loaded["issuer_identity"],
            "facts_pass_source_validation": True}


class ExecutionSpy:
    def __init__(self, recorder, *, abort=False, mutate_ack=True):
        self.recorder, self.abort, self.mutate_ack, self.calls = recorder, abort, mutate_ack, []
    async def enter(self, analysis, receipt, published_ns, prepared, on_decision, *, admission_guard):
        self.calls.append((deepcopy(analysis), receipt, published_ns, deepcopy(prepared), admission_guard))
        assert admission_guard() is True
        result = {"event_id": analysis["event_id"], "decision": "paper_entry_ready", "client_order_id": "SYNTHETIC:client",
            "orders_enabled": True, "paper": True, "reasons": [], "mode": "OBSERVED"}
        on_decision(result, Stamp.now())
        assert analysis["event_id"] in self.recorder.decided
        if self.abort:
            result.update(decision="skip", reasons=["SYNTHETIC_pre_header_abort"], pre_dispatch_abort=True,
                supersedes_candidate_client_id="SYNTHETIC:client")
            on_decision(result, Stamp.now())
        if self.mutate_ack:
            result["broker_acknowledged"] = True
            result["reasons"].append("SYNTHETIC_ack_only_mutation")
        return result


class CollectorPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/"synthetic-pipeline-observation.sqlite"
        self.journal = Journal(self.path, {**run_metadata(["ZZTEST", "OTHER"], "sip"), "fixture_type": "SYNTHETIC_NETWORK_FREE_TEST"})
        self.receipt = Stamp.now()
        self.published = self.receipt.utc_ns-SECOND
        self.recorder = Recorder(self.journal, ["ZZTEST", "OTHER"], analyze=True,
            contexts={"ZZTEST": context(self.published)})
        self.documents = Documents()

    async def asyncTearDown(self):
        tasks = list(self.recorder.checkpoints)
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.journal.close()
        self.tmp.cleanup()

    def intake(self, event="a", receipt=None, published=None, ticker="ZZTEST"):
        receipt = receipt or self.receipt
        publication = self.published if published is None else published
        result = self.recorder.handle_wire(json.dumps({"type": "alert", "ticker": ticker,
            "article_url": "https://rtpr.io/a/"+event, "article_published_at": iso_utc(publication)}), receipt, self.documents)
        self.assertEqual(result, "alert")
        return receipt

    def decisions(self):
        self.journal.close()
        return [row["data"] for row in read_rows(self.path, kinds=("decision",))]

    async def test_default_candidate_mode_and_no_paper_objects_or_live_flag(self):
        self.assertEqual(self.recorder.extractor_mode, "candidates")
        self.assertFalse(self.recorder.paper_enabled)
        self.assertIsNone(self.recorder.paper_execution)
        self.assertIsNone(self.recorder.paper_broker)
        self.assertFalse(run_metadata(["ZZTEST"], "sip")["live_orders_enabled"])
        with self.assertRaises(ValueError):
            Recorder(self.journal, ["ZZTEST"], extractor_mode="automatic")

    async def test_document_html_and_full_text_identity_are_forwarded_exactly(self):
        self.intake()
        analyzer = AnalyzerSpy()
        self.recorder.analyzer = analyzer
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        captured = analyzer.calls[0]
        self.assertEqual(captured["document"]["raw_html"], HTML)
        self.assertEqual(captured["document"]["text"], TEXT)
        proof = captured["document"]["issuer_identity"]
        self.assertTrue(proof["verified"])
        self.assertEqual(proof["canonical_company_id"], "SYNTHETIC:12345")
        self.assertEqual(proof["analysis_source_sha256"], digest(TEXT))
        self.assertEqual(proof["source_sha256"], digest(BODY))
        self.assertEqual(captured["document"]["announcement_clock"]["first_event_id"], "a")
        self.assertFalse(captured["defer_execution_checks"])

    async def test_actual_timed_analyzer_can_reach_paper_gate_without_any_broker_transport(self):
        self.recorder.paper_enabled = True
        async def extract(*args): return {"payload": deepcopy(PAYLOAD), "model": "SYNTHETIC_NETWORK_FREE"}
        self.recorder.analyzer = TimedAnalyzer(extract, self.journal)
        execution = self.recorder.paper_execution = ExecutionSpy(self.recorder)
        self.intake()
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertEqual(len(execution.calls), 1)
        analysis = execution.calls[0][0]
        self.assertEqual(analysis["decision"], "pending_execution_checks")
        self.assertTrue(analysis["facts_pass_source_validation"])
        self.assertEqual(analysis["issuer_identity"]["analysis_source_sha256"], analysis["source_sha256"])
        snapshots = self.decisions()
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]["decision"], "paper_entry_ready")
        self.assertTrue(snapshots[0]["completed_within_deadline"])
        self.assertNotIn("broker_acknowledged", snapshots[0])
        self.assertEqual(snapshots[0]["reasons"], [])

    async def test_pre_header_abort_supersedes_candidate_and_cannot_mutate_its_snapshot(self):
        self.recorder.paper_enabled = True
        self.recorder.analyzer = AnalyzerSpy()
        execution = self.recorder.paper_execution = ExecutionSpy(self.recorder, abort=True)
        self.intake()
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertEqual(len(execution.calls), 1)
        snapshots = self.decisions()
        self.assertEqual([s["decision"] for s in snapshots], ["paper_entry_ready", "skip"])
        self.assertEqual(snapshots[0]["reasons"], [])
        self.assertEqual(snapshots[1]["reasons"], ["SYNTHETIC_pre_header_abort"])
        self.assertEqual(snapshots[1]["supersedes_decision"], "paper_entry_ready")
        self.assertNotIn("broker_acknowledged", snapshots[1])
        self.assertEqual(self.recorder.final_decisions["a"]["decision"], "skip")

    async def test_original_late_skip_still_pins_new_id_correction(self):
        old = Stamp(self.receipt.utc_ns-6*SECOND, self.receipt.mono_ns-6*SECOND)
        old_publication = self.published-6*SECOND
        self.intake("a", old, old_publication)
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        await self.recorder.analyze_document("a", old, document(old_publication), True)
        self.intake("correction-b", self.receipt, self.published)
        await self.recorder.analyze_document("correction-b", self.receipt, document(self.published), True)
        binding = self.recorder.announcement_clocks.binding("correction-b")
        self.assertEqual(binding["effective_receipt"], old)
        self.assertEqual(binding["publication_ns"], old_publication)
        self.assertTrue(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertEqual(analyzer.calls, [])
        self.assertIn("five_second_deadline_expired", self.recorder.final_decisions["correction-b"]["reasons"])

    async def test_original_skip_for_document_metadata_mismatch_also_pins_clock(self):
        self.intake()
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        await self.recorder.analyze_document("a", self.receipt, document(self.published+1), True)
        self.assertTrue(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertIn("release_publication_time_does_not_match_alert", self.recorder.final_decisions["a"]["reasons"])
        self.assertFalse(analyzer.calls)

    async def test_legal_and_future_schedule_are_false_while_missing_body_remains_unknown(self):
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        self.intake("legal")
        legal = "SHAREHOLDER ALERT: SYNTHETIC legal notice for Example Research Inc. All values are fabricated."
        await self.recorder.analyze_document("legal", self.receipt, document(self.published,
            candidate_article_text=legal, article_body_text=legal), True)
        self.assertIs(self.recorder.announcement_clocks.alerts["legal"].relevant, False)
        self.intake("future")
        future = "SYNTHETIC\nExample Research Inc. (NYSE: ZZTEST) will report financial results for the quarter ended June 30, 2026 next week."
        await self.recorder.analyze_document("future", self.receipt, document(self.published,
            candidate_article_text=future, article_body_text=future), True)
        self.assertIs(self.recorder.announcement_clocks.alerts["future"].relevant, False)
        self.intake("missing")
        # Missing full text is still sent through the analyzer's missing-text path.
        self.recorder.analyzer = TimedAnalyzer(lambda *args: self.fail("No model without text"), self.journal)
        await self.recorder.analyze_document("missing", self.receipt, {"full_text_verified": False}, True)
        self.assertIsNone(self.recorder.announcement_clocks.alerts["missing"].relevant)
        self.intake("earnings")
        self.recorder.analyzer = analyzer
        await self.recorder.analyze_document("earnings", self.receipt, document(self.published), True)
        self.assertEqual(self.recorder.announcement_clocks.binding("earnings")["blocking_event_ids"], ["missing"])
        self.assertFalse(analyzer.calls)

    async def test_ticker_membership_and_headline_cannot_authorize_competitor_release(self):
        self.intake()
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        competitor = TEXT.replace("Example Research Inc. (NYSE: ZZTEST)", "Different Company (NYSE: OTHER)")
        await self.recorder.analyze_document("a", self.receipt, document(self.published,
            candidate_article_text=competitor, article_body_text=competitor), True)
        self.assertIsNone(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertIn("primary_issuer_identity_not_verified", self.recorder.final_decisions["a"]["reasons"])
        self.assertFalse(analyzer.calls)

    async def test_prepared_target_conflict_is_unknown_and_skips_without_model(self):
        self.recorder.contexts["ZZTEST"]["actual_period_end"] = "2026-09-30"
        self.intake()
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertIsNone(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertIn("prepared_actual_period_target_conflicts_with_release", self.recorder.final_decisions["a"]["reasons"])
        self.assertFalse(analyzer.calls)

    async def test_missing_expectations_still_classifies_and_pins_actual_earnings(self):
        self.recorder.contexts["ZZTEST"] = {"company": context(self.published)["company"]}
        self.intake()
        async def extract(*args): return {"payload": deepcopy(PAYLOAD)}
        self.recorder.analyzer = TimedAnalyzer(extract, self.journal)
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertTrue(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertEqual(self.recorder.final_decisions["a"]["decision"], "skip")
        self.assertTrue(self.recorder.final_decisions["a"]["reasons"])

    async def test_correction_reprepares_context_before_first_publication(self):
        cutoffs = []
        old = self.published-SECOND
        class Store:
            def prepare(inner, ticker, publication_ns, *, mode):
                cutoffs.append((ticker, publication_ns, mode))
                return context(old)
        self.recorder.context_store = Store()
        self.intake("a", published=old)
        self.recorder.analyzer = AnalyzerSpy()
        await self.recorder.analyze_document("a", self.receipt, document(old), True)
        self.intake("b", published=self.published)
        await self.recorder.analyze_document("b", self.receipt, document(self.published), True)
        self.assertEqual(cutoffs, [("ZZTEST", old, "OBSERVED"), ("ZZTEST", self.published, "OBSERVED"), ("ZZTEST", old, "OBSERVED")])
        b = self.recorder.analyzer.calls[-1]
        self.assertEqual(b["published_ns"], old)
        self.assertEqual(b["document"]["announcement_clock"]["first_event_id"], "a")

    async def test_context_preparation_never_blocks_ingestion_and_its_time_counts(self):
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        prepared = context(self.published)
        class Store:
            def prepare(inner, *args, **kwargs):
                loop.call_soon_threadsafe(entered.set)
                release.wait(2)
                return prepared
        self.recorder.context_store = Store()
        self.recorder.analyzer = AnalyzerSpy()
        early = Stamp(self.receipt.utc_ns-DEADLINE_NS+80_000_000, self.receipt.mono_ns-DEADLINE_NS+80_000_000)
        self.intake("a", early)
        working = asyncio.create_task(self.recorder.analyze_document("a", early, document(self.published), True))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            self.intake("other", ticker="OTHER")
            self.assertIn("other", self.recorder.announcement_clocks.alerts)
            await asyncio.sleep(0.12)
            self.assertIn("a", self.recorder.decided)
        finally:
            release.set()
            await working
        self.assertFalse(self.recorder.analyzer.calls)
        self.assertTrue(self.recorder.announcement_clocks.alerts["a"].relevant)
        self.assertIn("five_second_deadline_expired", self.recorder.final_decisions["a"]["reasons"])

    async def test_prior_run_clock_does_not_reopen_after_new_id(self):
        self.intake()
        self.recorder.analyzer = AnalyzerSpy()
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        first_run = self.journal.run_id
        tasks = list(self.recorder.checkpoints)
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.journal.close()
        self.journal = Journal(self.path, {**run_metadata(["ZZTEST"], "sip"), "fixture_type": "SYNTHETIC_NETWORK_FREE_TEST"})
        self.recorder = Recorder(self.journal, ["ZZTEST"], analyze=True, contexts={"ZZTEST": context(self.published)})
        analyzer = self.recorder.analyzer = AnalyzerSpy()
        self.intake("new-id-b")
        await self.recorder.analyze_document("new-id-b", self.receipt, document(self.published), True)
        bound = self.recorder.announcement_clocks.binding("new-id-b")
        self.assertEqual(bound["first_run_id"], first_run)
        self.assertFalse(bound["comparable_in_current_run"])
        self.assertFalse(analyzer.calls)
        self.assertIn("first_relevant_receipt_from_prior_run_research_only", self.recorder.final_decisions["new-id-b"]["reasons"])

    async def test_failed_durability_barrier_prevents_coordinator_entry(self):
        self.recorder.paper_enabled = True
        self.recorder.analyzer = AnalyzerSpy()
        execution = self.recorder.paper_execution = ExecutionSpy(self.recorder)
        self.intake()
        async def unavailable(*args): return False
        with patch.object(self.recorder.announcement_clocks, "wait_durable", unavailable):
            await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertFalse(execution.calls)
        self.assertIn("announcement_clock_not_durable_or_changed_before_deadline", self.recorder.final_decisions["a"]["reasons"])

    async def test_analysis_completion_cannot_use_a_stale_clock_binding(self):
        self.intake()
        registry = self.recorder.announcement_clocks
        class ChangingAnalyzer(AnalyzerSpy):
            async def process(inner, *args, **kwargs):
                result = await super().process(*args, **kwargs)
                registry.classify("a", False)  # Monotonic conflict, never a later clock.
                return result
        self.recorder.analyzer = ChangingAnalyzer()
        await self.recorder.analyze_document("a", self.receipt, document(self.published), True)
        self.assertIn("announcement_clock_changed_during_analysis", self.recorder.final_decisions["a"]["reasons"])

    async def test_runtime_paper_is_explicit_and_shares_market_client_without_real_transport(self):
        for paper, mode in ((False, "candidates"), (False, "numeric"), (True, "candidates")):
            with self.subTest(paper=paper, mode=mode):
                calls, models, adapters = [], [], []
                adapter = object()
                recorder = Recorder(self.journal, ["ZZTEST"], analyze=not paper, paper=paper,
                    market_inputs=adapter, extractor_mode=mode, paper_state="SYNTHETIC_UNUSED_PATH")
                async def idle(*args): await asyncio.Event().wait()
                def candidates(client, *args):
                    models.append(("candidates", client))
                    return "SYNTHETIC_CANDIDATE_EXTRACTOR"
                def numeric(client, *args):
                    models.append(("numeric", client))
                    return "SYNTHETIC_NUMERIC_EXTRACTOR"
                def ledger(path, *, mode):
                    calls.append(("ledger", path, mode))
                    return "SYNTHETIC_LEDGER"
                def broker(client, journal, *, allow_orders):
                    calls.append(("broker", client, allow_orders))
                    return "SYNTHETIC_BROKER_NO_TRANSPORT"
                class Execution:
                    def __init__(inner, led, brok, journal, calendar, market, *, kill_path):
                        calls.append(("execution", led, brok, kill_path))
                        adapters.append(market)
                    async def run(inner):
                        calls.append(("lifecycle_started",))
                        await asyncio.Event().wait()
                with patch.dict("os.environ", {"ALPACA_API_KEY": "SYNTHETIC", "ALPACA_API_SECRET": "SYNTHETIC", "RTPR_API_KEY": "SYNTHETIC"}), \
                     patch.object(recorder, "wire", idle), patch.object(recorder, "market", idle), patch.object(recorder, "health", idle), \
                     patch("recorder.collectors.CandidateExtractor", candidates), patch("recorder.collectors.OpenAIExtractor", numeric), \
                     patch("recorder.collectors.TimedAnalyzer", lambda extractor, journal: object()), \
                     patch("recorder.collectors.PaperLedger", ledger), patch("recorder.collectors.PaperBroker", broker), \
                     patch("recorder.collectors.PaperExecution", Execution):
                    await recorder.run(0.01)
                self.assertEqual([kind for kind, _ in models], [mode])
                if paper:
                    self.assertTrue(recorder.analysis_enabled)
                    self.assertEqual(calls[0], ("ledger", "SYNTHETIC_UNUSED_PATH", "OBSERVED"))
                    self.assertEqual(calls[1][0], "broker")
                    self.assertIs(calls[1][1], models[0][1])
                    self.assertTrue(calls[1][2])
                    self.assertEqual(adapters, [adapter])
                    self.assertEqual(calls[-1], ("lifecycle_started",))
                else:
                    self.assertEqual(calls, [])
                    self.assertIsNone(recorder.paper_broker)


if __name__ == "__main__": unittest.main()
