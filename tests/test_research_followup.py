"""Fabricated OBSERVED-schema fixtures, temporary files and mock HTTP only.

These exercise prospective capture semantics; none are observed market results.
"""
import asyncio
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest
from unittest.mock import patch

import httpx

from recorder.journal import Journal, read_rows
from recorder.research_followup import HistoricalResearchFollowup, MIN_AGE_NS, MODE
from recorder.timebase import SECOND, MarketCalendar, Stamp, iso_utc, parse_rfc3339


def ns(text):
    return parse_rfc3339(text)[0]


def digest_fixture(value):
    return hashlib.sha256(("SYNTHETIC_TEST_ONLY:"+value).encode()).hexdigest()


RECEIPT = ns("2026-11-09T21:01:00.123456789Z")
PUBLICATION = ns("2026-11-09T21:00:00Z")
EXIT = ns("2026-11-10T15:00:00Z")  # 30 minutes after next XNYS open; 07:00 Los Angeles.
PERIOD = "2026-09-30"
META = {"mode": "OBSERVED", "fixture_type": "SYNTHETIC_UNIT_TEST_NETWORK_FREE",
        "receipt_boundary": "fabricated boundary for unit test", "local_clock_precision": "fixture only"}


class Clock:
    def __init__(self, utc_ns):
        self.utc_ns = utc_ns

    def __call__(self):
        return Stamp(self.utc_ns, 100 * SECOND)


def quote(at, ask="10.02"):
    return {"t": iso_utc(at), "bp": "10.00", "ap": ask, "bs": 3, "as": 4, "bx": "Q", "ax": "Q", "c": ["R"]}


class FollowupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.capture = Path(self.temp.name) / "capture.sqlite"
        self.root = Path(self.temp.name) / "historical-diagnostics"
        self.clock = Clock(RECEIPT + 10 * SECOND)
        self.calendar = MarketCalendar(RECEIPT)
        self.requests, self.responses = [], []
        self.gate = None

        async def respond(request):
            self.requests.append(request)
            # Durable immutable registration MUST precede even the first GET.
            with sqlite3.connect(self.root / "followup-state.sqlite") as c:
                self.assertEqual(c.execute("SELECT COUNT(*) FROM cases").fetchone()[0], 1)
                self.assertEqual(c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 2)
            if self.gate is not None:
                await self.gate.wait()
            response = self.responses.pop(0) if self.responses else httpx.Response(200, json={"quotes": {}, "next_page_token": None})
            if isinstance(response, Exception):
                raise response
            return response

        self.client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.auth = patch("recorder.collectors.alpaca_headers", return_value={"APCA-API-KEY-ID": "SYNTHETIC_TEST_ONLY"})
        self.auth.start()
        self.workers = []
        self.source = Journal(self.capture, META, started=Stamp(RECEIPT-SECOND, 1000))
        self.binding = self.make_binding(self.source.run_id)
        self.classification = self.make_classification(self.binding)
        self.emit_event(self.source, self.binding, self.classification)
        self.source.close()

    async def asyncTearDown(self):
        for worker in self.workers:
            await worker.aclose()
        await self.client.aclose()
        self.auth.stop()
        self.temp.cleanup()

    def make_binding(self, run_id, event="original", receipt=RECEIPT, publication=PUBLICATION):
        return {"event_id": event, "ticker": "HIMS", "relevant": True, "first_event_id": "original",
                "first_ticker": "HIMS", "first_run_id": run_id, "first_receipt_utc_ns": receipt,
                "first_receipt_mono_ns": 1000, "publication_ns": publication, "clock_key": "fixture-clock",
                "identity_aliases": ["event:original", f"period:HIMS:QUARTER:{PERIOD}"],
                "block_reasons": [], "comparable_in_current_run": True}

    @staticmethod
    def make_classification(binding):
        return {"event_id": binding["event_id"], "relevant": True,
                "reason": "primary_issuer_current_quarter_eps_and_revenue", "current_period_end": PERIOD,
                "issuer_identity": {"verified": True, "ticker": "HIMS", "canonical_company_id": "SEC_CIK:TEST_ONLY",
                                    "source_sha256": digest_fixture("source-v1"), "analysis_source_sha256": digest_fixture("version-v1")},
                "catalogue_source_sha256": digest_fixture("version-v1"), "announcement_clock": deepcopy(binding)}

    def emit_event(self, journal, binding, classification, *, emit_first=True):
        if emit_first:
            journal.emit("alert", {"event_id": "original", "ticker": "HIMS", "published_ns": binding["publication_ns"],
                                   "published_precision_ns": SECOND, "collector_source": "RTPR",
                                   "publication_meaning": "fixture vendor timestamp; first public availability unknown"},
                         Stamp(binding["first_receipt_utc_ns"], binding["first_receipt_mono_ns"]))
        journal.emit("announcement_clock_binding", binding, Stamp(RECEIPT + SECOND, 2000))
        journal.emit("source_classification", classification, Stamp(RECEIPT + 2*SECOND, 3000))
        journal.emit("decision", {"event_id": classification["event_id"], "decision": "skip",
                                  "reasons": ["missing_point_in_time_expectations", "five_second_deadline_expired"]})

    def append(self, kind, data):
        journal = Journal(self.capture, META)
        journal.emit(kind, data)
        journal.close()

    def worker(self, **kwargs):
        worker = HistoricalResearchFollowup(self.capture, self.root, self.client, symbols=["HIMS"],
                                            calendar=self.calendar, clock=self.clock, **kwargs)
        self.workers.append(worker)
        return worker

    async def due_entry(self, worker):
        self.clock.utc_ns = RECEIPT + 6 * SECOND + MIN_AGE_NS
        return await worker.run_due_once()

    async def test_preregisters_only_fixed_two_windows_independent_of_skipped_entry(self):
        worker = self.worker()
        scan = await worker.scan_once()
        self.assertEqual(scan["registered"], 1)
        self.assertEqual(self.requests, [])
        state = await worker.snapshot()
        self.assertEqual(state["mode"], MODE)
        self.assertFalse(state["qualified_trade"])
        plan = state["cases"][0]["plan"]
        self.assertEqual(plan["entry_targets_ns"], [RECEIPT, RECEIPT+5*SECOND])
        self.assertEqual(plan["scheduled_exit_ns"], EXIT)
        self.assertEqual(plan["source_pin"], ["original", self.source.run_id, RECEIPT, 1000])
        self.assertFalse(plan["earliest_public_availability_established"])
        self.assertEqual(plan["source_fixture_type"], META["fixture_type"])
        jobs = {j["kind"]: j for j in state["jobs"]}
        self.assertEqual((jobs["entry"]["start_ns"], jobs["entry"]["end_ns"]), (RECEIPT-SECOND, RECEIPT+6*SECOND))
        self.assertEqual((jobs["exit"]["start_ns"], jobs["exit"]["end_ns"]), (EXIT-2*SECOND, EXIT+2*SECOND))

    async def test_exact_sixteen_minute_floor_and_source_time_not_retrieval_time(self):
        worker = self.worker()
        self.clock.utc_ns = RECEIPT + 6*SECOND + MIN_AGE_NS - 1
        self.assertEqual(await worker.run_due_once(), [])
        self.responses.append(httpx.Response(200, json={"quotes": {"HIMS": [quote(RECEIPT), quote(RECEIPT+5*SECOND, "10.12")]}, "next_page_token": None}))
        outcomes = await self.due_entry(worker)
        self.assertEqual(len(outcomes), 1)
        request = self.requests[0]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.url.host, "data.alpaca.markets")
        self.assertEqual(request.url.params["feed"], "sip")
        self.assertEqual(request.url.params["start"], iso_utc(RECEIPT-SECOND))
        self.assertEqual(request.url.params["end"], iso_utc(RECEIPT+6*SECOND))
        self.assertEqual(request.url.params["asof"], "2026-11-09")
        snapshots = outcomes[0]["snapshots"]
        self.assertEqual(snapshots[iso_utc(RECEIPT+5*SECOND)]["ask"], "10.12")
        self.assertFalse(snapshots[iso_utc(RECEIPT)]["known_to_system_at_target"])
        self.assertEqual(outcomes[0]["retrieval_times_utc"], [iso_utc(self.clock.utc_ns)])
        rows = list(read_rows(outcomes[0]["archive_path"]))
        self.assertEqual(rows[0]["data"]["mode"], MODE)
        self.assertNotIn("quote", [r["kind"] for r in rows])
        page = next(r for r in rows if r["kind"] == "historical_page")
        self.assertEqual(page["stamp"].utc_ns, self.clock.utc_ns)
        self.assertEqual(json.loads(page["data"]["raw"])["quotes"]["HIMS"][0]["t"], iso_utc(RECEIPT))

    async def test_exit_has_only_preregistered_open_plus_thirty_minute_target(self):
        worker = self.worker()
        self.clock.utc_ns = EXIT + 2*SECOND + MIN_AGE_NS
        self.responses.extend([httpx.Response(200, json={"quotes": {}, "next_page_token": None}),
                               httpx.Response(200, json={"quotes": {"HIMS": [quote(EXIT)]}, "next_page_token": None})])
        results = await worker.run_due_once()
        self.assertEqual(len(results), 2)
        self.assertEqual(self.requests[1].url.params["start"], iso_utc(EXIT-2*SECOND))
        self.assertEqual(self.requests[1].url.params["end"], iso_utc(EXIT+2*SECOND))
        self.assertEqual(list(results[1]["snapshots"]), [iso_utc(EXIT)])
        self.assertFalse(results[1]["price_comparison_computed"])

    async def test_prior_run_original_boundary_is_preserved_for_research(self):
        b = deepcopy(self.binding)
        b.update(event_id="correction", comparable_in_current_run=False,
                 block_reasons=["first_relevant_receipt_from_prior_run_research_only"])
        b["identity_aliases"].append("event:correction")
        journal = Journal(self.capture, META)
        self.emit_event(journal, b, self.make_classification(b), emit_first=False)
        journal.close()
        worker = self.worker()
        await worker.scan_once()
        state = await worker.snapshot()
        self.assertEqual(len(state["cases"]), 1)
        self.assertEqual(len(state["jobs"]), 2)
        self.assertEqual(state["cases"][0]["plan"]["source_pin"][2], RECEIPT)

    async def test_correction_appends_evidence_without_new_jobs_or_plan_overwrite(self):
        worker = self.worker()
        await worker.scan_once()
        before = (await worker.snapshot())["cases"][0]["plan"]
        b = deepcopy(self.binding)
        b["event_id"] = "corrected-version"
        b["identity_aliases"].append("event:corrected-version")
        self.append("announcement_clock_binding", b)
        d = self.make_classification(b)
        d["issuer_identity"]["analysis_source_sha256"] = digest_fixture("version-v2")
        d["catalogue_source_sha256"] = digest_fixture("version-v2")
        self.append("source_classification", d)
        await worker.scan_once()
        state = await worker.snapshot()
        self.assertEqual(len(state["cases"]), 1)
        self.assertEqual(len(state["jobs"]), 2)
        self.assertEqual(state["cases"][0]["plan"], before)
        self.assertTrue(any(e["data"].get("issuer_identity", {}).get("analysis_source_sha256") == digest_fixture("version-v2") for e in state["evidence"]))

    async def test_late_earlier_binding_after_fetch_invalidates_without_reanchoring(self):
        worker = self.worker()
        results = await self.due_entry(worker)
        old = await worker.snapshot()
        archive = Path(results[0]["archive_path"])
        bytes_before = archive.read_bytes()
        b = deepcopy(self.binding)
        b.update(first_event_id="earlier-late-classified", first_receipt_utc_ns=RECEIPT-3*SECOND,
                 first_receipt_mono_ns=900, event_id="later-binding")
        self.append("announcement_clock_binding", b)
        await worker.scan_once()
        state = await worker.snapshot()
        self.assertEqual(state["cases"][0]["status"], "clock_conflict")
        self.assertEqual(state["cases"][0]["plan"], old["cases"][0]["plan"])
        self.assertEqual(archive.read_bytes(), bytes_before)
        self.assertEqual(next(j for j in state["jobs"] if j["kind"] == "exit")["status"], "cancelled_clock_conflict")
        self.assertEqual(len(state["jobs"]), 2)

    async def test_source_changes_are_rechecked_before_fetch_even_without_explicit_scan(self):
        worker = self.worker()
        await worker.scan_once()
        b = deepcopy(self.binding)
        b["first_receipt_utc_ns"] -= SECOND
        self.append("announcement_clock_binding", b)
        self.assertEqual(await self.due_entry(worker), [])
        self.assertEqual(self.requests, [])
        self.assertEqual((await worker.snapshot())["cases"][0]["status"], "clock_conflict")

    async def test_same_canonical_alias_with_conflicting_issuer_cannot_create_more_jobs(self):
        worker = self.worker()
        await worker.scan_once()
        d = deepcopy(self.classification)
        d["issuer_identity"]["canonical_company_id"] = "SEC_CIK:DIFFERENT_TEST_ISSUER"
        self.append("source_classification", d)
        await worker.scan_once()
        state = await worker.snapshot()
        self.assertEqual(len(state["cases"]), 1)
        self.assertEqual(len(state["jobs"]), 2)
        self.assertEqual(state["cases"][0]["status"], "clock_conflict")

    async def test_unresolved_clock_is_recorded_as_skip_without_fetch(self):
        b = deepcopy(self.binding)
        b["block_reasons"] = ["earlier_same_company_release_unclassified_or_unresolved"]
        self.append("announcement_clock_binding", b)
        worker = self.worker()
        await worker.scan_once()
        self.assertEqual((await worker.snapshot())["cases"], [])
        self.assertEqual(self.requests, [])

    async def test_issuer_and_catalogue_from_different_source_versions_are_skipped(self):
        worker = self.worker()
        # Replace the fixture classification before its first scan; both hashes
        # remain well formed, but the catalogue came from another full text.
        with closing(sqlite3.connect(self.capture)) as c:
            d = deepcopy(self.classification)
            d["catalogue_source_sha256"] = digest_fixture("different-full-text-version")
            c.execute("UPDATE journal SET data=? WHERE kind='source_classification'", (json.dumps(d),))
            c.commit()
        await worker.scan_once()
        state = await worker.snapshot()
        self.assertEqual(state["cases"], [])
        self.assertEqual(state["jobs"], [])
        self.assertEqual(self.requests, [])
        self.assertTrue(any("issuer_and_catalogue_document_version_mismatch" in e["reasons"]
                            for e in state["evidence"] if e["status"] == "skipped"))

    async def test_unknown_relevance_or_primary_identity_does_not_select_by_price(self):
        # Use separate isolated roots/capture journals for all source mutations.
        for i, update in enumerate([{"relevant": None}, {"issuer_identity": {"verified": False}},
                                    {"current_period_end": None}]):
            with self.subTest(update=update):
                path = Path(self.temp.name) / f"unqualified-{i}.sqlite"
                j = Journal(path, META)
                b = self.make_binding(j.run_id)
                d = self.make_classification(b) | update
                self.emit_event(j, b, d)
                j.close()
                worker = HistoricalResearchFollowup(path, Path(self.temp.name)/f"unqualified-history-{i}", self.client,
                                                     symbols=["HIMS"], calendar=self.calendar, clock=self.clock)
                self.workers.append(worker)
                await worker.scan_once()
                state = await worker.snapshot()
                self.assertEqual(state["cases"], [])
                self.assertTrue(any(e["status"] == "skipped" for e in state["evidence"]))

    async def test_non_observed_source_database_is_refused_without_requests(self):
        self.append("run_start", {"mode": "SYNTHETIC"})
        worker = self.worker()
        with self.assertRaisesRegex(ValueError, "strictly_OBSERVED"):
            await worker.run_due_once()
        self.assertEqual(self.requests, [])

    async def test_calendar_rejects_regular_weekend_and_overnight_publication(self):
        for i, published in enumerate([ns("2026-11-09T18:00:00Z"), ns("2026-11-08T21:00:00Z"), ns("2026-11-09T07:00:00Z")]):
            path = Path(self.temp.name) / f"calendar-{i}.sqlite"
            j = Journal(path, META)
            b = self.make_binding(j.run_id, publication=published)
            self.emit_event(j, b, self.make_classification(b))
            j.close()
            worker = HistoricalResearchFollowup(path, Path(self.temp.name)/f"calendar-history-{i}", self.client,
                                                 symbols=["HIMS"], calendar=self.calendar, clock=self.clock)
            self.workers.append(worker)
            await worker.scan_once()
            self.assertEqual((await worker.snapshot())["cases"], [])

    async def test_holiday_and_early_close_exit_use_exchange_calendar(self):
        # Friday after Thanksgiving's 13:00 ET close: next open is Monday.
        receipt = ns("2026-11-27T18:01:00Z")
        path = Path(self.temp.name) / "early-close.sqlite"
        j = Journal(path, META)
        b = self.make_binding(j.run_id, receipt=receipt, publication=receipt-SECOND)
        self.emit_event(j, b, self.make_classification(b))
        j.close()
        worker = HistoricalResearchFollowup(path, Path(self.temp.name)/"holiday-history", self.client,
                                             symbols=["HIMS"], clock=Clock(receipt+SECOND))
        self.workers.append(worker)
        await worker.scan_once()
        plan = (await worker.snapshot())["cases"][0]["plan"]
        self.assertEqual(plan["scheduled_exit_ns"], ns("2026-11-30T15:00:00Z"))

    async def test_pagination_retains_empty_page_and_actual_retrieval_domains(self):
        worker = self.worker()
        self.responses.extend([httpx.Response(200, json={"quotes": {}, "next_page_token": "opaque-next"}),
                               httpx.Response(200, json={"quotes": {"HIMS": [quote(RECEIPT)]}, "next_page_token": None})])
        result = (await self.due_entry(worker))[0]
        self.assertTrue(result["complete"])
        self.assertEqual(result["pages"], 2)
        self.assertEqual(self.requests[1].url.params["page_token"], "opaque-next")
        self.assertEqual(len(result["retrieval_times_utc"]), 2)
        self.assertEqual(len([r for r in read_rows(result["archive_path"]) if r["kind"] == "historical_page"]), 2)

    async def test_page_budget_never_exposes_a_partial_snapshot_as_result(self):
        worker = self.worker(max_pages=1)
        self.responses.append(httpx.Response(200, json={"quotes": {"HIMS": [quote(RECEIPT)]}, "next_page_token": "remaining"}))
        result = (await self.due_entry(worker))[0]
        self.assertFalse(result["complete"])
        self.assertEqual(result["status"], "page_budget_exhausted")
        self.assertTrue(all(q["status"] == "missing_incomplete_archive" for q in result["snapshots"].values()))
        self.assertFalse(result["price_comparison_computed"])
        self.assertEqual(len(self.requests), 1)

    async def test_repeated_cursor_and_unexpected_schema_remain_incomplete(self):
        worker = self.worker()
        self.responses.extend([httpx.Response(200, json={"quotes": {}, "next_page_token": "same"}),
                               httpx.Response(200, json={"quotes": {}, "next_page_token": "same"})])
        result = (await self.due_entry(worker))[0]
        self.assertEqual(result["status"], "invalid_or_repeated_page_token")
        self.assertFalse(result["complete"])

    async def test_access_denial_is_not_retried_and_error_body_not_archived(self):
        worker = self.worker()
        self.responses.append(httpx.Response(403, text="SECRET_KEY_MUST_NOT_APPEAR"))
        result = (await self.due_entry(worker))[0]
        self.assertEqual(result["status"], "access_denied")
        self.assertEqual(len(self.requests), 1)
        self.clock.utc_ns += 5*60*SECOND
        self.assertEqual(await worker.run_due_once(), [])
        archived = json.dumps(list(read_rows(result["archive_path"])), default=str)
        self.assertNotIn("SECRET_KEY_MUST_NOT_APPEAR", archived)

    async def test_network_retry_is_bounded_delayed_and_each_attempt_append_only(self):
        worker = self.worker()
        self.responses.append(httpx.ReadTimeout("secret-url?api_key=SECRET"))
        first = (await self.due_entry(worker))[0]
        first_bytes = Path(first["archive_path"]).read_bytes()
        self.assertEqual(first["status"], "network_error")
        self.assertEqual(await worker.run_due_once(), [])
        self.clock.utc_ns += 60*SECOND
        second = (await worker.run_due_once())[0]
        self.assertTrue(second["complete"])
        self.assertNotEqual(first["archive_path"], second["archive_path"])
        self.assertEqual(Path(first["archive_path"]).read_bytes(), first_bytes)
        state = await worker.snapshot()
        self.assertEqual([a["ordinal"] for a in state["attempts"]], [1, 2])
        self.assertNotIn("api_key=SECRET", json.dumps(state))

    async def test_retry_budget_cannot_expand_after_restart(self):
        worker = self.worker()
        self.responses = [httpx.ConnectError("fixture") for _ in range(3)]
        await self.due_entry(worker)
        for _ in range(2):
            self.clock.utc_ns += 60*SECOND
            await worker.run_due_once()
        await worker.aclose()
        restarted = self.worker()
        self.clock.utc_ns += 60*SECOND
        self.assertEqual(await restarted.run_due_once(), [])
        state = await restarted.snapshot()
        self.assertEqual(len(self.requests), 3)
        self.assertEqual(len(state["attempts"]), 3)

    async def test_restart_recovers_abandoned_claim_without_overwriting_attempt(self):
        worker = self.worker()
        await worker.scan_once()
        self.clock.utc_ns = RECEIPT + 6*SECOND + MIN_AGE_NS
        claimed = await asyncio.to_thread(worker._claim)
        await worker.aclose()
        restarted = self.worker()
        self.assertEqual(await restarted.run_due_once(), [])
        self.clock.utc_ns += 60*SECOND
        await restarted.run_due_once()
        state = await restarted.snapshot()
        self.assertEqual(len(state["attempts"]), 2)
        self.assertEqual(state["attempts"][0]["status"], "interrupted")
        self.assertEqual(state["attempts"][0]["path"], claimed["path"])

    async def test_cancellation_propagates_and_retry_state_is_preserved(self):
        worker = self.worker()
        self.gate = asyncio.Event()
        task = asyncio.create_task(self.due_entry(worker))
        for _ in range(100):
            if self.requests:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(len(self.requests), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        state = await worker.snapshot()
        self.assertEqual(state["attempts"][0]["result"]["status"], "interrupted")
        self.assertEqual(state["jobs"][0]["status"], "retry")

    async def test_complete_jobs_do_not_refetch_after_restart(self):
        worker = self.worker()
        await self.due_entry(worker)
        await worker.aclose()
        restarted = self.worker()
        self.assertEqual(await restarted.run_due_once(), [])
        self.assertEqual(len(self.requests), 1)

    async def test_state_and_archives_are_private_and_do_not_mix_source_modes(self):
        worker = self.worker()
        results = await self.due_entry(worker)
        self.assertEqual(stat.S_IMODE(self.root.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(worker.state_path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(Path(results[0]["archive_path"]).stat().st_mode), 0o600)
        self.assertEqual({r["data"]["mode"] for r in read_rows(self.capture) if r["kind"] == "run_start"}, {"OBSERVED"})
        self.assertEqual({r["data"]["mode"] for r in read_rows(results[0]["archive_path"]) if r["kind"] == "run_start"}, {MODE})

    async def test_shared_client_remains_open_and_successful_empty_window_is_visible(self):
        worker = self.worker()
        result = (await self.due_entry(worker))[0]
        self.assertEqual(result["status"], "empty_window")
        self.assertTrue(all(v["status"] == "missing" for v in result["snapshots"].values()))
        await worker.aclose()
        self.assertFalse(self.client.is_closed)

    async def test_polling_research_error_does_not_terminate_run(self):
        worker = self.worker()
        calls = 0

        async def fail_once():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("do not log this exception text")
            raise asyncio.CancelledError

        with patch.object(worker, "run_due_once", side_effect=fail_once), patch("recorder.research_followup.asyncio.sleep", return_value=None):
            with self.assertRaises(asyncio.CancelledError):
                await worker.run()
        self.assertEqual(calls, 2)

    async def test_foreign_meta_database_is_rejected_without_any_database_mutation(self):
        self.root.mkdir()
        foreign = self.root / "followup-state.sqlite"
        with closing(sqlite3.connect(foreign)) as c:
            c.executescript("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO meta VALUES ('mode','PAPER'); CREATE TABLE private_broker_rows(value TEXT);")
            c.commit()
        foreign.chmod(0o640)
        original, permissions = foreign.read_bytes(), foreign.stat().st_mode
        worker = self.worker()
        with self.assertRaisesRegex(ValueError, "configuration_or_mode_mismatch"):
            await worker.scan_once()
        self.assertEqual(foreign.read_bytes(), original)
        self.assertEqual(foreign.stat().st_mode, permissions)
        self.assertFalse(Path(str(foreign)+"-wal").exists())
        self.assertEqual(self.requests, [])

    async def test_polling_closes_both_source_and_state_connections(self):
        connections = []
        original_connect = sqlite3.connect

        class TrackedConnection(sqlite3.Connection):
            closed = False

            def close(self):
                self.closed = True
                return super().close()

        def track(*args, **kwargs):
            c = original_connect(*args, **kwargs, factory=TrackedConnection)
            connections.append(c)
            return c

        worker = self.worker()
        with patch("recorder.research_followup.sqlite3.connect", side_effect=track):
            await worker.scan_once()
            await worker.scan_once()
            await worker.snapshot()
        self.assertGreater(len(connections), 3)
        self.assertTrue(all(c.closed for c in connections))

    async def test_oversized_response_is_bounded_and_cannot_publish_quotes(self):
        worker = self.worker()
        self.responses.append(httpx.Response(200, content=b"x" * 2048))
        with patch("recorder.research_followup.MAX_PAGE_BYTES", 1024):
            result = (await self.due_entry(worker))[0]
        self.assertEqual(result["status"], "response_budget_exhausted")
        self.assertFalse(result["complete"])
        page = next(r for r in read_rows(result["archive_path"]) if r["kind"] == "historical_page")
        self.assertFalse(page["data"]["body_complete"])
        self.assertIsNone(page["data"]["raw"])
        self.assertTrue(all(s["status"] == "missing_incomplete_archive" for s in result["snapshots"].values()))

    async def test_quote_timestamp_outside_window_or_unknown_symbol_is_not_accepted(self):
        worker = self.worker()
        self.responses.append(httpx.Response(200, json={"quotes": {"HIMS": [quote(RECEIPT-2*SECOND)]}, "next_page_token": None}))
        result = (await self.due_entry(worker))[0]
        self.assertEqual(result["status"], "unexpected_schema")
        self.assertFalse(result["complete"])
        self.assertTrue(all(s["status"] == "missing_incomplete_archive" for s in result["snapshots"].values()))

    async def test_clock_correction_arriving_during_http_suppresses_snapshot_publication(self):
        worker = self.worker()
        self.gate = asyncio.Event()
        self.responses.append(httpx.Response(200, json={"quotes": {"HIMS": [quote(RECEIPT)]}, "next_page_token": None}))
        task = asyncio.create_task(self.due_entry(worker))
        for _ in range(100):
            if self.requests:
                break
            await asyncio.sleep(0.001)
        b = deepcopy(self.binding)
        b.update(first_event_id="earlier-while-downloading", first_receipt_utc_ns=RECEIPT-SECOND)
        self.append("announcement_clock_binding", b)
        self.gate.set()
        result = (await task)[0]
        self.assertEqual(result["status"], "clock_conflict")
        self.assertTrue(result["archive_pages_complete"])
        self.assertFalse(result["case_research_eligible"])
        self.assertEqual(result["snapshots"], {})
        rows = list(read_rows(result["archive_path"]))
        self.assertTrue(any(r["kind"] == "historical_page" and r["data"]["raw"] for r in rows))
        recorded = next(r["data"] for r in rows if r["kind"] == "followup_historical_result")
        self.assertEqual(recorded["snapshots"], {})
        self.assertEqual((await worker.snapshot())["cases"][0]["status"], "clock_conflict")

    async def test_state_configuration_change_after_restart_fails_closed(self):
        worker = self.worker()
        await worker.scan_once()
        before = worker.state_path.read_bytes()
        await worker.aclose()
        other = HistoricalResearchFollowup(self.capture, self.root, self.client,
                                             symbols=["NVDA"], calendar=self.calendar, clock=self.clock)
        self.workers.append(other)
        with self.assertRaisesRegex(ValueError, "configuration_or_mode_mismatch"):
            await other.scan_once()
        self.assertEqual(worker.state_path.read_bytes(), before)
        self.assertEqual(self.requests, [])


class FollowupConfigTests(unittest.TestCase):
    def test_explicit_bounded_watchlist_and_finite_budgets(self):
        class Client:
            async def get(self, *args, **kwargs):
                raise AssertionError("network forbidden")
        for symbols in ([], ["HIMS"]*2, ["A", "B", "C", "D"], ["bad symbol"]):
            with self.assertRaises(ValueError):
                HistoricalResearchFollowup("capture", "history", Client(), symbols=symbols)
        for kwargs in ({"interval_s": 1}, {"interval_s": float("nan")}, {"max_pages": 0},
                       {"max_attempts": 4}, {"retry_s": 1}):
            with self.assertRaises(ValueError):
                HistoricalResearchFollowup("capture", "history", Client(), symbols=["HIMS"], **kwargs)


if __name__ == "__main__":
    unittest.main()
