"""Fabricated alerts only: first-receipt semantics, not trading evidence."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from recorder.announcement_clock import AnnouncementClockRegistry, ny_release_day
from recorder.journal import Journal, read_rows
from recorder.timebase import SECOND, Stamp, parse_rfc3339


BASE = parse_rfc3339("2026-11-02T21:00:00Z")[0]
PERIOD = "2026-09-30"


def stamp(seconds=0, *, base=BASE, mono=100 * SECOND):
    return Stamp(base + seconds * SECOND, mono + seconds * SECOND)


class ClockTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "clock.sqlite"
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)

    def tearDown(self):
        self.journal.close()
        self.directory.cleanup()

    def alert(self, name, seconds, ticker="ZZTEST", published_ns=None):
        received = stamp(seconds)
        self.registry.observe_alert(name, ticker, received.utc_ns if published_ns is None else published_ns, received)
        return received

    def earnings(self, name, **kwargs):
        return self.registry.classify(name, True, PERIOD, **kwargs)

    def restart(self):
        self.journal.close()
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)

    def test_original_skipped_entry_and_new_id_correction_cannot_reset_clock(self):
        original = self.alert("opaque-original", 0)
        first = self.earnings("opaque-original")  # This can later fail any entry rule.
        self.alert("opaque-correction", 25, published_ns=BASE + 20 * SECOND)
        corrected = self.earnings("opaque-correction")
        self.assertEqual(corrected["effective_receipt"], original)
        self.assertEqual(corrected["publication_ns"], BASE)
        self.assertEqual(corrected["first_event_id"], "opaque-original")
        self.assertEqual(corrected["clock_key"], first["clock_key"])
        self.assertIsNone(corrected["block_reason"])
        self.assertGreater(stamp(25).mono_ns, corrected["effective_receipt"].mono_ns + 5 * SECOND)

    def test_legal_notice_before_earnings_does_not_start_clock(self):
        self.alert("legal", 0)
        self.registry.classify("legal", False)
        received = self.alert("earnings", 2)
        bound = self.earnings("earnings")
        self.assertEqual(bound["effective_receipt"], received)
        self.assertEqual(bound["first_event_id"], "earnings")
        self.assertIsNone(bound["block_reason"])

    def test_future_publication_is_conflicting_and_a_later_repeat_cannot_cure_it(self):
        received = self.alert("future-published", 0, published_ns=BASE+SECOND)
        first = self.earnings("future-published")
        self.assertIn("original_publication_after_first_receipt", first["block_reasons"])
        self.alert("future-published", 2, published_ns=BASE+SECOND)
        repeated = self.earnings("future-published")
        self.assertEqual(repeated["effective_receipt"], received)
        self.assertIn("original_publication_after_first_receipt", repeated["block_reasons"])

    def test_unknown_earlier_alert_blocks_admission_without_starting_later_clock(self):
        self.alert("unknown-a", 0)
        first_relevant = self.alert("earnings-b", 2)
        blocked = self.earnings("earnings-b")
        self.assertEqual(blocked["blocking_event_ids"], ["unknown-a"])
        self.assertIn("earlier_same_company_release_unclassified_or_unresolved", blocked["block_reasons"])
        self.assertEqual(blocked["effective_receipt"], first_relevant)
        self.alert("correction-c", 30)
        again = self.earnings("correction-c")
        self.assertEqual(again["effective_receipt"], first_relevant)
        self.assertEqual(again["blocking_event_ids"], ["unknown-a"])
        self.registry.classify("unknown-a", False)
        resolved = self.registry.binding("correction-c")
        self.assertIsNone(resolved["block_reason"])
        self.assertEqual(resolved["effective_receipt"], first_relevant)

    def test_out_of_order_document_completion_moves_canonical_clock_only_earlier(self):
        first = self.alert("slow-original-a", 0)
        self.alert("fast-document-b", 1)
        b_before = self.earnings("fast-document-b")
        self.assertTrue(b_before["blocking_event_ids"])
        a = self.earnings("slow-original-a")
        b_after = self.registry.binding("fast-document-b")
        self.assertEqual(a["clock_key"], b_before["clock_key"])
        self.assertEqual(b_after["effective_receipt"], first)
        self.assertFalse(self.registry.is_current(b_before))
        self.assertTrue(self.registry.is_current(b_after))
        self.assertEqual(b_after["first_arrival_order"], 1)

    def test_later_cik_key_upgrade_cannot_reset_ticker_period_clock_or_first_key(self):
        first = self.alert("a", 0)
        original = self.earnings("a")
        self.alert("b", 50)
        upgraded = self.earnings("b", announcement_key="CIK:12345:2026-09-30:Q")
        self.alert("c", 60)
        only_key = self.registry.classify("c", True, announcement_key="CIK:12345:2026-09-30:Q")
        self.assertEqual(only_key["effective_receipt"], first)
        self.assertEqual(only_key["clock_key"], original["clock_key"])
        self.assertEqual(upgraded["first_key"], "period:ZZTEST:QUARTER:" + PERIOD)
        self.registry.classify("a", True, PERIOD, announcement_key="CIK:12345:2026-09-30:Q")
        self.assertEqual(self.registry.binding("a")["first_key"], original["first_key"])

    def test_period_alias_joins_versions_across_release_days(self):
        first = self.alert("original", 0)
        self.earnings("original")
        self.alert("next-day-correction", 86400)
        bound = self.earnings("next-day-correction")
        self.assertEqual(bound["effective_receipt"], first)
        self.assertIsNone(bound["block_reason"])

    def test_restart_never_translates_old_monotonic_time_to_new_window(self):
        first = self.alert("a", 0)
        original = self.earnings("a")
        old_run = self.journal.run_id
        self.restart()
        new_receipt = Stamp(BASE + 30 * SECOND, 2 * SECOND)
        self.registry.observe_alert("new-id-b", "ZZTEST", BASE + 29 * SECOND, new_receipt)
        b = self.earnings("new-id-b")
        self.assertEqual(b["effective_receipt"], first)
        self.assertEqual(b["publication_ns"], BASE)
        self.assertEqual(b["first_run_id"], old_run)
        self.assertEqual(b["clock_key"], original["clock_key"])
        self.assertFalse(b["comparable_in_current_run"])
        self.assertIn("first_relevant_receipt_from_prior_run_research_only", b["block_reasons"])

    def test_restore_unknown_then_classified_legal_resolves_but_retains_original_b(self):
        self.alert("a", 0)
        first_b = self.alert("b", 1)
        self.earnings("b")
        self.restart()
        self.registry.classify("a", False)
        self.alert("c", 80)
        b = self.earnings("c")
        self.assertEqual(b["effective_receipt"], first_b)
        self.assertEqual(b["blocking_event_ids"], [])
        self.assertFalse(b["comparable_in_current_run"])

    def test_legacy_raw_alert_restores_as_unknown_and_prevents_new_id_reset(self):
        self.journal.emit("alert", {"event_id": "legacy-a", "ticker": "ZZTEST", "published_ns": BASE}, stamp(0))
        self.restart()
        self.alert("b", 2)
        b = self.earnings("b")
        self.assertEqual(b["blocking_event_ids"], ["legacy-a"])
        self.earnings("legacy-a")
        self.assertEqual(self.registry.binding("b")["effective_receipt"], stamp(0))
        self.assertFalse(self.registry.binding("b")["comparable_in_current_run"])

    def test_duplicate_alert_cannot_replace_first_receipt_or_original_publication(self):
        first = self.alert("a", 0, published_ns=BASE - SECOND)
        initial = self.earnings("a")
        self.alert("a", 500, published_ns=BASE + 499 * SECOND)
        duplicate = self.registry.binding("a")
        self.assertEqual(duplicate["effective_receipt"], first)
        self.assertEqual(duplicate["publication_ns"], BASE - SECOND)
        self.assertEqual(duplicate["first_arrival_order"], initial["first_arrival_order"])

    def test_false_reclassification_never_erases_relevant_clock(self):
        first = self.alert("a", 0)
        self.earnings("a")
        self.registry.classify("a", False)
        self.alert("b", 10)
        b = self.earnings("b")
        self.assertEqual(b["effective_receipt"], first)
        self.assertIn("announcement_classification_or_period_conflict", b["block_reasons"])

    def test_period_conflict_on_one_id_or_shared_stable_key_blocks(self):
        self.alert("a", 0)
        self.earnings("a", announcement_key="stable")
        self.alert("b", 1)
        b = self.registry.classify("b", True, "2026-06-30", announcement_key="stable")
        self.assertIn("announcement_classification_or_period_conflict", b["block_reasons"])
        self.registry.classify("a", True, "2026-06-30")
        self.assertIn("announcement_classification_or_period_conflict", self.registry.binding("a")["block_reasons"])

    def test_same_id_company_conflict_and_accidental_cross_company_key_block(self):
        self.alert("a", 0)
        self.earnings("a", announcement_key="stable")
        self.alert("a", 1, ticker="OTHER")
        self.assertIn("event_id_company_conflict", self.registry.binding("a")["block_reasons"])
        self.alert("b", 2, ticker="THIRD")
        b = self.earnings("b", announcement_key="stable")
        self.assertIn("event_id_company_conflict", b["block_reasons"])

    def test_newer_unknown_alert_never_blocks_earlier_known_release(self):
        first = self.alert("a", 0)
        self.earnings("a")
        self.alert("unknown-b", 1)
        self.assertEqual(self.registry.binding("a")["effective_receipt"], first)
        self.assertIsNone(self.registry.binding("a")["block_reason"])

    def test_publication_and_receipt_windows_both_block_delayed_unknown_alert(self):
        # Different receive days, but same original release day.
        self.alert("delayed-unknown", 0, published_ns=BASE)
        self.alert("b", 86400, published_ns=BASE + SECOND)
        self.assertEqual(self.earnings("b")["blocking_event_ids"], ["delayed-unknown"])

    def test_window_override_is_explicit_and_other_company_does_not_block(self):
        self.registry = AnnouncementClockRegistry(self.journal, window_key=lambda ns: "custom-always-same")
        self.alert("different-company", 0, ticker="OTHER")
        self.alert("same-company-prior-day", 1)
        self.alert("earnings", 86400)
        bound = self.earnings("earnings")
        self.assertEqual(bound["blocking_event_ids"], ["same-company-prior-day"])
        self.assertEqual(bound["release_window_policy"], "custom_window_key")

    def test_output_snapshot_cannot_mutate_registry_cache(self):
        self.alert("a", 0)
        original = self.earnings("a")
        original["identity_aliases"].append("garbage")
        original["durability_token"]["generation"] = -1
        fresh = self.registry.binding("a")
        self.assertNotIn("garbage", fresh["identity_aliases"])
        self.assertGreater(fresh["durability_token"]["generation"], 0)

    def test_missing_original_publication_stays_visible_after_correction(self):
        self.registry.observe_alert("a", "ZZTEST", None, stamp(0))
        self.earnings("a")
        self.alert("a", 1)
        self.alert("b", 5)
        b = self.earnings("b")
        self.assertIsNone(b["publication_ns"])
        self.assertIn("missing_original_publication_time", b["block_reasons"])

    def test_no_identity_relevant_alert_blocks_later_unjoined_release(self):
        self.alert("a", 0)
        self.registry.classify("a", True)
        self.alert("b", 1)
        self.assertEqual(self.earnings("b")["blocking_event_ids"], ["a"])

    def test_binding_restore_without_classification_still_pins_original(self):
        first = self.alert("a", 0)
        original = self.earnings("a")
        self.journal.close()
        # Simulate recovery from a journal whose binding survived but original
        # classification did not. This is a private synthetic fault fixture.
        import sqlite3
        with sqlite3.connect(self.path) as connection:
            connection.execute("DELETE FROM journal WHERE kind='announcement_clock_classification'")
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)
        self.alert("b", 10)
        b = self.earnings("b")
        self.assertEqual(b["effective_receipt"], first)
        self.assertEqual(b["clock_key"], original["clock_key"])
        self.assertFalse(b["comparable_in_current_run"])

    def test_pin_without_alert_remains_research_only_and_reports_missing_source(self):
        self.alert("a", 0)
        self.earnings("a")
        self.alert("b", 1)
        self.earnings("b")
        self.journal.close()
        import sqlite3
        with sqlite3.connect(self.path) as connection:
            connection.execute("DELETE FROM journal WHERE kind IN ('alert','announcement_clock_alert') AND json_extract(data,'$.event_id')='a'")
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)
        b = self.registry.binding("b")
        self.assertEqual(b["first_event_id"], "a")
        self.assertIn("restored_clock_source_alert_missing", b["block_reasons"])

    def test_binding_alone_restores_alias_and_prevents_fresh_new_id_clock(self):
        first = self.alert("a", 0)
        original = self.earnings("a")
        self.journal.close()
        import sqlite3
        with sqlite3.connect(self.path) as connection:
            connection.execute("DELETE FROM journal WHERE kind IN ('alert','announcement_clock_alert','announcement_clock_classification')")
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)
        self.alert("new-id-b", 80)
        b = self.earnings("new-id-b")
        self.assertEqual(b["clock_key"], original["clock_key"])
        self.assertEqual(b["effective_receipt"], first)
        self.assertFalse(b["comparable_in_current_run"])
        self.assertIn("restored_clock_source_alert_missing", b["block_reasons"])

    def test_invalid_inputs_reject_before_creating_partial_state(self):
        for event_id, ticker, published, received in (("", "ZZTEST", BASE, stamp()),
                ("a", "lower", BASE, stamp()), ("a", "ZZTEST", True, stamp()),
                ("a", "ZZTEST", 10**40, stamp())):
            with self.subTest(event_id=event_id, ticker=ticker, published=published):
                with self.assertRaises(ValueError):
                    self.registry.observe_alert(event_id, ticker, published, received)
        self.assertFalse(self.registry.alerts)
        with self.assertRaises(ValueError):
            self.registry.classify("unobserved", True, PERIOD)


class ReleaseDayTests(unittest.TestCase):
    def test_four_am_new_york_boundary_and_dst(self):
        # November 2 is standard time, November 1 crossed from EDT to EST.
        before = parse_rfc3339("2026-11-02T08:59:59Z")[0]
        boundary = parse_rfc3339("2026-11-02T09:00:00Z")[0]
        self.assertEqual(ny_release_day(before), "2026-11-01")
        self.assertEqual(ny_release_day(boundary), "2026-11-02")
        self.assertEqual(ny_release_day(parse_rfc3339("2026-10-30T08:00:00Z")[0]), "2026-10-30")


class DurabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "clock.sqlite"
        self.journal = Journal(self.path, {"mode": "SYNTHETIC"})
        self.registry = AnnouncementClockRegistry(self.journal)

    async def asyncTearDown(self):
        self.journal.close()
        self.directory.cleanup()

    def relevant(self, name, received=None):
        received = received or Stamp.now()
        self.registry.observe_alert(name, "ZZTEST", received.utc_ns, received)
        return self.registry.classify(name, True, PERIOD)

    async def test_durable_binding_is_committed_and_admission_must_still_be_current(self):
        bound = self.relevant("a")
        self.assertTrue(await self.registry.wait_durable(bound, Stamp.now().mono_ns + SECOND))
        committed = list(read_rows(self.path, kinds=["announcement_clock_binding"]))
        self.assertTrue(any(row["data"].get("binding_signature") == bound["binding_signature"] for row in committed))
        self.assertTrue(self.registry.is_current(bound))

    async def test_expired_or_foreign_durability_tokens_fail_closed(self):
        bound = self.relevant("a")
        self.assertFalse(await self.registry.wait_durable(bound, Stamp.now().mono_ns - 1))
        bound["durability_token"]["registry_instance"] = "different-process"
        self.assertFalse(await self.registry.wait_durable(bound, Stamp.now().mono_ns + SECOND))

    async def test_earlier_relevance_during_wait_invalidates_previous_binding(self):
        a = Stamp.now()
        self.registry.observe_alert("a", "ZZTEST", a.utc_ns, a)
        b = self.relevant("b", Stamp(a.utc_ns + 1, a.mono_ns + 1))
        self.registry.classify("a", True, PERIOD)
        self.assertFalse(await self.registry.wait_durable(b, Stamp.now().mono_ns + SECOND))
        current = self.registry.binding("b")
        self.assertTrue(await self.registry.wait_durable(current, Stamp.now().mono_ns + SECOND))
        self.assertEqual(current["effective_receipt"], a)


if __name__ == "__main__":
    unittest.main()
