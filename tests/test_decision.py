import asyncio
import copy
import unittest

from recorder.analysis import TimedAnalyzer, compare_earnings, validate_facts
from recorder.analysis_demo import PAYLOAD, SOURCE, context_for
from recorder.decision import evaluate_paper_intent, next_exit
from recorder.context import LIQUIDITY_POLICY
from recorder.timebase import MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339


class Clock:
    def __init__(self):
        self.stamp = Stamp(parse_rfc3339("2026-11-02T21:30:04Z")[0], 100*SECOND)
    def now(self):
        return self.stamp
    def advance(self, ns):
        self.stamp = Stamp(self.stamp.utc_ns+ns, self.stamp.mono_ns+ns)


def inputs_for(now, published):
    prepared = {"available_at": iso_utc(published-60*SECOND), "source": "SYNTHETIC fixture"}
    return {"company": {**prepared, "ticker": "ZZTEST", "security_type": "US_COMMON_STOCK", "exchange": "NASDAQ",
                        "tradable": True, "price_tick": "0.01", "tick_source": "SYNTHETIC fixture"},
            "liquidity": {**prepared, "session_count": 20, "average_daily_dollar_volume": "50000000",
                          "last_session_close": "2026-10-30T20:00:00Z", "liquidity_policy": LIQUIDITY_POLICY},
            "reference": {**prepared, "price": "10", "observed_at": iso_utc(published-61*SECOND), "reliable": True},
            "quote": {"ticker": "ZZTEST", "bid": "10.00", "ask": "10.02", "ask_size_shares": "200",
                      "source_utc": iso_utc(now.utc_ns), "available_utc": iso_utc(now.utc_ns),
                      "available_mono_ns": now.mono_ns, "coverage_verified": True, "conditions_eligible": True,
                      "size_unit_verified": True, "broker_session_supported": True, "market_status": "tradable",
                      "market_status_available_utc": iso_utc(now.utc_ns)},
            "portfolio": {"paper": True, "reconciled": True, "available_utc": iso_utc(now.utc_ns),
                          "available_mono_ns": now.mono_ns, "kill_switch": False, "review_required": False,
                          "equity": "100000", "peak_equity": "100000", "cash": "100000", "daily_pnl": "0", "daily_pause": False,
                          "positions": [], "entry_reservations": [], "entered_company_events": [],
                          "currency": "USD", "source": "SYNTHETIC fixture"}}


class DecisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = MarketCalendar(parse_rfc3339("2026-11-02T21:30:00Z")[0])

    def setUp(self):
        self.clock = Clock()
        self.receipt = self.clock.now()
        self.published = self.receipt.utc_ns-4*SECOND
        self.inputs = inputs_for(self.receipt, self.published)
        self.analysis = {"event_id": "SYNTHETIC-id", "ticker": "ZZTEST", "mode": "SYNTHETIC", "reasons": [],
                         "checks": {"eps_exceeds_consensus": True, "revenue_beats_consensus_by_two_pct": True,
                                    "revenue_guidance_midpoint_increases": True}}

    def evaluate(self):
        return evaluate_paper_intent(self.analysis, self.inputs, self.receipt, self.published, self.calendar, self.clock.now)

    def test_all_checks_make_research_intent_without_submitting(self):
        result = self.evaluate()
        self.assertEqual(result["reasons"], [])
        self.assertEqual(result["decision"], "research_intent_ready")
        self.assertFalse(result["orders_enabled"])
        intent = result["paper_intent"]
        self.assertEqual(intent["qty"], "99")
        self.assertEqual(intent["limit_price"], "10.02")
        self.assertEqual(intent["scheduled_exit_utc"], "2026-11-03T15:00:00.000000000Z")
        self.assertEqual(intent["client_order_id"], self.evaluate()["paper_intent"]["client_order_id"])

    def test_missing_every_required_group_fails_closed(self):
        for group in self.inputs:
            with self.subTest(group=group):
                changed = copy.deepcopy(self.inputs)
                del changed[group]
                result = evaluate_paper_intent(self.analysis, changed, self.receipt, self.published, self.calendar, self.clock.now)
                self.assertEqual(result["decision"], "skip")
                self.assertIsNone(result["paper_intent"])

    def test_future_publication_cannot_make_post_receipt_context_look_prepublication(self):
        self.published = self.receipt.utc_ns+2*SECOND
        self.clock.advance(SECOND)
        for key in ('company','liquidity','reference'):
            self.inputs[key]['available_at'] = iso_utc(self.receipt.utc_ns+SECOND//2)
        result = self.evaluate()
        self.assertIsNone(result['paper_intent'])
        self.assertFalse(result['checks']['publication_not_after_first_receipt'])
        self.assertIn('publication_timestamp_missing_or_after_first_receipt', result['reasons'])

    def test_delayed_alert_cannot_open_a_position_after_its_scheduled_exit(self):
        # A very late alert can arrive in a supported premarket window after the
        # original announcement's next-session exit has already passed.
        self.clock.stamp = Stamp(parse_rfc3339('2026-11-04T12:00:00Z')[0], 100*SECOND)
        self.receipt = self.clock.now()
        self.inputs = inputs_for(self.receipt, self.published)
        result = self.evaluate()
        self.assertIsNone(result['paper_intent'])
        self.assertTrue(result['checks']['current_extended_hours_session'])
        self.assertIn('scheduled_exit_still_in_future:failed', result['reasons'])

    def test_freshly_downloaded_stale_source_quote_is_rejected(self):
        self.inputs["quote"]["source_utc"] = iso_utc(self.receipt.utc_ns-SECOND-1)
        self.assertIn("quote_available_and_fresh:failed", self.evaluate()["reasons"])

    def test_future_quote_or_future_availability_is_rejected(self):
        for field in ("source_utc", "available_utc"):
            self.inputs["quote"][field] = iso_utc(self.receipt.utc_ns+1)
            self.assertEqual(self.evaluate()["decision"], "skip")

    def test_unverified_sizes_coverage_and_market_status_cannot_qualify(self):
        for field in ("size_unit_verified", "coverage_verified", "conditions_eligible", "broker_session_supported"):
            with self.subTest(field=field):
                self.inputs = inputs_for(self.receipt, self.published)
                self.inputs["quote"][field] = False
                self.assertEqual(self.evaluate()["decision"], "skip")

    def test_deadline_not_reset_by_decision_stage(self):
        self.clock.advance(5*SECOND)
        result = self.evaluate()
        self.assertIn("five_second_deadline_expired", result["reasons"])
        self.assertIsNone(result["paper_intent"])

    def test_daily_loss_drawdown_kill_switch_and_paper_only(self):
        for key, value in (("daily_pnl", "-500"), ("equity", "95000"), ("kill_switch", True),
                           ("review_required", True), ("paper", False), ("cash", "-1")):
            with self.subTest(key=key):
                self.inputs = inputs_for(self.receipt, self.published)
                self.inputs["portfolio"][key] = value
                self.assertEqual(self.evaluate()["decision"], "skip")

    def test_pending_orders_count_toward_capacity_and_duplicate_protection(self):
        p = self.inputs["portfolio"]
        p["entry_reservations"] = [{"ticker": s, "notional": "1000"} for s in ("AAA", "BBB", "CCC")]
        self.assertIn("portfolio_capacity:failed", self.evaluate()["reasons"])
        p["entry_reservations"] = [{"ticker": "ZZTEST", "notional": "1000"}]
        self.assertIn("one_entry_per_company_event:failed", self.evaluate()["reasons"])
        p["entry_reservations"] = []
        p["entered_company_events"] = [["ZZTEST", "SYNTHETIC-id"]]
        self.assertEqual(self.evaluate()["decision"], "skip")

    def test_reservations_reduce_cash_and_size(self):
        self.inputs["portfolio"]["cash"] = "1500"
        self.inputs["portfolio"].update(equity="1500", peak_equity="1500")
        self.inputs["portfolio"]["entry_reservations"] = [{"ticker": "AAA", "notional": "1000"}]
        self.assertEqual(self.evaluate()["paper_intent"]["qty"], "49")

    def test_unreconciled_or_leveraged_exposure_cannot_qualify(self):
        self.inputs["portfolio"]["positions"] = [{"ticker": "AAA", "qty": "100", "market_value": "10000"}]
        self.assertIn("cash_equity_positions_reconcile:failed", self.evaluate()["reasons"])
        self.inputs["portfolio"]["cash"] = "90000"
        self.assertIn("existing_position_exposure_within_cap:failed", self.evaluate()["reasons"])

    def test_visible_ask_size_caps_intended_quantity(self):
        self.inputs["quote"]["ask_size_shares"] = "2"
        self.assertEqual(self.evaluate()["paper_intent"]["qty"], "2")

    def test_thresholds_are_not_relaxed_for_a_candidate(self):
        for group, key, value in (("quote", "ask", "10.50"), ("quote", "ask", "10.51"),
                                  ("quote", "ask", "4.99"), ("quote", "ask_size_shares", "0"),
                                  ("liquidity", "session_count", 19), ("liquidity", "average_daily_dollar_volume", "49999999")):
            with self.subTest(key=key, value=value):
                self.inputs = inputs_for(self.receipt, self.published)
                self.inputs[group][key] = value
                self.assertEqual(self.evaluate()["decision"], "skip")

    def test_twenty_session_average_must_use_preselected_pre_announcement_day_window(self):
        self.inputs["liquidity"]["last_session_close"] = "2026-11-02T21:00:00Z"
        self.assertIn("liquidity_20_completed_sessions:failed", self.evaluate()["reasons"])

    def test_calendar_exits_for_premarket_holiday_weekend_and_early_close(self):
        for timestamp, expected in (("2026-11-02T13:00:00Z", "2026-11-02T15:00:00.000000000Z"),
                                    ("2026-11-25T22:00:00Z", "2026-11-27T15:00:00.000000000Z"),
                                    ("2026-11-27T19:00:00Z", "2026-11-30T15:00:00.000000000Z")):
            self.assertEqual(iso_utc(next_exit(self.calendar, parse_rfc3339(timestamp)[0])), expected)


class FullBudgetTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.calendar = MarketCalendar(parse_rfc3339("2026-11-02T21:30:00Z")[0])

    async def test_extraction_quote_and_risk_share_the_original_budget(self):
        clock = Clock()
        receipt = clock.now()
        published = receipt.utc_ns-4*SECOND
        async def load():
            clock.advance(300_000_000)
            return {"full_text_verified": True, "text": SOURCE}
        async def extract(*args):
            clock.advance(3*SECOND)
            return {"payload": copy.deepcopy(PAYLOAD)}
        async def inputs():
            clock.advance(500_000_000)
            return inputs_for(clock.now(), published)
        result = await TimedAnalyzer(extract, clock=clock.now).process("SYNTHETIC-id", "ZZTEST", receipt, load,
                    context=context_for(published), published_ns=published, mode="SYNTHETIC",
                    decision_inputs=inputs, calendar=self.calendar)
        self.assertEqual(result["decision"], "research_intent_ready")
        self.assertEqual(result["elapsed_from_receipt_ns"], 3_800_000_000)

    async def test_slow_market_fetch_cannot_create_late_intent(self):
        clock = Clock()
        receipt = clock.now()
        published = receipt.utc_ns-4*SECOND
        async def load():
            return {"full_text_verified": True, "text": SOURCE}
        async def extract(*args):
            clock.advance(4*SECOND)
            return {"payload": copy.deepcopy(PAYLOAD)}
        async def inputs():
            clock.advance(SECOND)
            return inputs_for(clock.now(), published)
        result = await TimedAnalyzer(extract, clock=clock.now).process("SYNTHETIC-id", "ZZTEST", receipt, load,
                    context=context_for(published), published_ns=published, decision_inputs=inputs,
                    calendar=self.calendar)
        self.assertEqual(result["decision"], "skip")
        self.assertIsNone(result["paper_intent"])
        self.assertIn("five_second_deadline_expired", result["reasons"])

    def test_unknown_currency_can_be_retained_but_never_match_usd_consensus(self):
        payload = copy.deepcopy(PAYLOAD)
        payload["facts"][0]["unit"] = "UNSPECIFIED_CURRENCY_PER_SHARE"
        payload["facts"][0]["evidence"] = payload["facts"][0]["evidence"][:1]
        facts, errors = validate_facts(payload, SOURCE, "ZZTEST")
        self.assertEqual(errors, [])
        published = Clock().now().utc_ns
        _, reasons = compare_earnings(facts, context_for(published), "ZZTEST", published)
        self.assertIn("eps:missing_or_conflicting_comparable_fact", reasons)
