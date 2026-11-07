"""Fabricated source/availability fixtures; no real network or market observations."""
from decimal import Decimal
from datetime import datetime,time,timezone
import json
from pathlib import Path
import tempfile
import unittest

from recorder.context import ContextStore, company_from_asset, company_from_sec_cover, daily_liquidity, expected_liquidity_end, LIQUIDITY_POLICY, sec_reporting_currency_proof
from recorder.timebase import MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339
from zoneinfo import ZoneInfo


class Clock:
    def __init__(self):
        self.value = Stamp(parse_rfc3339("2026-11-01T12:00:00Z")[0], 100*SECOND)
    def now(self): return self.value
    def advance(self, ns): self.value = Stamp(self.value.utc_ns+ns, self.value.mono_ns+ns)


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = Clock()
        self.store = ContextStore(Path(self.tmp.name)/"synthetic-context.sqlite", mode="SYNTHETIC", clock=self.clock.now)
        self.source = "SYNTHETIC: All amounts are USD. GAAP EPS consensus is 1.20 for the quarter ended September 30, 2026. GAAP revenue consensus is 1000 million for the quarter ended September 30, 2026. GAAP revenue guidance for the fiscal year ending December 31, 2026 is 4200 million to 4300 million."
        self.doc = self.store.add_document("https://example.test/estimates", self.source.encode(), source_kind="SYNTHETIC")
        self.evidence = [{"document_id": self.doc, "type": "text", "passage": self.source}]

    def tearDown(self): self.tmp.cleanup()

    def financial(self, kind="eps", provider="SYNTHETIC_PROVIDER", value="1.20", **extra):
        payload = {"period_end": "2026-09-30", "period_kind": "QUARTER", "unit": "USD_PER_SHARE", "basis": "GAAP", "value": value,
                   "provider_series_id": provider, **extra}
        return self.store.add_fact("ZZTEST", kind, payload, document_ids=[self.doc], evidence=self.evidence)

    def prepared(self, cutoff=None):
        return self.store.prepare("ZZTEST", cutoff or self.clock.now().utc_ns+SECOND, actual_period_end="2026-09-30",
                                 guidance_period_end="2026-12-31", guidance_period_kind="FISCAL_YEAR", mode="SYNTHETIC")

    def test_import_cannot_backdate_system_availability(self):
        with self.assertRaisesRegex(ValueError, "availability_and_verification_are_derived"):
            self.financial(available_at="2020-01-01T00:00:00Z")
        self.financial()
        result = self.prepared(self.clock.now().utc_ns)
        self.assertNotIn("eps", result)
        self.assertIn("missing_point_in_time:eps", result["gaps"])

    def test_old_publication_claim_does_not_make_today_known_yesterday(self):
        yesterday = self.clock.now().utc_ns-86400*SECOND
        doc = self.store.add_document("https://example.test/old-release", self.source.encode(), source_kind="SYNTHETIC",
                                      source_claim={"published_at": "2020-01-01T00:00:00Z"})
        self.store.add_fact("ZZTEST", "eps", {"period_end":"2026-09-30","period_kind":"QUARTER","unit":"USD_PER_SHARE",
            "basis":"GAAP","value":"1.20","provider_series_id":"SYNTHETIC"}, document_ids=[doc],
            evidence=[{"document_id":doc,"type":"text","passage":self.source}])
        self.assertNotIn("eps", self.prepared(yesterday))

    def test_after_event_revision_cannot_replace_pre_event_snapshot(self):
        self.financial()
        cutoff = self.clock.now().utc_ns+SECOND
        self.clock.advance(2*SECOND)
        revised = self.source.replace("1.20", "1.30")
        doc = self.store.add_document("https://example.test/estimates", revised.encode(), source_kind="SYNTHETIC")
        self.store.add_fact("ZZTEST", "eps", {"period_end":"2026-09-30","period_kind":"QUARTER","unit":"USD_PER_SHARE",
            "basis":"GAAP","value":"1.30","provider_series_id":"SYNTHETIC_PROVIDER"}, document_ids=[doc],
            evidence=[{"document_id":doc,"type":"text","passage":revised}])
        self.assertEqual(self.prepared(cutoff)["eps"]["value"], "1.20")
        self.assertEqual(self.prepared()["eps"]["value"], "1.30")

    def test_provider_series_are_not_merged_or_cherry_picked(self):
        self.financial(provider="SYNTHETIC_A"); self.financial(provider="SYNTHETIC_B")
        result = self.prepared()
        self.assertNotIn("eps", result)
        self.assertIn("ambiguous_prepared_series:eps", result["gaps"])

    def test_expired_or_wrong_day_event_plan_cannot_grant_event_identity(self):
        for payload, reason in (
            ({"announcement_key":"SYNTHETIC:old", "expires_at":"2026-11-01T12:00:00Z"}, "expired_prepared_record:event_plan"),
            ({"announcement_key":"SYNTHETIC:other-day", "scheduled_date":"2026-10-31"}, "publication_day_does_not_match_prepared_event_plan"),
        ):
            with self.subTest(reason=reason):
                self.store.add_fact("ZZTEST", "event_plan", payload, document_ids=[self.doc], evidence=self.evidence)
                result = self.prepared()
                self.assertIn(reason, result["gaps"])
                self.assertIsNone(result["announcement_key"])
                self.assertFalse(result["ready"])

    def event_plan(self,**extra):
        payload={"announcement_key":"SYNTHETIC:issuer:2026-Q3","actual_period_end":"2026-09-30","actual_period_kind":"QUARTER",
                 "guidance_period_end":"2026-12-31","guidance_period_kind":"FISCAL_YEAR",**extra}
        return self.store.add_fact("ZZTEST","event_plan",payload,document_ids=[self.doc],evidence=self.evidence)

    def test_plan_targets_and_source_provenance_are_exposed(self):
        identifier=self.event_plan()
        result=self.store.prepare("ZZTEST",self.clock.now().utc_ns+SECOND,mode="SYNTHETIC")
        self.assertEqual([result[k] for k in ("actual_period_end","actual_period_kind","guidance_period_end","guidance_period_kind")],
                         ["2026-09-30","QUARTER","2026-12-31","FISCAL_YEAR"])
        self.assertEqual(result["event_plan"]["context_record_id"],identifier)
        self.assertEqual(result["event_plan"]["source_documents"],[self.doc])
        self.assertIn(self.doc,result["event_plan"]["source_versions"])
        self.assertEqual(result["event_plan"]["available_at"],iso_utc(self.clock.now().utc_ns))
        self.assertEqual(self.prepared()["announcement_key"],result["announcement_key"])

    def test_conflicting_explicit_targets_cannot_relabel_planned_event(self):
        self.event_plan()
        for field,value in {"actual_period_end":"2026-06-30","actual_period_kind":"FISCAL_YEAR",
                            "guidance_period_end":"2027-03-31","guidance_period_kind":"QUARTER"}.items():
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,"prepared_event_plan_target_conflict:"+field):
                self.store.prepare("ZZTEST",self.clock.now().utc_ns+SECOND,mode="SYNTHETIC",**{field:value})

    def test_default_quarter_does_not_override_annual_plan_and_late_plan_is_not_used(self):
        self.event_plan(actual_period_end="2026-12-31",actual_period_kind="FISCAL_YEAR")
        cutoff=self.clock.now().utc_ns
        result=self.store.prepare("ZZTEST",cutoff+SECOND,mode="SYNTHETIC")
        self.assertEqual(result["actual_period_kind"],"FISCAL_YEAR")
        self.assertEqual(result["actual_period_end"],"2026-12-31")
        unavailable=self.store.prepare("ZZTEST",cutoff,actual_period_end="2026-09-30",mode="SYNTHETIC")
        self.assertIsNone(unavailable["event_plan"])
        self.assertEqual(unavailable["actual_period_kind"],"QUARTER")
        self.assertEqual(unavailable["actual_period_end"],"2026-09-30")
        self.assertIsNone(unavailable["announcement_key"])

    def test_source_passage_and_numeric_evidence_are_enforced(self):
        with self.assertRaisesRegex(ValueError, "context_amount_not_in_selected_evidence"):
            self.financial(value="9.99")
        with self.assertRaisesRegex(ValueError, "context_passage_not_in_source"):
            self.store.add_fact("ZZTEST", "company", {}, document_ids=[self.doc], evidence=[{"document_id":self.doc,"type":"text","passage":"INVENTED"}])

    def test_currency_basis_and_period_are_not_guessed(self):
        for changes in ({"period_end":"2026-12-31"}, {"basis":"NON_GAAP"}, {"unit":"BILLION_USD"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.financial(**changes)
        source = self.source.replace("USD", "dollars")
        doc = self.store.add_document("https://example.test/no-usd", source.encode(), source_kind="SYNTHETIC")
        with self.assertRaisesRegex(ValueError, "context_usd_currency_not_supported"):
            self.store.add_fact("ZZTEST","eps",{"period_end":"2026-09-30","period_kind":"QUARTER","unit":"USD_PER_SHARE",
                "basis":"GAAP","value":"1.20","provider_series_id":"SYNTHETIC"},document_ids=[doc],
                evidence=[{"document_id":doc,"type":"text","passage":source}])

    def test_generic_non_gaap_cannot_grant_consensus_comparability(self):
        source=self.source.replace("GAAP EPS","non-GAAP EPS")
        doc=self.store.add_document("https://example.test/non-gaap",source.encode(),source_kind="SYNTHETIC")
        self.store.add_fact("ZZTEST","eps",{"period_end":"2026-09-30","period_kind":"QUARTER","unit":"USD_PER_SHARE",
            "basis":"NON_GAAP","value":"1.20","provider_series_id":"SYNTHETIC"},document_ids=[doc],
            evidence=[{"document_id":doc,"type":"text","passage":source}])
        result = self.prepared()
        self.assertFalse(result["eps"]["comparability_verified"])
        self.assertIn("unverified_metric_comparability:eps", result["gaps"])
        self.assertFalse(result["ready"])

    def test_mode_isolation_and_secret_urls(self):
        with self.assertRaises(ValueError): ContextStore(self.store.path, mode="OBSERVED")
        with self.assertRaises(ValueError): self.store.prepare("ZZTEST", self.clock.now().utc_ns+SECOND)
        for url in ("http://example.test", "https://user:secret@example.test", "https://example.test?api_key=SECRET"):
            with self.assertRaises(ValueError): self.store.add_document(url,b"x",source_kind="SYNTHETIC")

    def test_json_pointer_checks_exact_source_field(self):
        doc = self.store.add_document("https://example.test/asset", b'{"name":"SYNTHETIC Common Stock","tradable":true}',source_kind="SYNTHETIC",content_type="application/json")
        with self.assertRaisesRegex(ValueError,"context_json_value_not_in_source"):
            self.store.add_fact("ZZTEST","company",{},document_ids=[doc], evidence=[{"document_id":doc,"type":"json_pointer","pointer":"/tradable","value":False}])

    def test_explicit_two_source_definition_mapping_is_preserved_not_invented(self):
        provider=self.store.add_document("https://example.test/provider-definition",b"SYNTHETIC provider definition matches metric M",source_kind="SYNTHETIC")
        issuer=self.store.add_document("https://issuer.test/definition",b"SYNTHETIC issuer definition of metric M",source_kind="SYNTHETIC")
        mapping={"comparability_verified":True,"verified_by":"SYNTHETIC reviewer","methodology_id":"SYNTHETIC_M",
                 "issuer_metric_definition_id":"SYNTHETIC_M","definition_document_ids":[provider,issuer],
                 "methodology_evidence":[{"document_id":provider,"type":"text","passage":"SYNTHETIC provider definition matches metric M"},
                                        {"document_id":issuer,"type":"text","passage":"SYNTHETIC issuer definition of metric M"}]}
        self.store.add_fact("ZZTEST","eps",{"period_end":"2026-09-30","period_kind":"QUARTER","unit":"USD_PER_SHARE",
            "basis":"GAAP","value":"1.20","provider_series_id":"SYNTHETIC"},document_ids=[self.doc,provider,issuer],
            evidence=self.evidence,verification=mapping)
        result=self.prepared()["eps"]
        self.assertTrue(result["comparability_verified"])
        self.assertEqual(result["methodology_id"],"SYNTHETIC_M")
        self.assertEqual(len(result["methodology_evidence"]),2)
        self.assertEqual(set(result["source_versions"]),{self.doc,provider,issuer})

    def test_cents_conversion_is_deterministic_and_period_mapping_uses_issuer_calendar(self):
        text="SYNTHETIC third-quarter 2026 EPS consensus is 7 cents per share."
        source=self.store.add_document("https://example.test/cents",text.encode(),source_kind="SYNTHETIC")
        master={"tickers":["ZZTEST"],"fiscalYearEnd":"1231","filings":{"recent":{"form":["10-Q","10-Q"],"reportDate":["2026-03-31","2026-06-30"]}}}
        calendar_doc=self.store.add_document("https://example.test/calendar",json.dumps(master).encode(),source_kind="SYNTHETIC",content_type="application/json")
        payload={"period_end":"2026-09-30","period_kind":"QUARTER","unit":"UNSPECIFIED_CURRENCY_PER_SHARE","basis":"UNSPECIFIED",
                 "value":"0.07","provider_series_id":"SYNTHETIC","calculation":{"method_version":"cents_to_per_share_v1","reported_cents":"7"},
                 "period_mapping":{"method_version":"issuer_calendar_period_target_v1","calendar_document_id":calendar_doc,"fiscal_year":2026,"fiscal_quarter":3}}
        self.store.add_fact("ZZTEST","eps",payload,document_ids=[source,calendar_doc],evidence=[{"document_id":source,"type":"text","passage":text}])
        self.assertEqual(self.prepared()["eps"]["value"],"0.07")
        payload["period_mapping"]["fiscal_year"]=2027
        with self.assertRaises(ValueError):
            self.store.add_fact("ZZTEST","eps",payload,document_ids=[source,calendar_doc],evidence=[{"document_id":source,"type":"text","passage":text}])


class DerivedContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.asof = parse_rfc3339("2026-11-07T05:00:00Z")[0]
        cls.calendar = MarketCalendar(cls.asof)

    def test_asset_class_alone_never_becomes_verified_common_stock(self):
        base = {"symbol":"ZZTEST","class":"us_equity","exchange":"NASDAQ","status":"active","tradable":True}
        for name in ("Example ETF Trust", "Example Preferred Stock", "Example American Depositary Shares", "Example Fund Common Shares", "Example Corporation"):
            self.assertFalse(company_from_asset(base | {"name":name})["classification_verified"])
        common = company_from_asset(base | {"name":"Example Corporation Common Stock"})
        self.assertTrue(common["classification_verified"])
        self.assertIsNone(common["price_tick"])
        verified = company_from_asset(base | {"name":"Example Corporation Common Stock"},tick_source="SYNTHETIC broker documented precision")
        self.assertEqual(verified["price_tick"],"0.01")
        self.assertIn("not asserted venue minimum", verified["tick_meaning"])

    def test_sec_security_cover_fields_must_share_symbol_and_context(self):
        raw=b'''<ix:nonNumeric name="dei:TradingSymbol" contextRef="classA">ZZTEST</ix:nonNumeric>
            <ix:nonNumeric name="dei:EntityRegistrantName" contextRef="classA">SYNTHETIC Corporation</ix:nonNumeric>
            <ix:nonNumeric name="dei:Security12bTitle" contextRef="classA">Class A common stock</ix:nonNumeric>
            <ix:nonNumeric name="dei:SecurityExchangeName" contextRef="classA">New York Stock Exchange</ix:nonNumeric>'''
        asset={"symbol":"ZZTEST","name":"SYNTHETIC Corporation","class":"us_equity","exchange":"NYSE","status":"active","tradable":True}
        self.assertTrue(company_from_sec_cover(asset,raw)["classification_verified"])
        self.assertTrue(company_from_sec_cover(asset,raw)["issuer_identity_verified"])
        self.assertEqual(company_from_sec_cover(asset,raw)["verified_issuer_aliases"],["SYNTHETIC Corporation"])
        self.assertTrue(company_from_sec_cover(asset,raw)["exchange_ticker_verified"])
        self.assertFalse(company_from_sec_cover(asset,raw.replace(b'contextRef="classA">Class',b'contextRef="other">Class'))["classification_verified"])
        self.assertFalse(company_from_sec_cover(asset | {"symbol":"OTHER"},raw)["classification_verified"])

    def test_currency_proof_requires_same_cik_consolidated_revenue_and_eps_units(self):
        raw=b'''<!-- SYNTHETIC XBRL fixture -->
          <xbrli:context id="q"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000000123</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2026-04-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate></xbrli:period></xbrli:context>
          <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
          <xbrli:unit id="usdPerShare"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator></xbrli:divide></xbrli:unit>
          <ix:nonFraction unitRef="usd" contextRef="q" name="us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax">100</ix:nonFraction>
          <ix:nonFraction unitRef="usdPerShare" contextRef="q" name="us-gaap:EarningsPerShareDiluted">1</ix:nonFraction>'''
        proof=sec_reporting_currency_proof(raw,'123')
        self.assertEqual(proof['currency'],'USD')
        self.assertFalse(proof['automatic_comparison_currency_resolution'])
        for item in proof['proof']:self.assertEqual(raw.decode()[slice(*item['span'])],item['raw'])
        for changed,cik in ((raw,'456'),(raw.replace(b'iso4217:USD',b'iso4217:EUR'),'123'),
                            (raw.replace(b'</xbrli:entity>',b'<xbrldi:explicitMember>Segment</xbrldi:explicitMember></xbrli:entity>'),'123'),
                            (raw.replace(b'unitRef="usdPerShare" contextRef="q"',b'unitRef="usdPerShare" contextRef="other"'),'123')):
            with self.subTest(cik=cik,changed=changed[:80]):self.assertIsNone(sec_reporting_currency_proof(changed,cik))

    def bars(self):
        schedule = self.calendar.calendar.schedule
        past = [(label,row) for label,row in schedule.iterrows() if int(row["close"].value)<self.asof][-21:]
        return [{"t":datetime.combine(label.date(),time(),ZoneInfo("America/New_York")).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),"vw":"10","v":10000000} for label,row in past]

    def test_daily_proxy_is_explicit_and_needs_policy_selection(self):
        result = daily_liquidity(self.bars(),self.calendar,self.asof)
        self.assertEqual(result["session_count"],20)
        self.assertEqual(Decimal(result["average_daily_dollar_volume"]),100000000)
        self.assertTrue(result["dollar_volume_is_proxy"])
        self.assertTrue(result["measurement_policy_selected"])
        self.assertEqual(result["liquidity_policy"], LIQUIDITY_POLICY)
        self.assertIn("extended-hours",result["session_scope"])

    def test_missing_duplicate_and_unfinished_session_data_are_not_assumed_complete(self):
        bars=self.bars()
        with self.assertRaisesRegex(ValueError,"missing_completed_session_bar"):
            daily_liquidity(bars[-19:],self.calendar,self.asof)
        with self.assertRaisesRegex(ValueError,"conflicting_daily_bar_versions"):
            daily_liquidity(bars+[bars[-1] | {"v":1}],self.calendar,self.asof)
        # At 16:30 NY, today's full-day volume is not final; the window ends yesterday.
        late_regular_close=parse_rfc3339("2026-11-06T21:30:00Z")[0]
        result=daily_liquidity(bars,self.calendar,late_regular_close)
        self.assertEqual(result["last_session_close"],"2026-11-05T21:00:00.000000000Z")

    def test_liquidity_window_excludes_announcement_day_for_both_sessions(self):
        for timestamp in ("2026-11-09T13:00:00Z", "2026-11-09T21:05:00Z"):
            pub=parse_rfc3339(timestamp)[0]
            self.assertEqual(expected_liquidity_end(self.calendar,pub),parse_rfc3339("2026-11-06T21:00:00Z")[0])
        pub=parse_rfc3339("2026-11-06T21:05:00Z")[0]
        self.assertEqual(expected_liquidity_end(self.calendar,pub),parse_rfc3339("2026-11-05T21:00:00Z")[0])
