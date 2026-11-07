"""Fabricated issuer/declaration fixtures, never observed market releases."""
import hashlib
import unittest

from recorder.issuer import verify_issuer


class IssuerTests(unittest.TestCase):
    def setUp(self):
        self.company={"ticker":"ZZTEST","exchange":"NYSE","canonical_company_id":"SYNTHETIC:issuer:123",
            "classification_verified":True,"issuer_identity_verified":True,"exchange_ticker_verified":True,
            "legal_issuer_name":"Example & Research, Inc.","verified_issuer_aliases":["Example & Research, Inc."]}

    def verify(self,text,company=None,**extra):
        return verify_issuer({"text":text,"full_text_verified":True,**extra},company or self.company)

    def test_direct_legal_name_and_exchange_are_exact_source_spans(self):
        text='SYNTHETIC release.\nCITY, Nov. 9, 2026 -- Example & Research, Inc. (NYSE: ZZTEST), a research platform, today announced financial results for the third quarter ended September 30, 2026.'
        result=self.verify(text)
        self.assertTrue(result["verified"],result)
        self.assertEqual(result["source_sha256"],hashlib.sha256(text.encode()).hexdigest())
        self.assertEqual({e["role"] for e in result["evidence"]},{"first_financial_results_declaration","verified_issuer_name","verified_exchange_ticker"})
        for e in result["evidence"]:self.assertEqual(text[e["start"]:e["end"]],e["passage"])

    def test_supported_direct_revenue_declaration_and_verified_exchange(self):
        text='SYNTHETIC\nCITY — Example (NYSE: ZZTEST) today reported revenue for the third quarter ended September 30, 2026 of 100 million USD.'
        result=self.verify(text)
        self.assertTrue(result["verified"],result)
        self.assertIn("verified_exchange_ticker",[e["role"] for e in result["evidence"]])

    def test_legal_name_without_ticker_can_bind(self):
        self.assertTrue(self.verify('SYNTHETIC\nExample & Research, Inc. announced financial results for the third quarter ended September 30, 2026.')["verified"])

    def test_source_ticker_membership_and_competitor_mention_do_not_bind(self):
        text='SYNTHETIC\nOther Company (NYSE: OTHER) today announced financial results for the third quarter ended September 30, 2026. Its competitor is Example & Research, Inc. (NYSE: ZZTEST).'
        result=self.verify(text,source_tickers=["OTHER","ZZTEST"])
        self.assertFalse(result["verified"])
        self.assertIn("first_declaration_exchange_ticker_conflicts",result["reasons"])

    def test_first_declaration_wins_even_when_later_issuer_is_correct(self):
        text='SYNTHETIC\nOther Company announced financial results for the third quarter ended September 30, 2026.\nExample & Research, Inc. (NYSE: ZZTEST) announced financial results for the third quarter ended September 30, 2026.'
        self.assertFalse(self.verify(text)["verified"])
        self.assertFalse(self.verify(text.replace('Other Company announced','Other Company posted'))["verified"])

    def test_future_schedule_is_not_actual_results(self):
        for text in (
            'Example & Research, Inc. (NYSE: ZZTEST) will report financial results for the third quarter on November 9.',
            'Example & Research, Inc. (NYSE: ZZTEST) announced it will release financial results for the third quarter on November 9.',
            'Example & Research, Inc. (NYSE: ZZTEST) is scheduled to report financial results for the third quarter.',
        ):
            with self.subTest(text=text):self.assertFalse(self.verify('SYNTHETIC\n'+text)["verified"])

    def test_legal_ads_and_secondary_attribution_fail(self):
        for text in (
            'SHAREHOLDER ALERT: Example & Research, Inc. (NYSE: ZZTEST) reported financial results for the third quarter ended September 30, 2026.',
            'Law firm announces investigation after Example & Research, Inc. (NYSE: ZZTEST) reported financial results for the third quarter ended September 30, 2026.',
            'Analyst said Example & Research, Inc. (NYSE: ZZTEST) reported financial results for the third quarter ended September 30, 2026.',
            '“Example & Research, Inc. (NYSE: ZZTEST) reported financial results for the third quarter ended September 30, 2026.”',
        ):
            with self.subTest(text=text):self.assertFalse(self.verify('SYNTHETIC\n'+text)["verified"])

    def test_joint_issuers_and_conflicting_ticker_fail(self):
        for subject in (
            'Example & Research, Inc. and Other Company (NYSE: ZZTEST)',
            'Example & Research, Inc. (NYSE: ZZTEST) and Other Company (NASDAQ: OTHER)',
            'Example & Research, Inc. (NASDAQ: ZZTEST)',
            'Example & Research, Inc., together with Other Company,',
            'Example & Research, Inc., and Another Company,',
            'Example & Research, Inc. (NYSE: ZZTEST), and Another Company,',
        ):
            with self.subTest(subject=subject):self.assertFalse(self.verify('SYNTHETIC\n'+subject+' today announced financial results for the third quarter ended September 30, 2026.')["verified"])

    def test_unverified_names_body_or_canonical_identity_fail(self):
        text='Example & Research, Inc. (NYSE: ZZTEST) announced financial results for the third quarter ended September 30, 2026.'
        for company in (
            self.company|{"issuer_identity_verified":False,"exchange_ticker_verified":False},
            self.company|{"classification_verified":False},
            self.company|{"canonical_company_id":None},
        ):
            with self.subTest(company=company):self.assertFalse(self.verify('SYNTHETIC\n'+text,company)["verified"])
        self.assertFalse(self.verify(text,full_text_verified=False)["verified"])

    def test_body_field_avoids_headline_or_navigation_identity(self):
        result=self.verify('Example & Research, Inc. (NYSE: ZZTEST) announced financial results for the third quarter.',
            article_body_text='SYNTHETIC\nOther Company (NYSE: OTHER) reported financial results for the third quarter ended September 30, 2026.')
        self.assertFalse(result["verified"])
        self.assertEqual(result["text_field"],"article_body_text")


if __name__ == '__main__':unittest.main()
