"""Fabricated source fixtures only; actual primary releases are separate diagnostics."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

import httpx

from recorder.analysis import validate_facts,compare_earnings
from recorder.analysis import TimedAnalyzer
from recorder.analysis_demo import SOURCE, context_for
from recorder.candidates import CandidateError, CandidateExtractor, FinancialTables, build_catalogue, select_payload
from recorder.case_study import parse_exhibit
from recorder.timebase import Stamp,SECOND


def all_selected(catalogue):
    return {"ticker": catalogue["ticker"], "selected_ids": catalogue["preferred_ids"], "rejected_ids": [], "missing_information": []}


class CatalogueTests(unittest.TestCase):
    def test_clear_prose_fixes_numbers_periods_units_basis_and_exact_evidence(self):
        catalogue = build_catalogue(SOURCE, 'ZZTEST')
        self.assertEqual(catalogue['fatal_errors'], [])
        payload = select_payload(catalogue, all_selected(catalogue))
        facts, errors = validate_facts(payload, SOURCE, 'ZZTEST')
        self.assertEqual(errors, [])
        self.assertEqual([(f['name'],f['value'],f['basis'],f['period_end']) for f in facts], [
            ('DILUTED_EPS','1.25','NON_GAAP','2026-06-30'),('REVENUE','1040','GAAP','2026-06-30'),
            ('REVENUE_GUIDANCE_LOW','4400','GAAP','2026-12-31'),('REVENUE_GUIDANCE_HIGH','4600','GAAP','2026-12-31')])
        for fact in facts:
            for span in fact['evidence_spans']:
                self.assertEqual(SOURCE[span['start']:span['end']],span['text'])

    def test_dollar_sign_never_establishes_usd_and_ordinary_eps_is_not_assumed_gaap(self):
        source = 'ZZTEST reports results for the quarter ended June 30, 2026.\nDiluted EPS was $0.18 versus $0.31, while non-GAAP diluted EPS was $0.36 versus $0.45.\nRevenue was $174.5 million.'
        catalogue = build_catalogue(source,'ZZTEST')
        payload = select_payload(catalogue,all_selected(catalogue))
        self.assertEqual([(f['value'],f['basis']) for f in payload['facts'] if f['name']=='DILUTED_EPS'], [('0.18','UNSPECIFIED'),('0.36','NON_GAAP')])
        self.assertTrue(all('UNSPECIFIED_CURRENCY' in f['unit'] for f in payload['facts']))
        self.assertFalse(any(f['value'] in ('0.31','0.45') for f in payload['facts']))

    def test_annual_and_explicit_prior_period_prose_are_not_current_quarter_actuals(self):
        source = ('ZZTEST reports results for the quarter ended June 30, 2026.\n'
                  'For the fiscal year 2026, revenue was $900 million.\n'
                  'Revenue for the quarter ended June 30, 2025 was $80 million.\n'
                  'Revenue for the quarter ended June 30, 2026 was $100 million.\n'
                  'For the quarter ended June 30, 2025, diluted EPS was $0.50.\n'
                  'For the quarter ended June 30, 2026, diluted EPS was $0.60.')
        catalogue=build_catalogue(source,'ZZTEST')
        payload=select_payload(catalogue,all_selected(catalogue))
        self.assertEqual({f['value'] for f in payload['facts']},{'100','0.60'})

    def test_table_binds_current_quarter_not_prior_year_or_same_end_annual(self):
        raw = b'''<p>ZZTEST reports results for the quarter ended September 30, 2026.</p>
<p>(In millions, except per-share amounts)</p>
<table><tr><th></th><th colspan="2">Three Months Ended</th><th>Twelve Months Ended</th></tr>
<tr><th></th><th>September 30, 2026</th><th>September 30, 2025</th><th>September 30, 2026</th></tr>
<tr><td>Total revenue</td><td>100</td><td>90</td><td>400</td></tr>
<tr><td>Earnings per share:</td><td></td><td></td><td></td></tr>
<tr><td>Diluted</td><td>1.25</td><td>1.10</td><td>4.99</td></tr>
<tr><td>Shares used in computing earnings per share:</td><td></td><td></td><td></td></tr>
<tr><td>Diluted</td><td>100000</td><td>90000</td><td>400000</td></tr></table>'''
        source,_=parse_exhibit(raw)
        catalogue=build_catalogue(source,'ZZTEST',raw)
        self.assertEqual(catalogue['fatal_errors'],[])
        payload=select_payload(catalogue,all_selected(catalogue))
        self.assertEqual({(f['name'],f['value']) for f in payload['facts']},{('REVENUE','100'),('DILUTED_EPS','1.25')})
        self.assertEqual(validate_facts(payload,source,'ZZTEST')[1],[])
        self.assertTrue(all(c['provenance']['column_period_verified'] for c in catalogue['candidates']))

    def test_split_month_day_and_year_headers_bind_to_the_same_column(self):
        raw=b'''<p>ZZTEST reports results for the quarter ended September 30, 2026.</p>
<table><tr><th></th><th colspan="2">Three Months Ended September 30,</th><th>Six Months Ended September 30,</th></tr>
<tr><th></th><th>2026</th><th>2025</th><th>2026</th></tr><tr><td colspan="4">(In thousands)</td></tr>
<tr><td>Revenue</td><td>100000</td><td>90000</td><td>200000</td></tr></table>'''
        source,_=parse_exhibit(raw)
        catalogue=build_catalogue(source,'ZZTEST',raw)
        payload=select_payload(catalogue,all_selected(catalogue))
        self.assertEqual([(f['value'],f['period_end']) for f in payload['facts']],[('100000','2026-09-30')])
        self.assertEqual(validate_facts(payload,source,'ZZTEST')[1],[])

    def test_explicit_table_eps_basis_uses_recomputed_row_and_column_proof(self):
        raw=b'''<p>ZZTEST reports results for the quarter ended September 30, 2026.</p>
<table><tr><th></th><th>Three Months Ended</th><th>Three Months Ended</th></tr>
<tr><th></th><th>September 30, 2026</th><th>September 30, 2025</th></tr>
<tr><td>Diluted net income per share</td><td></td><td></td></tr>
<tr><td>GAAP</td><td>1.25</td><td>1.10</td></tr><tr><td>Non-GAAP*</td><td>1.40</td><td>1.30</td></tr>
<tr><td>Weighted average shares used in diluted EPS</td><td>100000</td><td>90000</td></tr></table>'''
        source,_=parse_exhibit(raw);catalogue=build_catalogue(source,'ZZTEST',raw);selection=all_selected(catalogue)
        payload=select_payload(catalogue,selection)
        result={'candidate_catalogue':catalogue,'raw_output':json.dumps(selection),'payload':payload}
        facts,errors=CandidateExtractor.validate_result(result,{'text':source,'raw_html':raw},'ZZTEST')
        self.assertEqual(errors,[])
        self.assertEqual({(f['value'],f['basis']) for f in facts},{('1.25','GAAP'),('1.40','NON_GAAP')})
        self.assertTrue(all(f['basis_validation_method']=='verified_dom_eps_section_row_column_v1' for f in facts))
        # Changing the proof document cannot retain a previous selection as valid.
        swapped=raw.replace(b'<td>1.40</td><td>1.30</td>',b'<td>1.30</td><td>1.40</td>')
        self.assertIn('candidate_catalogue_proof_mismatch',CandidateExtractor.validate_result(result,{'text':source,'raw_html':swapped},'ZZTEST')[1])

    def test_policy_change_is_not_confused_with_ordinary_fair_value_changes(self):
        base='ZZTEST reports results for the quarter ended June 30, 2026.\nNon-GAAP diluted EPS was $1.25.\n'
        ordinary=build_catalogue(base+'Non-GAAP measures exclude stock-based compensation and changes in fair value.','ZZTEST')
        self.assertEqual(ordinary['metric_definition_changes'],[])
        changed=build_catalogue(base+'Beginning in fiscal 2026, non-GAAP measures no longer exclude stock-based compensation.','ZZTEST')
        self.assertEqual(len(changed['metric_definition_changes']),1)

    def test_unscaled_prose_inherits_only_explicit_global_amount_scale(self):
        source='All monetary amounts are in USD millions.\nZZTEST reports results for the quarter ended June 30, 2026.\nRevenue was $100.\nDiluted EPS was $1.25.'
        catalogue=build_catalogue(source,'ZZTEST');payload=select_payload(catalogue,all_selected(catalogue))
        self.assertEqual([(f['name'],f['value'],f['unit']) for f in payload['facts']],[('REVENUE','100','MILLION_USD')])
        self.assertIn('global_amount_scale_per_share_exception_not_verified',catalogue['missing_information'])

    def test_nested_or_hidden_tables_fail_closed(self):
        for body in ('<td style="display:none">Revenue</td><td>100</td>', '<td>Revenue<table><tr><td>100</td></tr></table></td>'):
            raw=('<p>ZZTEST reports results for the quarter ended September 30, 2026.</p><table><tr>'+body+'</tr></table>').encode()
            source,_=parse_exhibit(raw)
            catalogue=build_catalogue(source,'ZZTEST',raw)
            self.assertEqual(catalogue['preferred_ids'],[])
            self.assertTrue(any(t['status']=='unsupported' for t in catalogue['table_findings']))

    def test_conflicting_source_values_cannot_be_erased_by_model_selection(self):
        source='ZZTEST reports results for the quarter ended June 30, 2026.\nRevenue was $100 million.\nRevenue was $200 million.'
        catalogue=build_catalogue(source,'ZZTEST')
        self.assertTrue(catalogue['fatal_errors'])
        with self.assertRaises(CandidateError):
            select_payload(catalogue,all_selected(catalogue))

    def test_model_cannot_add_an_id_edit_a_fact_or_omit_classification(self):
        catalogue=build_catalogue(SOURCE,'ZZTEST')
        selection=all_selected(catalogue)
        for changed in (selection|{'selected_ids':['invented']}, selection|{'selected_ids':selection['selected_ids'][:-1]},
                        selection|{'facts':[{'value':'999'}]}):
            with self.assertRaises(CandidateError):
                select_payload(catalogue,changed)

    def test_flattened_tables_are_not_structured_proof(self):
        source='ZZTEST reports results for the quarter ended September 30, 2026.\nRevenue\t100\t90'
        catalogue=build_catalogue(source,'ZZTEST')
        self.assertEqual(catalogue['preferred_ids'],[])
        self.assertTrue(catalogue['table_findings'])

    def test_recomputed_proof_rejects_mutated_catalogue_or_payload(self):
        catalogue=build_catalogue(SOURCE,'ZZTEST')
        selection=all_selected(catalogue)
        result={'candidate_catalogue':catalogue,'raw_output':json.dumps(selection),'payload':select_payload(catalogue,selection)}
        self.assertEqual(CandidateExtractor.validate_result(result,{'text':SOURCE},'ZZTEST')[1],[])
        changed=copy.deepcopy(result);changed['payload']['facts'][0]['value']='999'
        self.assertIn('candidate_payload_differs_from_immutable_selection',CandidateExtractor.validate_result(changed,{'text':SOURCE},'ZZTEST')[1])
        changed=copy.deepcopy(result);changed['candidate_catalogue']['candidates'][0]['fact']['period_end']='2027-06-30'
        self.assertIn('candidate_catalogue_proof_mismatch',CandidateExtractor.validate_result(changed,{'text':SOURCE},'ZZTEST')[1])

    def test_pre_intro_segment_numbers_and_post_outlook_business_metrics_are_excluded(self):
        source=('Record Data Center revenue of $90 million.\nZZTEST reports results for the quarter ended June 30, 2026.\n'
                'Revenue was $100 million.\nOutlook\nData Center revenue was $90 million.\nEdge revenue was $10 million.')
        catalogue=build_catalogue(source,'ZZTEST')
        self.assertEqual(catalogue['fatal_errors'],[])
        self.assertEqual([c['fact']['value'] for c in catalogue['candidates'] if c['id'] in catalogue['preferred_ids']],['100'])

    def test_generic_non_gaap_label_cannot_replace_source_backed_methodology_mapping(self):
        catalogue=build_catalogue(SOURCE,'ZZTEST');payload=select_payload(catalogue,all_selected(catalogue))
        facts,_=validate_facts(payload,SOURCE,'ZZTEST')
        published=Stamp.now().utc_ns
        for field,value in (('comparability_verified',False),('methodology_id',''),('methodology_evidence',[]),('issuer_metric_definition_id',None)):
            context=context_for(published);context['eps'][field]=value
            _,reasons=compare_earnings(facts,context,'ZZTEST',published)
            self.assertIn('eps:non_gaap_methodology_mapping_not_verified',reasons)

    def test_current_definition_change_must_match_verified_pre_event_issuer_definition(self):
        catalogue=build_catalogue(SOURCE,'ZZTEST');payload=select_payload(catalogue,all_selected(catalogue))
        facts,_=validate_facts(payload,SOURCE,'ZZTEST');published=Stamp.now().utc_ns;context=context_for(published)
        facts[0]['comparability_constraints']={'requires_definition_match':True,'issuer_metric_definition_id':'CHANGED_ISSUER_DEFINITION'}
        _,reasons=compare_earnings(facts,context,'ZZTEST',published)
        self.assertIn('eps:issuer_metric_definition_not_mapped',reasons)
        context['eps']['issuer_metric_definition_id']='CHANGED_ISSUER_DEFINITION'
        self.assertEqual(compare_earnings(facts,context,'ZZTEST',published)[1],[])

    def test_gaap_expectation_needs_explicit_provenance_not_us_issuer_assumption(self):
        catalogue=build_catalogue(SOURCE,'ZZTEST');payload=select_payload(catalogue,all_selected(catalogue))
        facts,_=validate_facts(payload,SOURCE,'ZZTEST');published=Stamp.now().utc_ns;context=context_for(published)
        context['revenue']['evidence']=[]
        self.assertIn('revenue:gaap_basis_provenance_not_verified',compare_earnings(facts,context,'ZZTEST',published)[1])
        context['revenue']['evidence']=[{'type':'json_pointer','document_id':'SYNTHETIC','pointer':'/consensus/basis','value':'GAAP'}]
        self.assertEqual(compare_earnings(facts,context,'ZZTEST',published)[1],[])


class ExtractorTests(unittest.IsolatedAsyncioTestCase):
    async def test_llm_selects_only_immutable_ids_and_no_tool_or_order_fields_exist(self):
        requests=[]
        async def respond(request):
            body=json.loads(request.content);requests.append(body)
            offered=json.loads(body['input'])['untrusted_financial_candidates']
            selection={'ticker':'ZZTEST','selected_ids':[c['id'] for c in offered],'rejected_ids':[],'missing_information':[]}
            return httpx.Response(200,json={'status':'completed','model':'SYNTHETIC_MODEL','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(selection)}]}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.dict('os.environ',{'OPENAI_API_KEY':'SYNTHETIC'}):
                result=await CandidateExtractor(client)(SOURCE,'ZZTEST')
        self.assertEqual(validate_facts(result['payload'],SOURCE,'ZZTEST')[1],[])
        self.assertNotIn('tools',requests[0]);self.assertFalse(requests[0]['store'])
        schema=requests[0]['text']['format']['schema']
        self.assertEqual(set(schema['properties']),{'ticker','selected_ids','rejected_ids','missing_information'})
        self.assertEqual(result['model'],'SYNTHETIC_MODEL')

    async def test_no_model_call_when_no_explicit_reported_period_exists(self):
        def forbidden(request):
            self.fail('Unsupported source must not call a model')
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            result=await CandidateExtractor(client)('Revenue was USD 100 million.','ZZTEST')
        self.assertEqual(result['model'],'not_called')
        self.assertIn('decoding_error',result)

    async def test_analyzer_records_candidate_definition_and_defers_execution_without_order_authority(self):
        async def respond(request):
            offered=json.loads(json.loads(request.content)['input'])['untrusted_financial_candidates']
            result={'ticker':'ZZTEST','selected_ids':[c['id'] for c in offered],'rejected_ids':[],'missing_information':[]}
            return httpx.Response(200,json={'status':'completed','model':'SYNTHETIC_MODEL','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(result)}]}]})
        async def load():
            return {'text':SOURCE,'full_text_verified':True}
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            extractor=CandidateExtractor(client)
            receipt=Stamp.now(); published=receipt.utc_ns-SECOND
            with patch.dict('os.environ',{'OPENAI_API_KEY':'SYNTHETIC'}):
                result=await TimedAnalyzer(extractor).process('SYNTHETIC_EVENT','ZZTEST',receipt,load,context=context_for(published),
                    published_ns=published,defer_execution_checks=True)
        self.assertEqual(result['decision'],'pending_execution_checks')
        self.assertFalse(result['orders_enabled'])
        self.assertEqual(result['prompt_version'],'candidate-selection-v1')
        self.assertEqual(result['extractor_mode'],'verified-candidates-v1')
        self.assertEqual(result['reasons'],[])
        self.assertTrue(result['facts_pass_source_validation'])
        self.assertTrue(all('candidate_provenance' in f for f in result['facts']))
