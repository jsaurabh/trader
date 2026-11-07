"""Fabricated journals test OBSERVED-envelope reporting; no market/broker is used."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from recorder.journal import Journal
from recorder.report import build_report, quote_at, write_report
from recorder.timebase import SECOND, Stamp, iso_utc, parse_rfc3339

CLIENT = 'research-'+'a'*32
EXIT = 'research-'+'b'*32
BROKER_PRIVATE_ID = 'SYNTHETIC_PRIVATE_BROKER_ID'


class PaperReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root/'fixture.sqlite'
        self.base = Stamp(parse_rfc3339('2026-11-02T21:30:04Z')[0],100*SECOND)
        self.journal = None

    def tearDown(self):
        if self.journal:
            self.journal.close()
        self.temp.cleanup()

    def at(self,ns):
        return Stamp(self.base.utc_ns+ns,self.base.mono_ns+ns)

    def emit(self,kind,data,ns=0):
        self.journal.emit(kind,data,self.at(ns))

    def start(self,mode='OBSERVED',paper=True):
        self.journal = Journal(self.path,{'mode':mode,'feed':'sip','paper_enabled':paper,'orders_enabled':False,
            'api_key':'SYNTHETIC_API_SECRET','APCA-API-KEY-ID':'SYNTHETIC_API_SECRET','accountId':'SYNTHETIC_ACCOUNT_SECRET',
            'private_source':'https://example.invalid/private?token=SYNTHETIC_SIGNED_SECRET'},started=self.base)
        self.emit('alert',{'event_id':'FIXTURE_EVENT','ticker':'ZZTEST','alert_kind':'alert',
            'published_raw':'2026-11-02T21:30:00Z','published_ns':self.base.utc_ns-4*SECOND})
        self.emit('transport',{'stream':'alpaca','state':'ready','quotes':['ZZTEST'],'feed':'sip'},-SECOND)
        for offset,ask in ((0,'10.02'),(5*SECOND,'10.12')):
            self.emit('quote',{'feed':'sip','raw':{'S':'ZZTEST','t':iso_utc(self.at(offset).utc_ns),'bp':'10.00','ap':ask,'bs':'100','as':'100'}},offset)
        self.emit('observation_window',{'event_id':'FIXTURE_EVENT','receipt_mono_ns':self.base.mono_ns},5*SECOND)

    def payload(self,client=CLIENT,side='buy',qty='99'):
        return {'client_order_id':client,'symbol':'ZZTEST','side':side,'type':'limit','qty':qty,'limit_price':'10.04',
            'time_in_force':'day','extended_hours':side=='buy'}

    def order(self,filled='0',status='new',ns=SECOND,client=CLIENT,side='buy',qty='99'):
        return self.payload(client,side,qty)|{'id':BROKER_PRIVATE_ID+client[-1],'status':status,'filled_qty':filled,
            'filled_avg_price':'10.04' if filled!='0' else None,'updated_at':iso_utc(self.at(ns).utc_ns),
            'account_id':'SYNTHETIC_ACCOUNT_SECRET','private_url':'https://example.invalid/private?apiKey=SYNTHETIC_SIGNED_SECRET'}

    def decision(self,*,abort=False):
        return {'event_id':'FIXTURE_EVENT','ticker':'ZZTEST','mode':'OBSERVED','paper':True,
            'decision':'skip' if abort else 'paper_entry_ready','reasons':['quote_or_portfolio_expired_before_dispatch'] if abort else [],
            'orders_enabled':True,'client_order_id':CLIENT,'elapsed_from_receipt_ns':SECOND,
            **({'pre_dispatch_abort':True,'supersedes_candidate_client_id':CLIENT} if abort else {}),
            'decision_trace':{'inputs':{'quote':{'ticker':'ZZTEST','bid':'10.02','ask':'10.04','ask_size_shares':'100',
                'source_utc':iso_utc(self.at(SECOND).utc_ns),'available_utc':iso_utc(self.at(SECOND).utc_ns),
                'available_mono_ns':self.at(SECOND).mono_ns,'feed':'sip','coverage_verified':True,'conditions_eligible':True,
                'size_unit_verified':True,'market_status':'tradable','private_url':'https://example.invalid?secret=SYNTHETIC_SIGNED_SECRET'}},
                'paper_intent':self.payload()|{'scheduled_exit_utc':'2026-11-03T15:00:00Z','expires_utc':iso_utc(self.at(2*SECOND).utc_ns)}}}

    def dispatch(self,client=CLIENT,side='buy',qty='99'):
        self.emit('paper_order_dispatch',{'paper':True,'client_order_id':client,'payload':self.payload(client,side,qty),
            'elapsed_from_receipt_ns':SECOND,'dispatch_boundary':'before POST headers'},SECOND)

    def finish(self):
        self.journal.close()
        self.journal = None
        return build_report(self.path)

    def test_configuration_dispatch_ack_and_unfilled_order_are_distinct_from_quotes(self):
        self.start()
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        self.emit('paper_order_ack',{'client_order_id':CLIENT,'http_status':200,'dispatch_observed':True},SECOND)
        self.emit('paper_order_snapshot',{'source':'POST_response','order':self.order()},SECOND)
        report = self.finish()
        self.assertFalse(report['live_orders_enabled'])
        self.assertFalse(report['orders_enabled'])
        self.assertTrue(report['paper_enabled_requested'])
        self.assertTrue(report['paper_enabled'])
        self.assertEqual(report['orders_submitted'],1)
        self.assertEqual(report['paper_summary']['orders_with_fill_evidence'],0)
        self.assertEqual(report['paper_summary']['orders'][0]['latest_reported_filled_qty'],'0')
        self.assertIsNone(report['paper_summary']['orders'][0]['returns_after_costs'])
        self.assertIsNone(report['paper_summary']['orders'][0]['dispatch_to_first_fill_observation_ns'])
        self.assertEqual(report['paper_summary']['orders'][0]['dispatch_to_ack_observation_ns'],0)
        event = report['events'][0]
        self.assertEqual(event['quote_at_receipt']['ask'],'10.02')
        self.assertEqual(event['quote_at_five_seconds']['ask'],'10.12')
        self.assertEqual(event['execution_quote_at_decision']['ask'],'10.04')
        self.assertIsNone(event['returns_after_costs'])
        self.assertEqual(event['orders_submitted'],1)
        write_report(report,self.root/'report.json',self.root/'report.html')
        rendered = (self.root/'report.html').read_text()
        self.assertIn('Observed paper dispatches',rendered)
        self.assertIn('Checked for entry',rendered)
        self.assertNotIn('no orders submitted',rendered)

    def test_abort_supersedes_candidate_and_never_counts_as_submitted(self):
        self.start()
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        self.emit('paper_order_dispatch_aborted',{'client_order_id':CLIENT,'reason':'quote_or_portfolio_expired_before_dispatch'},SECOND+1)
        self.emit('decision',self.decision(abort=True),SECOND+2)
        report = self.finish()
        event = report['events'][0]
        self.assertEqual(event['decision'],'skip')
        self.assertEqual(len(event['decision_history']),2)
        self.assertEqual(event['decision_history'][0]['decision'],'paper_entry_ready')
        self.assertTrue(event['decision_history'][1]['pre_dispatch_abort'])
        self.assertEqual(report['orders_submitted'],0)
        self.assertEqual(event['orders_submitted'],0)
        self.assertEqual(report['paper_summary']['aborted_order_attempts'],1)
        self.assertEqual(report['paper_summary']['orders'][0]['submission_state'],'aborted_before_dispatch')

    def test_transport_abort_alone_also_supersedes_old_candidate_recording(self):
        self.start()
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        self.emit('paper_order_dispatch_aborted',{'client_order_id':CLIENT,'reason':'manual_kill_switch'},SECOND+1)
        report = self.finish()
        self.assertEqual(report['events'][0]['decision'],'skip')
        self.assertTrue(report['events'][0]['decision_superseded_by_transport_abort'])
        self.assertIn('manual_kill_switch',report['events'][0]['decision_reasons'])
        self.assertEqual(report['paper_orders_dispatched'],0)

    def test_partial_fill_cancel_confirmation_exit_and_privacy_are_preserved(self):
        self.start()
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        partial = {'event':'partial_fill','execution_id':'SYNTHETIC_EXECUTION','qty':'25','price':'10.04',
            'account_id':'SYNTHETIC_ACCOUNT_SECRET','order':self.order('25','partially_filled',2*SECOND)}
        for ns in (2*SECOND,2*SECOND+1):
            self.emit('paper_trade_update',{'raw':{'stream':'trade_updates','data':partial}},ns)
        self.emit('paper_cancel_requested',{'order_id':BROKER_PRIVATE_ID+'a','http_status':204},3*SECOND)
        self.emit('paper_read',{'path':'/v2/account','body':{'id':'SYNTHETIC_ACCOUNT_SECRET','api_key':'SYNTHETIC_API_SECRET'}},3*SECOND)
        self.emit('paper_read',{'path':'/v2/orders:by_client_order_id','body':self.order('25','canceled',4*SECOND)},4*SECOND)
        self.emit('paper_portfolio',{'portfolio':{'paper':True,'reconciled':True,'cash':'99749','equity':'100000',
            'positions':[{'ticker':'ZZTEST','qty':'25','market_value':'251'}],'source':'https://example.invalid?secret=SYNTHETIC_SIGNED_SECRET'},
            'flags':{'account_hash':'SYNTHETIC_ACCOUNT_SECRET','reconciled':True}},4*SECOND)
        self.emit('paper_exit_overdue',{'parent_client_id':CLIENT,'scheduled_exit_utc':'2026-11-03T15:00:00Z','reason':'exit_state_expired_before_dispatch'},5*SECOND)
        self.emit('paper_exit_decision',{'client_order_id':EXIT,'parent_client_id':CLIENT,'scheduled_exit_utc':'2026-11-03T15:00:00Z'},6*SECOND)
        self.dispatch(EXIT,'sell','25')
        self.emit('paper_order_snapshot',{'source':'POST_response','order':self.order('25','filled',7*SECOND,EXIT,'sell','25')},7*SECOND)
        report = self.finish()
        summary = report['paper_summary']
        self.assertEqual(summary['unique_dispatched_orders'],2)
        self.assertEqual(summary['unique_fill_updates'],1)
        self.assertEqual(summary['orders_with_fill_evidence'],2)
        self.assertEqual(summary['cancel_request_count'],1)
        self.assertEqual(summary['exit_overdue_count'],1)
        entry = next(o for o in summary['orders'] if o['client_order_id']==CLIENT)
        self.assertEqual(entry['latest_reported_filled_qty'],'25')
        self.assertEqual(entry['dispatch_to_first_fill_observation_ns'],SECOND)
        self.assertTrue(entry['cancellation_confirmed'])
        self.assertEqual(len(entry['cancel_requests']),1)
        self.assertEqual(report['events'][0]['orders_submitted'],2)
        output = json.dumps(report)
        for secret in ('SYNTHETIC_ACCOUNT_SECRET','SYNTHETIC_API_SECRET','SYNTHETIC_SIGNED_SECRET',BROKER_PRIVATE_ID):
            self.assertNotIn(secret,output)
        self.assertNotIn('example.invalid',output)
        self.assertEqual(summary['latest_portfolio_by_run'][0]['portfolio']['positions'][0]['qty'],'25')

    def test_synthetic_run_or_synthetic_record_never_enters_observed_paper_totals(self):
        self.start(mode='SYNTHETIC')
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        self.emit('paper_order_snapshot',{'order':self.order('99','filled')},SECOND)
        report = self.finish()
        self.assertEqual(report['paper_summary']['orders'],[])
        self.assertEqual(report['orders_submitted'],0)
        self.assertFalse(report['paper_enabled'])
        self.assertGreater(report['paper_summary']['excluded_non_observed_paper_records'],0)
        self.assertEqual(report['events'][0]['observed_paper_orders'],[])
        # Even an explicitly synthetic row in an observed run is excluded.
        self.path = self.root/'mixed-fixture.sqlite'
        self.start()
        self.emit('paper_order_dispatch',{'mode':'SYNTHETIC','client_order_id':CLIENT,'payload':self.payload()},SECOND)
        mixed = self.finish()
        self.assertEqual(mixed['orders_submitted'],0)
        self.assertEqual(mixed['paper_summary']['orders'],[])

    def test_requested_configuration_without_dispatch_is_zero_orders(self):
        self.start()
        report = self.finish()
        self.assertTrue(report['paper_enabled_requested'])
        self.assertEqual(report['paper_orders_dispatched'],0)
        self.assertEqual(report['paper_summary']['acknowledgment_count'],0)

    def test_later_exit_abort_does_not_rewrite_the_original_entry_decision(self):
        self.start()
        self.emit('decision',self.decision(),SECOND)
        self.dispatch()
        self.emit('paper_exit_decision',{'client_order_id':EXIT,'parent_client_id':CLIENT},6*SECOND)
        self.dispatch(EXIT,'sell','25')
        self.emit('paper_order_dispatch_aborted',{'client_order_id':EXIT,'reason':'exit_state_expired_before_dispatch'},6*SECOND+1)
        report = self.finish()
        self.assertEqual(report['events'][0]['decision'],'paper_entry_ready')
        self.assertFalse(report['events'][0]['decision_superseded_by_transport_abort'])
        self.assertEqual(report['paper_orders_dispatched'],1)

    def test_current_rest_sizes_are_shares_without_rewriting_vendor_fields(self):
        raw = {'S':'ZZTEST','t':iso_utc(self.base.utc_ns),'bp':'10','ap':'10.02','bs':17,'as':23,'z':'C'}
        row = {'seq':1,'stamp':self.base,'data':{'feed':'sip','raw':raw,'observation_method':'rest_latest_snapshot'}}
        quote = quote_at([row],self.base,'sip',True)
        self.assertEqual((quote['bid_size_vendor_units'],quote['ask_size_vendor_units']),(17,23))
        self.assertTrue(quote['size_unit_verified'])
        self.assertEqual((quote['bid_size_shares'],quote['ask_size_shares']),('17','23'))
        self.assertEqual(quote['size_provenance']['conversion_multiplier'],1)
        self.assertEqual(raw['as'],23)
        old = copy.deepcopy(row)
        old['data']['raw']['t']='2025-10-31T20:00:00Z'
        self.assertFalse(quote_at([old],self.base,'sip',True)['size_unit_verified'])
        websocket = copy.deepcopy(row)
        del websocket['data']['observation_method']
        result = quote_at([websocket],self.base,'sip',True)
        self.assertEqual(result['size_unit'],'shares')
        self.assertTrue(result['size_unit_verified'])
        self.assertEqual(result['size_provenance']['method'],'websocket_stream')
        self.assertIn('documentation_conflict',result['size_provenance'])
        websocket['data']['feed']='iex'
        result = quote_at([websocket],self.base,'iex',True)
        self.assertEqual(result['size_unit'],'unknown')
        self.assertFalse(result['size_unit_verified'])

    def test_canonical_clock_is_distinct_from_raw_vendor_id_observation_window(self):
        self.start()
        clock = {'event_id':'FIXTURE_EVENT','first_event_id':'EARLIER_FIXTURE_EVENT','first_run_id':self.journal.run_id,
            'first_receipt_utc_ns':self.base.utc_ns-3*SECOND,'first_receipt_mono_ns':self.base.mono_ns-3*SECOND,
            'comparable_in_current_run':True,'blocking_event_ids':[],'block_reasons':[]}
        self.emit('source_classification',{'event_id':'FIXTURE_EVENT','relevant':True,'current_period_end':'2026-06-30',
            'reason':'source_verified_current_quarter','issuer_identity':{'verified':True},'announcement_clock':clock},SECOND)
        self.emit('announcement_clock_binding',clock,SECOND)
        self.emit('decision',self.decision()|{'announcement_clock':clock},SECOND)
        report = self.finish()
        event = report['events'][0]
        self.assertEqual(event['five_second_target_utc'],iso_utc(self.base.utc_ns+5*SECOND))
        self.assertEqual(event['canonical_announcement_clock']['original_five_second_deadline_utc'],iso_utc(self.base.utc_ns+2*SECOND))
        self.assertEqual(event['canonical_announcement_clock']['first_event_id'],'EARLIER_FIXTURE_EVENT')
        self.assertTrue(event['source_classification'][0]['relevant'])
        self.assertEqual(event['decision_history'][0]['announcement_clock']['first_event_id'],'EARLIER_FIXTURE_EVENT')


if __name__=='__main__':
    unittest.main()
