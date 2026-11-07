"""Every alert, quote, broker, order and fill here is an explicit fabricated fixture."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from recorder.journal import Journal, read_rows
from recorder.paper_broker import EntryPermit, PaperBroker, PaperError
from recorder.paper_demo import SimulationBroker, SyntheticClock, run_paper_demo, write_paper_demo_report
from recorder.portfolio import PaperLedger
from recorder.timebase import DEADLINE_NS, SECOND


class SyntheticPaperDemoTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.sequence = 0

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def run_demo(self, scenario='complete'):
        self.sequence += 1
        path = self.root/str(self.sequence)
        journal = Journal(str(path)+'.sqlite', {'mode':'SYNTHETIC','purpose':'fabricated test fixture'})
        try:
            # An actual Alpaca client/transport must never be constructed or called.
            with patch.dict('os.environ',{},clear=True), \
                 patch('recorder.paper_demo.httpx.AsyncClient',side_effect=AssertionError('Offline client forbidden')), \
                 patch.object(PaperBroker,'__init__',side_effect=AssertionError('Actual broker forbidden')), \
                 patch.object(PaperBroker,'read',side_effect=AssertionError('Actual broker request forbidden')), \
                 patch.object(PaperBroker,'submit',side_effect=AssertionError('Actual broker submission forbidden')):
                report = await run_paper_demo(journal,str(path)+'-state.sqlite',scenario=scenario)
        finally:
            journal.close()
        return report,list(read_rows(str(path)+'.sqlite'))

    async def test_full_measured_entry_then_explicit_partial_cancel_and_calendar_exit(self):
        report,records = await self.run_demo()
        self.assertEqual(report['entry_result']['decision'],'synthetic_entry_ready')
        self.assertTrue(report['analysis']['facts_pass_source_validation'])
        self.assertEqual(report['entry_result']['reasons'],[])
        self.assertTrue(report['entry_result']['admission_audit_committed'])
        self.assertLess(report['entry_measured_monotonic_duration_ns'],DEADLINE_NS)
        self.assertLess(report['entry_result']['elapsed_from_receipt_ns'],DEADLINE_NS)
        self.assertEqual(report['injected_entry_delay_ns'],0)
        self.assertTrue(report['lifecycle_completed'])
        entry,exit_order = report['final_orders']
        self.assertEqual((entry['state'],entry['payload']['qty'],entry['filled_qty']),('canceled','99','25'))
        self.assertEqual((exit_order['state'],exit_order['payload']['qty'],exit_order['filled_qty']),('filled','25','25'))
        self.assertEqual(entry['exit_utc_ns'],report['final_orders'][1]['exit_utc_ns'])
        self.assertEqual(report['synthetic_dispatches'][1]['payload']['extended_hours'],False)
        self.assertEqual(report['synthetic_dispatches'][0]['payload']['extended_hours'],True)
        self.assertEqual(report['remaining_shares'],'0')
        self.assertEqual(report['final_portfolio']['positions'],[])
        self.assertEqual(report['final_portfolio']['entry_reservations'],[])
        self.assertEqual(report['final_portfolio']['cash'],'100000.00')
        self.assertTrue(report['final_portfolio']['reconciled'])
        self.assertEqual(report['synthetic_cancel_requests'][0]['confirmed'],True)
        phases = {r['phase']:r for r in report['lifecycle']}
        self.assertEqual(phases['after_entry_acknowledgment; no fill inferred']['remaining_shares'],'0')
        pending = phases['entry_expired_cancel_requested; remainder still reserved']
        self.assertEqual(pending['orders'][0]['state'],'cancel_requested')
        self.assertEqual(pending['remaining_shares'],'25')
        confirmed = phases['entry_cancellation_confirmed; filled shares retained']
        self.assertEqual(confirmed['orders'][0]['state'],'canceled')
        self.assertEqual(confirmed['remaining_shares'],'25')
        self.assertEqual(report['api_attempts'],0)
        self.assertEqual(report['offline_selector_calls'],1)
        self.assertEqual(report['broker_network_requests'],0)
        self.assertEqual(report['actual_broker_order_submissions'],0)
        self.assertTrue(all(not d['http_request_sent'] for d in report['synthetic_dispatches']))
        self.assertEqual(report['mode'],'SYNTHETIC')
        self.assertTrue(all(r['data'].get('mode')=='SYNTHETIC' for r in records))
        self.assertTrue(any(r['kind']=='candidate_catalogue' for r in records))
        audit = next(r for r in records if r['kind']=='paper_admission_audit')
        marker = next(r for r in records if r['kind']=='_durability_barrier')
        self.assertLess(audit['seq'], marker['seq'])
        self.assertEqual(audit['data']['original_receipt']['mono_ns']+DEADLINE_NS, audit['data']['original_deadline_mono_ns'])
        # Existing state cannot be reopened as an observed-paper account.
        with self.assertRaisesRegex(ValueError,'Observed and synthetic'):
            PaperLedger(report['ledger_path'],mode='OBSERVED')
        write_paper_demo_report(report,self.root/'proof.json',self.root/'proof.html')
        self.assertEqual(json.loads((self.root/'proof.json').read_text())['actual_broker_order_submissions'],0)
        self.assertIn('zero actual broker requests or orders',(self.root/'proof.html').read_text())

    async def test_late_market_and_queued_admission_and_dispatch_never_send(self):
        for scenario in ('late_market','queued_admission','queued_dispatch'):
            with self.subTest(scenario=scenario):
                report,_ = await self.run_demo(scenario)
                self.assertEqual(report['entry_result']['decision'],'skip')
                self.assertFalse(report['lifecycle_completed'])
                self.assertEqual(report['simulated_order_submissions'],0)
                self.assertEqual(report['actual_broker_order_submissions'],0)
                self.assertEqual(report['remaining_shares'],'0')
                self.assertGreaterEqual(report['injected_entry_delay_ns'],DEADLINE_NS)
                self.assertTrue(any('five_second_deadline' in r for r in report['entry_result']['reasons']))
                self.assertFalse(any(r['filled_qty']!='0' for r in report['final_orders']))

    async def test_missing_release_or_unmapped_context_or_model_rejection_is_a_skip(self):
        for scenario,reason in (('source_gap','missing_complete_release_text'),
                                ('context_gap','eps:non_gaap_methodology_mapping_not_verified'),
                                ('model_rejects_revenue','revenue:missing_or_conflicting_comparable_fact')):
            with self.subTest(scenario=scenario):
                report,_ = await self.run_demo(scenario)
                self.assertEqual(report['entry_result']['decision'],'skip')
                self.assertEqual(report['simulated_order_submissions'],0)
                self.assertEqual(report['final_orders'],[])
                self.assertIn(reason,report['entry_result']['reasons'])
                if scenario=='source_gap':
                    self.assertEqual(report['offline_selector_calls'],0)

    async def test_synthetic_broker_has_no_client_parameter_and_real_permit_stays_rejected(self):
        clock = SyntheticClock()
        with self.assertRaises(TypeError):
            SimulationBroker(clock,client=object())
        with self.assertRaises(TypeError):
            SimulationBroker(clock,object())
        with self.assertRaises(TypeError):
            SimulationBroker(object())
        receipt = clock.now()
        permit = EntryPermit('research-'+'a'*32,receipt,receipt.mono_ns,receipt.mono_ns,receipt.utc_ns,
            receipt.mono_ns,receipt.mono_ns+SECOND,mode='SYNTHETIC')
        permit.check_timing(clock.now(),lambda:False)
        with self.assertRaisesRegex(PaperError,'research_input_cannot_submit_to_broker'):
            permit.check(clock.now(),lambda:False)
        broker = SimulationBroker(clock)
        broker.account_authorized,broker.killed = lambda:True,lambda:False
        permit.mode = 'OBSERVED'
        payload = {'symbol':'ZZTEST','side':'buy','qty':'1','type':'limit','limit_price':'10.02',
            'time_in_force':'day','extended_hours':True,'client_order_id':permit.client_order_id}
        with self.assertRaisesRegex(PaperError,'synthetic_matching_entry_permit_required'):
            await broker.submit(payload,permit=permit)
        self.assertEqual(broker.dispatches,[])


if __name__ == '__main__':
    unittest.main()
