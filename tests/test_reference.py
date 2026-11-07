"""Network-free fabricated reference samples; no observed market data."""
import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from recorder.context import ContextStore
from recorder.reference import ContextReferenceRecorder, VERSION
from recorder.timebase import MarketCalendar, SECOND, Stamp, iso_utc, parse_rfc3339


class Clock:
    def __init__(self):self.value=Stamp(parse_rfc3339('2026-11-02T16:00:00Z')[0],100*SECOND)
    def now(self):return self.value
    def advance(self,ns):self.value=Stamp(self.value.utc_ns+ns,self.value.mono_ns+ns)


def sample(clock,ticker='ZZTEST'):
    stamp=clock.now()
    return {'mode':'SYNTHETIC','ticker':ticker,'feed':'sip','quote_scope':'consolidated_SIP',
            'observation_method':'rest_latest_snapshot','http_status':200,'coverage_verified':True,
            'conditions_eligible':True,'size_unit_verified':True,'bid_size_shares':'100','ask_size_shares':'200',
            'bid':'10','ask':'10.02','source_utc':iso_utc(stamp.utc_ns),'available_utc':iso_utc(stamp.utc_ns),
            'available_mono_ns':stamp.mono_ns,'calendar_session':'regular','broker_session_supported':True,
            'market_status':'open_quote_asof_timestamp','halt_state':'unknown','open_quote_asof_timestamp':True,
            'eligible_for_execution_checks':False,'reference_price':'999999',
            'raw_quote':{'t':iso_utc(stamp.utc_ns),'bp':'10','ap':'10.02','bs':100,'as':200,
                         'bx':'Q','ax':'Q','c':['R'],'z':'C'}}


class Provider:
    mode='SYNTHETIC'
    feed='sip'
    def __init__(self,clock):
        self.clock=clock;self.calendar=MarketCalendar(clock.now().utc_ns)
        self.result=None;self.calls=[];self.active=0;self.peak=0
    async def __call__(self,ticker):
        self.calls.append(ticker);self.active+=1;self.peak=max(self.peak,self.active)
        try:
            await asyncio.sleep(0)
            if isinstance(self.result,Exception):raise self.result
            return deepcopy(self.result if self.result is not None else sample(self.clock,ticker))
        finally:self.active-=1


class Journal:
    def __init__(self,target=1):self.values=[];self.event=asyncio.Event();self.target=target
    def emit(self,kind,data,stamp):
        self.values.append((kind,data,stamp))
        if sum(d['status']=='recorded' for _,d,_ in self.values)>=self.target:self.event.set()


class ReferenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.clock=Clock()
        self.store=ContextStore(Path(self.tmp.name)/'synthetic.sqlite',mode='SYNTHETIC',clock=self.clock.now)
        self.provider=Provider(self.clock);self.journal=Journal()
        self.prepare_company('ZZTEST');self.clock.advance(1)
    async def asyncTearDown(self):self.tmp.cleanup()

    def prepare_company(self,ticker):
        text='SYNTHETIC prepared common-stock company '+ticker
        doc=self.store.add_document('https://example.test/'+ticker,text.encode(),source_kind='SYNTHETIC')
        self.store.add_fact(ticker,'company',{'ticker':ticker,'classification_verified':True,
            'canonical_company_id':'SYNTHETIC:issuer:'+ticker,'tradable':True},document_ids=[doc],
            evidence=[{'document_id':doc,'type':'text','passage':text}])

    def recorder(self,**kwargs):
        return ContextReferenceRecorder(self.store,self.provider,self.journal,symbols=['ZZTEST'],**kwargs)

    def references(self):
        return [r for r in self.store._records('ZZTEST',self.clock.now().utc_ns+1) if r['kind']=='reference']

    async def test_exact_raw_source_midpoint_and_actual_availability_are_preserved(self):
        result=await self.recorder().record_once('ZZTEST')
        self.assertEqual(result['status'],'recorded');self.assertEqual(result['price'],'10.01')
        self.assertFalse(result['entry_authority'])
        doc=self.store.document(result['document_id']);body=json.loads(doc['raw'])
        self.assertEqual(body['mode'],'SYNTHETIC')
        self.assertEqual(result['source_sha256'],hashlib.sha256(doc['raw']).hexdigest())
        self.assertEqual(doc['received_utc_ns'],self.clock.now().utc_ns)
        record=self.references()[0]
        self.assertEqual(record['reference_method'],VERSION)
        self.assertTrue(record['unknown_trading_action_accepted_for_reference_only'])
        self.assertFalse(record['entry_authority'])
        self.assertEqual(record['observed_at'],iso_utc(self.clock.now().utc_ns))
        self.assertFalse([r for r in self.store._records('ZZTEST',self.clock.now().utc_ns) if r['kind']=='reference'])
        self.assertTrue(any(e['type']=='derived' and e['input_sha256']==result['source_sha256'] for e in record['evidence']))

    async def test_unknown_action_only_supports_open_price_reference_and_known_halts_fail(self):
        for changes in ({'market_status':'unknown','open_quote_asof_timestamp':False},
                        {'market_status':'halted','halt_state':'halted'},
                        {'market_status':'tradable','halt_state':'halted'},
                        {'market_status':'quotation_only','halt_state':'quotation_only'},
                        {'market_status':'restricted','halt_state':'restricted'}):
            with self.subTest(changes=changes):
                self.provider.result=sample(self.clock)|changes
                result=await self.recorder().record_once('ZZTEST')
                self.assertEqual(result['status'],'gap');self.assertEqual(self.references(),[])

    async def test_stale_or_future_source_and_local_stamps_fail(self):
        for changes in ({'source_utc':iso_utc(self.clock.now().utc_ns-SECOND-1)},
                        {'source_utc':iso_utc(self.clock.now().utc_ns+1)},
                        {'available_utc':iso_utc(self.clock.now().utc_ns-SECOND-1)},
                        {'available_utc':iso_utc(self.clock.now().utc_ns+1)},
                        {'available_mono_ns':self.clock.now().mono_ns-SECOND-1},
                        {'available_mono_ns':self.clock.now().mono_ns+1}):
            with self.subTest(changes=changes):
                self.provider.result=sample(self.clock)|changes
                self.assertEqual((await self.recorder().record_once('ZZTEST'))['status'],'gap')
        self.assertEqual(self.references(),[])

    async def test_invalid_raw_prices_conditions_sizes_and_disagreeing_normalization_fail(self):
        for raw_changes in ({'bp':'0'},{'ap':'9.99'},{'bp':'NaN'},{'ap':10.02},{'c':['H']},{'as':True},{'as':0},{'z':'O'},{'S':'OTHER'}):
            with self.subTest(raw_changes=raw_changes):
                self.provider.result=sample(self.clock);self.provider.result['raw_quote'].update(raw_changes)
                self.assertEqual((await self.recorder().record_once('ZZTEST'))['status'],'gap')
        self.provider.result=sample(self.clock)|{'bid':'999'}
        self.assertEqual((await self.recorder().record_once('ZZTEST'))['status'],'gap')
        self.assertEqual(self.references(),[])

    async def test_delayed_iex_stream_unknown_coverage_and_mode_never_become_references(self):
        for changes in ({'feed':'iex'},{'feed':'delayed_sip'},{'observation_method':'websocket_stream'},
                        {'coverage_verified':False},{'conditions_eligible':False},{'size_unit_verified':False},
                        {'http_status':403},{'mode':'OBSERVED'},{'broker_session_supported':False}):
            with self.subTest(changes=changes):
                self.provider.result=sample(self.clock)|changes
                self.assertEqual((await self.recorder().record_once('ZZTEST'))['status'],'gap')
        self.provider.feed='iex'
        count=len(self.provider.calls)
        self.assertEqual((await self.recorder().record_once('ZZTEST'))['status'],'gap')
        self.assertEqual(len(self.provider.calls),count)
        self.assertEqual(self.references(),[])

    async def test_unprepared_unlisted_and_closed_symbols_do_not_call_provider(self):
        unprepared=ContextReferenceRecorder(self.store,self.provider,self.journal,symbols=['OTHER'])
        self.assertEqual((await unprepared.record_once('OTHER'))['reason_codes'],['reference_prepared_company_identity_missing'])
        self.assertEqual((await self.recorder().record_once('OTHER'))['status'],'gap')
        self.clock.advance(5*86400*SECOND)
        self.assertEqual((await self.recorder().record_once('ZZTEST'))['reason_codes'],['reference_calendar_session_not_covered'])
        self.assertEqual(self.provider.calls,[])

    async def test_minimum_interval_is_enforced_even_for_direct_calls(self):
        recorder=self.recorder()
        self.assertEqual((await recorder.record_once('ZZTEST'))['status'],'recorded')
        self.assertEqual((await recorder.record_once('ZZTEST'))['reason_codes'],['reference_sampling_interval_not_elapsed'])
        self.clock.advance(10*SECOND)
        self.assertEqual((await recorder.record_once('ZZTEST'))['status'],'recorded')
        self.assertEqual(len(self.provider.calls),2)

    async def test_provider_failures_emit_class_only_and_never_a_reference(self):
        self.provider.result=RuntimeError('SYNTHETIC_SECRET_MUST_NOT_APPEAR')
        result=await self.recorder().record_once('ZZTEST')
        self.assertEqual(result['error_class'],'RuntimeError')
        self.assertNotIn('SYNTHETIC_SECRET',json.dumps(result))
        self.assertEqual(self.references(),[])

    async def test_background_runs_three_bounded_requests_then_cancels_cleanly(self):
        self.prepare_company('ZZTWO');self.prepare_company('ZZTHREE');self.clock.advance(1)
        journal=Journal(target=3)
        recorder=ContextReferenceRecorder(self.store,self.provider,journal,symbols=['ZZTEST','ZZTWO','ZZTHREE'])
        task=asyncio.create_task(recorder.run())
        try:await asyncio.wait_for(journal.event.wait(),3)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(set(self.provider.calls),{'ZZTEST','ZZTWO','ZZTHREE'})
        self.assertLessEqual(self.provider.peak,3)
        self.assertEqual(recorder.counters['recorded'],3)
        self.assertEqual(journal.values[-1][1]['status'],'stopped')

    async def test_explicit_watchlist_interval_and_synthetic_isolation_are_mandatory(self):
        for symbols in ([],['ZZTEST']*2,['ZZTEST','A','B','C'],'ZZTEST',[[1]]):
            with self.subTest(symbols=symbols),self.assertRaises(ValueError):
                ContextReferenceRecorder(self.store,self.provider,symbols=symbols)
        for interval in (0,9.9,True,float('inf'),float('nan')):
            with self.subTest(interval=interval),self.assertRaises(ValueError):self.recorder(interval_s=interval)
        observed=ContextStore(Path(self.tmp.name)/'observed.sqlite',mode='OBSERVED',clock=self.clock.now)
        with self.assertRaisesRegex(ValueError,'observed_reference_requires_observed'):
            ContextReferenceRecorder(observed,self.provider,symbols=['ZZTEST'])
        self.provider.mode='OBSERVED'
        with self.assertRaisesRegex(ValueError,'provider_and_store_mode_must_match'):self.recorder()


if __name__=='__main__':unittest.main()
