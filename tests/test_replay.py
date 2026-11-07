"""Fabricated quote/announcement fixtures only; no broker or market-data calls."""
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import json
import tempfile
import unittest

from recorder.execution_model import ConservativeExecutionModel, ExecutionAssumptions, ModelOrder, QuoteObservation
from recorder.replay import EvaluationRegistry, ReplayCase, ReplayPlan, ReplayRiskState, REQUIRED_EVIDENCE, replay_cases
from recorder.replay import QuoteCapabilities, historical_quotes_from_journal, reproduce_report
from recorder.journal import Journal
from recorder.timebase import SECOND, parse_rfc3339

PUB = parse_rfc3339('2026-11-02T21:30:00Z')[0]
EXIT = parse_rfc3339('2026-11-03T15:00:00Z')[0]


def quote(at, *, bid='10.00', ask='10.01', size='100', mode='SYNTHETIC', **kwargs):
    return QuoteObservation('ZZTEST', at, at, bid, ask, size, size, mode,
                            'observed_receipt' if mode == 'OBSERVED_REPLAY' else 'assumed_from_source_plus_delay',
                            'SYNTHETIC-'+str(at), 'SYNTHETIC_BOOK', True, True, True, True, True, **kwargs)


def buy(at=PUB, qty=20, limit='10.02'):
    return ModelOrder('SYNTHETIC_ENTRY', 'SYNTHETIC_ANNOUNCEMENT', 'ZZTEST', 'buy', qty, limit, at, at+250_000_000, at+SECOND, 'SYNTHETIC')


def case(**kwargs):
    values = dict(case_id='SYNTHETIC_CASE', announcement_key='SYNTHETIC_ANNOUNCEMENT', symbol='ZZTEST', publication_utc_ns=PUB,
                  processing_time_ns=3*SECOND, processing_scope='end_to_end_receipt_to_risk_ready', qualified=True,
                  assumed_feed_delay_ns=4*SECOND, entry_qty=99, entry_limit_price='10.02', price_tick='0.01',
                  evidence={key: True for key in REQUIRED_EVIDENCE})
    values.update(kwargs)
    return ReplayCase(**values)


def plan(**kwargs):
    values = dict(mode='SYNTHETIC', strategy_version='SYNTHETIC_STRATEGY_V1', split='development',
                  windows={'development': (PUB-SECOND, EXIT+120*SECOND)}, exit_retry_interval_ns=SECOND)
    values.update(kwargs)
    return ReplayPlan(**values)


def synthetic_quotes():
    ready = PUB+7*SECOND
    return [quote(PUB+3*SECOND, bid='8.99', ask='9.00', size='10000'),  # Not available after entry arrival.
            quote(ready-100_000_000, ask='10.02'),
            quote(ready+300_000_000), quote(ready+400_000_000),  # Duplicate level cannot replenish ten shares.
            quote(ready+500_000_000, size='150'),  # Only five extra shares of participation capacity.
            quote(ready+600_000_000, ask='10.00'),  # New ask price: ten additional shares.
            quote(ready+1_300_000_000, ask='9.50', size='10000'),  # Arrives after effective entry expiry.
            quote(EXIT-100_000_000, bid='10.20', ask='10.21', size='300'),
            quote(EXIT+100_000_000, bid='10.20', ask='10.21', size='300'),  # Before first exit order arrival.
            quote(EXIT+300_000_000, bid='10.18', ask='10.19', size='300'),  # Does not reach first exit limit.
            quote(EXIT+1_200_000_000, bid='10.17', ask='10.18', size='300'),
            quote(EXIT+1_550_000_000, bid='10.18', ask='10.19', size='300')]


class ExecutionModelTests(unittest.TestCase):
    def test_prearrival_and_unverified_quotes_never_fill(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        self.assertEqual(model.on_quote(quote(PUB+100_000_000, ask='8')), [])
        for field in ('size_unit_verified', 'conditions_eligible', 'continuity_verified', 'session_supported', 'market_tradable'):
            at = PUB+300_000_000
            q = replace(quote(at), **{field: False})
            self.assertEqual(model.on_quote(q), [])
        self.assertEqual(model.holdings(), {})

    def test_partial_fills_expiry_and_limit_price_are_conservative_assumptions(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        fills = model.on_quote(quote(PUB+300_000_000, bid='8.99', ask='9.00'))
        self.assertEqual(fills[0]['qty'], 10)
        self.assertEqual(fills[0]['price'], '10.02')  # No favorable pre-entry or price-improvement credit.
        model.advance(PUB+SECOND)
        self.assertEqual(model.orders['SYNTHETIC_ENTRY'].state, 'expired')
        self.assertEqual(model.on_quote(quote(PUB+SECOND+1, bid='.99', ask='1', size='10000')), [])

    def test_duplicate_unchanged_quotes_do_not_refill_consumed_book(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        self.assertEqual(sum(fill['qty'] for fill in model.on_quote(quote(PUB+300_000_000))), 10)
        self.assertEqual(model.on_quote(quote(PUB+400_000_000)), [])
        self.assertEqual(sum(fill['qty'] for fill in model.on_quote(quote(PUB+500_000_000, size='150'))), 5)
        self.assertEqual(model.on_quote(quote(PUB+600_000_000, size='100')), [])

    def test_old_source_received_after_arrival_is_not_a_postarrival_fill(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        q = replace(quote(PUB+300_000_000), source_utc_ns=PUB+100_000_000)
        self.assertEqual(model.on_quote(q), [])

    def test_same_source_timestamp_as_order_arrival_does_not_resolve_sequence(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        self.assertEqual(model.on_quote(quote(PUB+250_000_000)), [])

    def test_string_false_is_not_a_verified_capability(self):
        with self.assertRaises(ValueError):
            replace(quote(PUB), size_unit_verified='false')

    def test_cannot_oversell_or_exit_an_entry_whose_remaining_order_is_live(self):
        model = ConservativeExecutionModel(); model.submit(buy())
        model.on_quote(quote(PUB+300_000_000))
        sell = ModelOrder('SELL', 'E', 'ZZTEST', 'sell', 10, '10', PUB+500_000_000, PUB+600_000_000, PUB+2*SECOND, 'SYNTHETIC', 'SYNTHETIC_ENTRY')
        with self.assertRaises(ValueError):
            model.submit(sell)
        model.advance(PUB+SECOND)
        sell.submitted_utc_ns=PUB+SECOND; sell.arrives_utc_ns=PUB+SECOND+1; sell.qty=11
        with self.assertRaises(ValueError):
            model.submit(sell)

    def test_historical_availability_requires_explicit_later_retrieval(self):
        with self.assertRaises(ValueError):
            quote(PUB, mode='HISTORICAL')
        q = quote(PUB, mode='HISTORICAL', retrieved_at_utc_ns=PUB+86400*SECOND)
        self.assertEqual(q.availability_kind, 'assumed_from_source_plus_delay')
        self.assertNotEqual(q.available_utc_ns, q.retrieved_at_utc_ns)


class ReplayTests(unittest.TestCase):
    def test_synthetic_full_round_trip_preserves_partial_entry_and_repriced_exit(self):
        assumptions = ExecutionAssumptions(each_side_cost_bps='5', per_share_cost='0.001', per_filled_order_cost='0.02')
        result = replay_cases([case()], synthetic_quotes(), plan(), assumptions=assumptions)
        r = result['cases'][0]
        self.assertEqual(r['entry_filled_qty'], 25)
        self.assertEqual(r['entry_unfilled_qty'], 74)
        self.assertEqual(r['exit_filled_qty'], 25)
        self.assertEqual(r['exit_attempts'], 2)
        self.assertEqual(r['last_exit_fill_delay_ns'], 1_550_000_000)
        self.assertEqual(Decimal(r['realized_model_pnl']), Decimal('3.407625'))
        self.assertEqual(result['execution_model']['cash'], '100003.407625')
        self.assertEqual(result['execution_model']['open_positions'], {})
        self.assertFalse(result['empirical_trading_performance_established'])
        self.assertEqual(result['observed_broker_trade_count'], 0)

    def test_exact_ceiling_and_extraction_only_timing_do_not_create_entries(self):
        for changed in (case(processing_time_ns=5*SECOND), case(processing_scope='llm_only')):
            result = replay_cases([changed], synthetic_quotes(), plan())
            self.assertEqual(result['qualified_case_count'], 0)
            self.assertEqual(result['execution_model']['orders'], [])

    def test_saved_inputs_reproduce_the_same_model_fills_and_configuration(self):
        original = replay_cases([case()], synthetic_quotes(), plan())
        restored = reproduce_report(json.loads(json.dumps(original)))
        self.assertEqual(original['execution_model']['fills'], restored['execution_model']['fills'])
        self.assertEqual(original['configuration_hash'], restored['configuration_hash'])
        self.assertEqual(original['dataset_hash'], restored['dataset_hash'])

    def test_changed_inputs_cannot_be_called_exact_reproduction(self):
        original = replay_cases([case()], synthetic_quotes(), plan())
        original['input_quotes'][0]['ask'] = '1'
        with self.assertRaisesRegex(ValueError, 'captured inputs or implementation changed'):
            reproduce_report(original)

    def test_missing_point_in_time_evidence_means_skip_even_for_tempting_prices(self):
        evidence = {key: True for key in REQUIRED_EVIDENCE}; evidence['expectations_point_in_time'] = False
        result = replay_cases([case(evidence=evidence)], synthetic_quotes(), plan())
        self.assertIn('missing_evidence:expectations_point_in_time', result['cases'][0]['reasons'])
        self.assertEqual(result['execution_model']['fills'], [])

    def test_no_receipt_assumption_is_invented(self):
        result = replay_cases([case(assumed_feed_delay_ns=None)], synthetic_quotes(), plan())
        self.assertIn('missing_or_invalid_receipt_processing_provenance', result['cases'][0]['reasons'])

    def test_holiday_and_early_close_exit_uses_exchange_calendar(self):
        p = parse_rfc3339('2026-11-25T22:00:00Z')[0]
        target = parse_rfc3339('2026-11-27T15:00:00Z')[0]
        event = case(publication_utc_ns=p, qualified=False)
        pplan = plan(windows={'development': (p-SECOND, target+SECOND)})
        result = replay_cases([event], [], pplan)
        self.assertEqual(result['cases'][0]['scheduled_exit_utc'], '2026-11-27T15:00:00.000000000Z')

    def test_conflicting_same_timestamp_quotes_cannot_choose_better_state(self):
        ready = PUB+7*SECOND
        quotes = [quote(ready-100_000_000, ask='10.02'), quote(ready+300_000_000), quote(ready+300_000_000, bid='8.99', ask='9')]
        result = replay_cases([case()], quotes, plan())
        self.assertEqual(result['execution_model']['fills'], [])

    def test_qualified_flag_cannot_substitute_a_missing_current_entry_quote(self):
        result = replay_cases([case()], [quote(PUB+8*SECOND)], plan())
        self.assertIn('missing_fresh_eligible_entry_decision_quote', result['cases'][0]['reasons'])
        self.assertEqual(result['execution_model']['orders'], [])

    def test_daily_loss_pause_survives_recovery_and_does_not_prevent_existing_exit(self):
        second = case(case_id='SYNTHETIC_SECOND', announcement_key='SECOND_ANNOUNCEMENT', symbol='ZZNEXT', publication_utc_ns=PUB+120*SECOND)
        quotes = synthetic_quotes()
        # Fully fill the first entry, then mark a >$500 loss before recovery.
        quotes = [replace(q, bid_size_shares='1000', ask_size_shares='1000') for q in quotes]
        quotes += [quote(PUB+10*SECOND, bid='4.00', ask='4.01', size='1000'),
                   quote(PUB+126_900_000_000, bid='10.02', ask='10.03', size='1000'),
                   replace(quote(PUB+126_900_000_000, ask='10.02', size='1000'), symbol='ZZNEXT')]
        result = replay_cases([case(), second], quotes, plan())
        self.assertIn('daily_loss_pause_active', result['cases'][1]['reasons'])
        self.assertEqual(result['cases'][0]['exit_status'], 'closed')
        self.assertEqual(result['portfolio_risk']['daily_pause_dates'], ['2026-11-02'])

    def test_drawdown_review_persists_and_missing_close_never_becomes_a_zero_loss(self):
        risk = ReplayRiskState()
        risk.observe(PUB, Decimal('95000'))
        risk.observe(PUB+SECOND, Decimal('100000'))
        self.assertTrue(risk.review_required)
        risk.close(parse_rfc3339('2026-11-03T21:00:00Z')[0], None)
        reasons = risk.rejection_reasons(parse_rfc3339('2026-11-04T15:00:00Z')[0], Decimal('100000'))
        self.assertIn('missing_previous_close_portfolio_baseline', reasons)

    def test_overlapping_splits_and_cross_boundary_outcomes_are_rejected(self):
        with self.assertRaises(ValueError):
            plan(windows={'development': (PUB, PUB+10), 'validation': (PUB+9, PUB+20)})
        result = replay_cases([case()], [], plan(windows={'development': (PUB-SECOND, EXIT)}))
        self.assertIn('outcome_crosses_split_boundary', result['cases'][0]['reasons'])

    def test_evaluation_registry_blocks_retuning_on_same_event_and_allows_exact_reproduction(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = EvaluationRegistry(Path(tmp)/'evaluation.sqlite')
            real_case = case()  # Fabricated contents exercise historical-mode protocol, not empirical results.
            pplan = plan(mode='HISTORICAL', split='evaluation', windows={'development': (PUB-20*SECOND, PUB-10*SECOND),
                         'validation': (PUB-10*SECOND, PUB-2*SECOND), 'evaluation': (PUB-SECOND, EXIT+120*SECOND)},
                         selection_cutoff_utc_ns=PUB-3*SECOND, locked_at_utc_ns=PUB-2*SECOND)
            result = replay_cases([real_case], [], pplan, evaluation_registry=registry)
            self.assertEqual(result['evaluation_status'], 'first_registered_evaluation')
            with self.assertRaises(ValueError):
                replay_cases([real_case], [], replace(pplan, strategy_version='RETUNED'), evaluation_registry=registry)
            result = replay_cases([real_case], [], pplan, evaluation_registry=registry, reproduction=True)
            self.assertEqual(result['evaluation_status'], 'exact_reproduction_of_registered_evaluation')

    def test_historical_journal_never_assumes_share_units_or_original_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'SYNTHETIC_HISTORICAL_SOURCE.sqlite'
            journal = Journal(path, {'mode': 'HISTORICAL_DEVELOPMENT', 'fixture': 'SYNTHETIC'})
            journal.emit('historical_page', {'schema_valid': True, 'feed': 'sip', 'raw': json.dumps({'quotes': {'ZZTEST': [
                {'t': '2026-11-02T21:30:01Z', 'bp': '10', 'ap': '10.01', 'bs': 10, 'as': 20, 'c': ['R']} ]}})})
            journal.emit('historical_result', {'complete': True}); journal.close()
            observations, info = historical_quotes_from_journal(path, 'ZZTEST', assumed_quote_delivery_delay_ns=250_000_000)
            q = observations[0]
            self.assertEqual(q.available_utc_ns-q.source_utc_ns, 250_000_000)
            self.assertIsNone(q.ask_size_shares)
            self.assertFalse(q.size_unit_verified)
            self.assertNotEqual(q.available_utc_ns, q.retrieved_at_utc_ns)
            self.assertIn('unverified_share_size', q.issues(q.available_utc_ns))
            # Explicit unit conversion comes from an external, instrument/date-aware verifier.
            def verified(raw, source_ns):
                return QuoteCapabilities('1', 'SYNTHETIC documented shares', True, True, True, True, ('SYNTHETIC capabilities proof',))
            observations, _ = historical_quotes_from_journal(path, 'ZZTEST', assumed_quote_delivery_delay_ns=0, capabilities_for_quote=verified)
            self.assertEqual(observations[0].ask_size_shares, '20')
            self.assertTrue(observations[0].size_unit_verified)
