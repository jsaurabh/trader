import copy
import unittest

from recorder.analysis import ModelResponseError, numeric_tokens, resolve_block_facts, source_blocks, validate_facts


class BlockCitationTests(unittest.TestCase):
    def setUp(self):
        self.source = ('Example reports the quarter ended September 30, 2026.\n'
                       'Diluted EPS was $1.25. Revenue was $100 million.\n'
                       'All amounts are in U.S. dollars (USD).\n'
                       'GAAP revenue for the quarter ended September 30, 2026 was $100 million.\n')
        self.blocks = source_blocks(self.source)
        self.payload = {'ticker': 'EXAMPLE', 'missing_information': [], 'facts': [
            {'name': 'DILUTED_EPS', 'number_token_id': next(t['id'] for t in numeric_tokens(self.blocks) if t['value'] == '1.25'), 'scale': 'PER_SHARE', 'period_end': '2026-09-30',
             'period_kind': 'QUARTER', 'evidence_ids': [0, 1]}]}

    def test_every_block_maps_exactly_to_source(self):
        for block in self.blocks:
            self.assertEqual(self.source[block['start']:block['end']], block['text'])
        self.assertEqual(self.blocks[1]['text'], 'Diluted EPS was $1.25.')

    def test_currency_and_basis_are_determined_from_cited_source_only(self):
        resolved = resolve_block_facts(self.payload, self.blocks)
        fact = resolved['facts'][0]
        self.assertEqual(fact['unit'], 'UNSPECIFIED_CURRENCY_PER_SHARE')
        self.assertEqual(fact['basis'], 'UNSPECIFIED')
        self.assertEqual(validate_facts(resolved, self.source, 'EXAMPLE')[1], [])
        # An explicit header must actually be cited before USD can be used.
        usd_id = next(b['id'] for b in self.blocks if 'USD' in b['text'])
        self.payload['facts'][0]['evidence_ids'].append(usd_id)
        self.assertEqual(resolve_block_facts(self.payload, self.blocks)['facts'][0]['unit'], 'USD_PER_SHARE')

    def test_out_of_range_boolean_and_negative_ids_are_rejected(self):
        for invalid in (-1, True, 10000, '1'):
            self.payload['facts'][0]['evidence_ids'] = [invalid]
            with self.assertRaises(ModelResponseError):
                resolve_block_facts(self.payload, self.blocks)

    def test_model_cannot_add_currency_basis_or_evidence_text(self):
        for key in ('unit', 'basis', 'evidence'):
            payload = copy.deepcopy(self.payload)
            payload['facts'][0][key] = 'INVENTED'
            with self.assertRaises(ModelResponseError):
                resolve_block_facts(payload, self.blocks)

    def test_citation_does_not_make_an_invented_number_valid(self):
        self.payload['facts'][0]['value'] = '9.99'
        with self.assertRaises(ModelResponseError):
            resolve_block_facts(self.payload, self.blocks)

    def test_mixed_accounting_basis_remains_unspecified(self):
        source = 'GAAP diluted EPS was USD 1.25, and non-GAAP diluted EPS was USD 1.40 for the quarter ended September 30, 2026.'
        self.payload['facts'][0]['evidence_ids'] = [0]
        self.payload['facts'][0]['number_token_id'] = 0
        resolved = resolve_block_facts(self.payload, source_blocks(source))
        self.assertEqual(resolved['facts'][0]['basis'], 'UNSPECIFIED')

    def test_adjusted_label_for_another_eps_cannot_leak_to_unlabelled_eps(self):
        source = ('Diluted net income per share and non-GAAP diluted net income per share: '
                  'Diluted net income per share was $0.18, versus $0.31, while non-GAAP diluted net income per share was $0.36, versus $0.45 '
                  'for the quarter ended September 30, 2026.')
        self.payload['facts'][0]['number_token_id'] = next(t['id'] for t in numeric_tokens(source_blocks(source)) if t['value'] == '0.18')
        self.payload['facts'][0]['evidence_ids'] = [0]
        resolved = resolve_block_facts(self.payload, source_blocks(source))
        self.assertEqual(resolved['facts'][0]['basis'], 'UNSPECIFIED')
        resolved['facts'][0]['basis'] = 'NON_GAAP'
        self.assertIn('fact_0:accounting_basis_not_supported', validate_facts(resolved, source, 'EXAMPLE')[1])
        self.payload['facts'][0]['number_token_id'] = next(t['id'] for t in numeric_tokens(source_blocks(source)) if t['value'] == '0.36')
        self.assertEqual(resolve_block_facts(self.payload, source_blocks(source))['facts'][0]['basis'], 'NON_GAAP')

    def test_numeric_lexemes_preserve_offsets_and_do_not_convert_scale(self):
        source = 'Revenue was $174,537 thousand. Diluted EPS was ($0.20).'
        tokens = numeric_tokens(source_blocks(source))
        self.assertEqual([t['value'] for t in tokens], ['174537', '-0.20'])
        for token in tokens:
            self.assertEqual(source[token['start']:token['end']], token['raw'])

    def test_date_number_cannot_become_eps_by_citing_a_different_block(self):
        self.payload['facts'][0]['number_token_id'] = 0
        with self.assertRaisesRegex(ModelResponseError, 'number_token_metric_not_supported'):
            resolve_block_facts(self.payload, self.blocks)

    def test_a_thousands_table_cannot_be_scaled_using_another_millions_passage(self):
        source = 'Revenue was $174.5 million.\n(In thousands, except per-share amounts)\nRevenue was $174,537 thousand.'
        blocks = source_blocks(source)
        token_id = next(t['id'] for t in numeric_tokens(blocks) if t['value'] == '174537')
        payload = {'ticker': 'EXAMPLE', 'missing_information': [], 'facts': [
            {'name': 'REVENUE', 'number_token_id': token_id, 'scale': 'MILLIONS', 'period_end': None,
             'period_kind': 'QUARTER', 'evidence_ids': [0, 1, 2]}]}
        with self.assertRaisesRegex(ModelResponseError, 'number_token_scale_not_supported'):
            resolve_block_facts(payload, blocks)
        payload['facts'][0]['scale'] = 'THOUSANDS'
        self.assertEqual(resolve_block_facts(payload, blocks)['facts'][0]['value'], '174537')

    def test_table_columns_remain_ineligible_until_structurally_bound(self):
        source = 'Revenue\t$174,537\t$168,525'
        payload = {'ticker': 'EXAMPLE', 'missing_information': [], 'facts': [
            {'name': 'REVENUE', 'number_token_id': 0, 'scale': 'THOUSANDS', 'period_end': None,
             'period_kind': 'QUARTER', 'evidence_ids': [0]}]}
        with self.assertRaisesRegex(ModelResponseError, 'financial_table_column_binding_not_verified'):
            resolve_block_facts(payload, source_blocks(source))

    def test_prior_comparison_eps_cannot_be_selected_as_current(self):
        source = 'GAAP diluted EPS was USD 1.00 versus USD 1.50 for the quarter ended September 30, 2026.'
        blocks = source_blocks(source)
        self.payload['facts'][0].update(number_token_id=1, evidence_ids=[0])
        with self.assertRaises(ModelResponseError):
            resolve_block_facts(self.payload, blocks)
        self.payload['facts'][0]['number_token_id'] = 0
        self.assertEqual(resolve_block_facts(self.payload, blocks)['facts'][0]['value'], '1.00')

    def test_unique_unconditional_currency_header_can_be_attached_by_software(self):
        for suffix, expected in (('', 'USD_PER_SHARE'), (' unless otherwise indicated', 'UNSPECIFIED_CURRENCY_PER_SHARE')):
            source = 'All monetary amounts, including per-share amounts, are in U.S. dollars (USD)'+suffix+'.\nDiluted EPS was $1.25.'
            blocks = source_blocks(source)
            self.payload['facts'][0].update(number_token_id=0, evidence_ids=[1], period_end=None, period_kind='UNSPECIFIED')
            resolved = resolve_block_facts(self.payload, blocks)
            self.assertEqual(resolved['facts'][0]['unit'], expected)

    def test_conflicting_global_headers_never_grant_usd_status(self):
        source = 'All amounts are in U.S. dollars (USD).\nAll amounts are in Canadian dollars.\nDiluted EPS was $1.25.'
        blocks = source_blocks(source)
        self.payload['facts'][0].update(number_token_id=0, evidence_ids=[2], period_end=None, period_kind='UNSPECIFIED')
        self.assertEqual(resolve_block_facts(self.payload, blocks)['facts'][0]['unit'], 'UNSPECIFIED_CURRENCY_PER_SHARE')
