import tempfile
from pathlib import Path
import unittest

from recorder.case_study import conditional_markouts, parse_exhibit, quote_rows
from recorder.history import source_quote_at
from recorder.journal import Journal
from recorder.timebase import SECOND, parse_rfc3339


class DevelopmentCaseTests(unittest.TestCase):
    def test_exhibit_serializer_preserves_table_columns_and_ignores_script(self):
        text, tables = parse_exhibit(b'<html><body><p>Example release</p><table><tr><th>Quarter</th><th>Revenue</th></tr>'
                                    b'<tr><td>June 30, 2026</td><td>$100</td></tr></table><script>submit_order()</script></body></html>')
        self.assertEqual(tables, 1)
        self.assertIn('Quarter\tRevenue', text)
        self.assertIn('June 30, 2026\t$100', text)
        self.assertNotIn('submit_order', text)

    def test_markout_starts_at_later_ask_and_stresses_costs(self):
        p = parse_rfc3339('2026-11-02T21:30:00Z')[0]
        def q(t, bid, ask):
            from recorder.timebase import iso_utc
            return {'t': iso_utc(t), 'bp': bid, 'ap': ask, 'bs': 1, 'as': 1}
        observations = [q(p-1, '9', '9.01')]
        observations += [q(p+i*SECOND, '10', '10.02') for i in range(1, 12)]
        # A tempting future price cannot supply the earlier entry.
        observations += [q(p+30*SECOND, '20', '20.01')]
        exit_q = source_quote_at([q(p+100*SECOND, '10.10', '10.11')], p+100*SECOND, p, True)
        rows = conditional_markouts(observations, exit_q, p, p-SECOND, True)
        self.assertEqual(len(rows), 18)
        first = rows[0]
        self.assertEqual(first['entry_source_observation']['ask'], '10.02')
        self.assertFalse(first['filled'])
        from decimal import Decimal
        self.assertEqual(Decimal(first['ask_to_exit_bid_pct']), 100*(Decimal('10.10')/Decimal('10.02')-1))
        self.assertEqual(Decimal(first['after_additional_round_trip_cost_pct']['25_bps']), Decimal(first['ask_to_exit_bid_pct'])-Decimal('.25'))
        self.assertNotEqual(first['entry_source_observation']['ask'], '9.01')

    def test_partial_or_stale_quotes_never_produce_markout(self):
        p = parse_rfc3339('2026-11-02T21:30:00Z')[0]
        quotes = [{'t': '2026-11-02T21:30:00Z', 'bp': '10', 'ap': '10.01', 'bs': 1, 'as': 1}]
        exit_q = source_quote_at(quotes, p, p, True)
        for complete in (True, False):
            rows = conditional_markouts(quotes, exit_q, p, p, complete)
            self.assertTrue(all('ask_to_exit_bid_pct' not in row for row in rows))

    def test_interrupted_history_is_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'partial.sqlite'
            journal = Journal(path, {'mode': 'HISTORICAL_DEVELOPMENT'})
            journal.emit('historical_request', {'start_utc': '2026-11-02T21:30:00Z'})
            journal.close()
            self.assertEqual(quote_rows(path, 'AAPL'), ([], parse_rfc3339('2026-11-02T21:30:00Z')[0], False))
