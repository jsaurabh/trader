"""Local status must not reset a running ledger's reconciliation state."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from recorder.cli import main
from recorder.portfolio import PaperLedger


class ReadOnlyStatusTests(unittest.TestCase):
    def test_status_preserves_reconciliation_and_omits_account_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory)/"fabricated.sqlite", mode="SYNTHETIC")
            with ledger.transaction() as connection:
                ledger.put(connection, "reconciled", True)
                ledger.put(connection, "account_hash", "FABRICATED_PRIVATE_IDENTITY")
            before = ledger.flags()
            with ledger.read() as connection:
                count_before = connection.execute("SELECT count(*) FROM ledger_events").fetchone()[0]
            stream = io.StringIO()
            with redirect_stdout(stream):
                result = main(["paper-status", "--state", str(ledger.path)])
            self.assertEqual(result, 0)
            self.assertEqual(ledger.flags(), before)
            with ledger.read() as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM ledger_events").fetchone()[0], count_before)
            exported = json.loads(stream.getvalue())
            self.assertTrue(exported["flags"]["reconciled"])
            self.assertEqual(exported["mode"], "SYNTHETIC")
            self.assertNotIn("FABRICATED_PRIVATE_IDENTITY", stream.getvalue())
            self.assertFalse(exported["live_orders_enabled"])
