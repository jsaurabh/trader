"""Background research lifecycle with fabricated adapters; no HTTP requests."""
import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from recorder.collectors import Recorder, run_metadata
from recorder.journal import Journal


class FollowupIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_optional_research_shares_client_classifies_without_model_and_closes(self):
        instances, document_callbacks = [], []
        class Research:
            def __init__(self, path, root, client, **kwargs):
                self.client, self.kwargs, self.ran, self.closed = client, kwargs, False, False
                instances.append(self)
            async def run(self):
                self.ran = True
                await asyncio.Event().wait()
            async def aclose(self):
                self.closed = True
        class Documents:
            worker_count = 1
            def __init__(self, journal, client, *, on_result=None):
                self.pending = asyncio.Queue()
                document_callbacks.append(on_result)
            async def worker(self):
                await asyncio.Event().wait()
        async def wait_forever(*args):
            await asyncio.Event().wait()
        async def market(recorder):
            recorder.market_metadata_ready.set()
            await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as directory:
            for enabled in (False, True):
                journal = Journal(Path(directory)/f'{enabled}.sqlite', run_metadata(['ZZTEST'], 'iex'))
                try:
                    recorder = Recorder(journal, ['ZZTEST'], contexts={'ZZTEST': {'fixture': 'SYNTHETIC'}},
                        followup_symbols=['ZZTEST'] if enabled else [])
                    with patch.dict('os.environ', {'RTPR_API_KEY':'SYNTHETIC','ALPACA_API_KEY':'SYNTHETIC','ALPACA_API_SECRET':'SYNTHETIC'}), \
                            patch('recorder.research_followup.HistoricalResearchFollowup', Research), \
                            patch('recorder.collectors.DocumentCollector', Documents), \
                            patch.object(Recorder, 'wire', wait_forever), \
                            patch.object(Recorder, 'market', market), \
                            patch.object(Recorder, 'health', wait_forever), \
                            patch('httpx.AsyncClient.get', side_effect=AssertionError('Network forbidden')):
                        await recorder.run(.025)
                    self.assertFalse(recorder.analysis_enabled)
                    self.assertIsNone(recorder.analyzer)
                    self.assertEqual(document_callbacks[-1] is not None, enabled)
                    if enabled:
                        self.assertTrue(instances[-1].ran)
                        self.assertTrue(instances[-1].closed)
                        self.assertIs(instances[-1].client, recorder.market_inputs.client)
                        self.assertEqual(instances[-1].kwargs['symbols'], ['ZZTEST'])
                    else:
                        self.assertIsNone(recorder.research_followup)
                finally:
                    journal.close()
