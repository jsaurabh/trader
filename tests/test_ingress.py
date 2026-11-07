"""Real websockets 16 parser, fabricated frames and in-memory transport only."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from websockets.client import ClientProtocol
from websockets.datastructures import Headers
from websockets.frames import Frame, OP_BINARY, OP_CONT, OP_PING, OP_PONG, OP_TEXT
from websockets.http11 import Response
from websockets.protocol import OPEN
from websockets.uri import parse_uri

from recorder.collectors import Recorder, run_metadata
from recorder.ingress import IngressError, StampedClientConnection, receive_stamped, ingress_metadata
from recorder.journal import Journal, read_rows
from recorder.report import quote_at
from recorder.timebase import SECOND, Stamp, iso_utc


class Clock:
    def __init__(self, now=None): self.value = now or Stamp.now()
    def now(self): return self.value
    def advance(self, seconds):
        self.value = Stamp(self.value.utc_ns+seconds*SECOND, self.value.mono_ns+seconds*SECOND)


class MemoryTransport(asyncio.Transport):
    def __init__(self): self.aborted, self.writes, self.pauses = False, [], 0
    def set_write_buffer_limits(self, high=None, low=None): pass
    def write(self, data): self.writes.append(data)
    def pause_reading(self): self.pauses += 1
    def resume_reading(self): pass
    def abort(self): self.aborted = True
    def is_closing(self): return self.aborted
    def get_extra_info(self, name, default=None): return default


def packet(opcode, data, *, fin=True):
    return Frame(opcode, data, fin=fin).serialize(mask=False)


def connection(clock=None, **kwargs):
    clock = clock or Clock()
    protocol = ClientProtocol(parse_uri("wss://synthetic.invalid/"), state=OPEN)
    conn = StampedClientConnection(protocol, clock=clock.now, ping_interval=None, **kwargs)
    transport = MemoryTransport()
    conn.connection_made(transport)
    conn.response = Response(101, "Switching Protocols", Headers())
    return conn, clock, transport


class StampedIngressTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragment_control_and_binary_messages_preserve_first_callback_fifo(self):
        conn, clock, transport = connection()
        first = clock.now()
        conn.data_received(packet(OP_TEXT, b"SYNTHETIC ", fin=False))
        clock.advance(3)
        conn.data_received(packet(OP_PING, b"health")+packet(OP_PONG, b"unsolicited")+
            packet(OP_CONT, b"fragment", fin=True)+packet(OP_BINARY, b"second"))
        read = clock.now()
        self.assertEqual(await conn.recv_stamped(), ("SYNTHETIC fragment", first, read))
        self.assertEqual(await conn.recv_stamped(), (b"second", read, read))
        self.assertTrue(transport.writes)  # Actual library automatic pong, no socket.
        self.assertEqual(ingress_metadata(first, read)["library_queue_delay_ns"], 3*SECOND)

    async def test_cancellation_before_first_frame_and_midfragment_do_not_remove_stamp(self):
        conn, clock, _ = connection()
        empty = asyncio.create_task(conn.recv_stamped())
        await asyncio.sleep(0)
        empty.cancel()
        with self.assertRaises(asyncio.CancelledError): await empty
        first = clock.now()
        conn.data_received(packet(OP_TEXT, b"first", fin=False))
        fragmented = asyncio.create_task(conn.recv_stamped())
        await asyncio.sleep(0)
        fragmented.cancel()
        with self.assertRaises(asyncio.CancelledError): await fragmented
        clock.advance(6)
        conn.data_received(packet(OP_CONT, b" completed")+packet(OP_TEXT, b"second"))
        self.assertEqual(await conn.recv_stamped(), ("first completed", first, clock.now()))
        self.assertEqual(await conn.recv_stamped(), ("second", clock.now(), clock.now()))

    async def test_overflow_is_permanent_and_cannot_yield_previously_queued_prefix(self):
        conn, clock, transport = connection(stamp_capacity=1)
        conn.data_received(packet(OP_TEXT, b"one")+packet(OP_TEXT, b"two"))
        self.assertTrue(transport.aborted)
        for _ in range(2):
            with self.assertRaises(IngressError) as failure: await conn.recv_stamped()
            self.assertEqual(failure.exception.code, "ingress_stamp_capacity_exceeded")
            clock.advance(10)
            conn.data_received(packet(OP_TEXT, b"never a fresh replacement"))

    async def test_sequence_or_assembler_mismatch_aborts_instead_of_fabricating_receipt(self):
        conn, _, transport = connection()
        conn.data_received(packet(OP_TEXT, b"one"))
        conn._message_stamps[0].sequence += 1  # Explicit synthetic internal corruption.
        with self.assertRaises(IngressError) as failure: await conn.recv_stamped()
        self.assertEqual(failure.exception.code, "ingress_message_sequence_mismatch")
        self.assertTrue(transport.aborted)
        other, _, transport = connection()
        other.recv_messages.put(Frame(OP_TEXT, b"bypassed ingress"))
        with self.assertRaises(IngressError) as failure: await other.recv_stamped()
        self.assertEqual(failure.exception.code, "received_message_without_ingress_stamp")
        self.assertTrue(transport.aborted)

    async def test_unstamped_receive_apis_are_disabled(self):
        for streaming in (False, True):
            conn, _, transport = connection()
            with self.assertRaises(IngressError):
                if streaming:
                    await anext(conn.recv_streaming())
                else:
                    await conn.recv()
            self.assertTrue(transport.aborted)

    async def test_missing_custom_hook_never_calls_plain_recv(self):
        class Plain:
            async def recv(inner): self.fail("Production must not use a later fallback receipt")
        with self.assertRaises(IngressError) as failure: await receive_stamped(Plain())
        self.assertEqual(failure.exception.code, "stamped_receive_hook_missing")


class CollectorIngressTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/"synthetic-ingress-observation.sqlite"
        self.journal = Journal(self.path, {**run_metadata(["ZZTEST"], "sip"), "fixture_type": "SYNTHETIC_NETWORK_FREE_TEST"})
        self.recorder = Recorder(self.journal, ["ZZTEST"], analyze=True, feed="sip")

    async def asyncTearDown(self):
        tasks = list(self.recorder.checkpoints)
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.journal.close()
        self.tmp.cleanup()

    async def test_multiple_library_queued_alerts_consume_original_five_seconds(self):
        now = Stamp.now()
        ingress = Stamp(now.utc_ns-6*SECOND, now.mono_ns-6*SECOND)
        conn, clock, _ = connection(Clock(ingress))
        alerts = [{"type": "alert", "ticker": "ZZTEST", "article_url": "https://rtpr.io/a/"+event,
                   "article_published_at": iso_utc(ingress.utc_ns-SECOND)} for event in ("a", "b")]
        conn.data_received(b"".join(packet(OP_TEXT, json.dumps(alert).encode()) for alert in alerts))
        clock.advance(6)
        enqueued, ready = [], asyncio.Event()
        class Documents:
            def enqueue(inner, *args, **kwargs):
                enqueued.append((args, kwargs))
                if len(enqueued) == 2: ready.set()
        @asynccontextmanager
        async def fake_connect(*args, **kwargs):
            self.assertIs(kwargs["create_connection"], StampedClientConnection)
            yield conn
        with patch.dict("os.environ", {"RTPR_API_KEY": "SYNTHETIC"}), patch("recorder.collectors.connect", fake_connect):
            wire = asyncio.create_task(self.recorder.wire(Documents()))
            try:
                await asyncio.wait_for(ready.wait(), 1)
                await asyncio.sleep(0.02)
            finally:
                wire.cancel()
                await asyncio.gather(wire, return_exceptions=True)
        self.assertEqual([args[2] for args, _ in enqueued], [ingress, ingress])
        for event in ("a", "b"):
            self.assertEqual(self.recorder.announcement_clocks.alerts[event].receipt, ingress)
            self.assertIn("five_second_deadline_expired", self.recorder.final_decisions[event]["reasons"])
        self.journal.close()
        rows = list(read_rows(self.path, kinds=("wire_frame", "alert")))
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(row["stamp"], ingress)
            self.assertEqual(row["data"]["library_queue_delay_ns"], 6*SECOND)
            self.assertEqual(row["data"]["application_read_mono_ns"], now.mono_ns)

    async def test_fragmented_quote_is_not_known_before_complete_message_availability(self):
        conn, clock, _ = connection()
        ingress = clock.now()
        future_source = ingress.utc_ns+2*SECOND
        raw = json.dumps([{"T": "q", "S": "ZZTEST", "t": iso_utc(future_source),
            "bp": 10, "ap": 10.02, "bs": 100, "as": 100, "c": ["R"], "z": "C"}]).encode()
        half = len(raw)//2
        conn.data_received(packet(OP_TEXT, raw[:half], fin=False))
        waiting = asyncio.create_task(conn.recv_stamped())
        await asyncio.sleep(0)
        clock.advance(2)
        conn.data_received(packet(OP_CONT, raw[half:]))
        message, first, available = await waiting
        self.recorder.handle_market(message, first, available)
        self.journal.close()
        frames = list(read_rows(self.path, kinds=("market_frame",)))
        quotes = list(read_rows(self.path, kinds=("quote",)))
        self.assertEqual(frames[0]["stamp"], ingress)
        self.assertEqual(quotes[0]["stamp"], available)
        self.assertEqual(quotes[0]["data"]["transport_callback_mono_ns"], ingress.mono_ns)
        between = Stamp(ingress.utc_ns+SECOND, ingress.mono_ns+SECOND)
        self.assertEqual(quote_at(quotes, between, "sip", True)["status"], "missing")
        self.assertEqual(quote_at(quotes, available, "sip", True)["ask"], "10.02")

    async def test_permanent_ingress_fault_is_recorded_and_never_retimed_by_collector(self):
        conn, _, _ = connection(stamp_capacity=1)
        conn.data_received(packet(OP_TEXT, b"one")+packet(OP_TEXT, b"two"))
        @asynccontextmanager
        async def fake_connect(*args, **kwargs): yield conn
        async def stop_after_gap(*args): raise asyncio.CancelledError
        with patch.dict("os.environ", {"RTPR_API_KEY": "SYNTHETIC"}), \
             patch("recorder.collectors.connect", fake_connect), patch("recorder.collectors.backoff", stop_after_gap):
            with self.assertRaises(asyncio.CancelledError): await self.recorder.wire(None)
        self.journal.close()
        self.assertEqual(list(read_rows(self.path, kinds=("alert",))), [])
        disconnects = [r["data"] for r in read_rows(self.path, kinds=("transport",)) if r["data"].get("state") == "disconnected"]
        self.assertEqual(disconnects[0]["ingress_fault"], "ingress_stamp_capacity_exceeded")


if __name__ == "__main__": unittest.main()
