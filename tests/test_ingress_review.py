"""Independent ingress regressions using real parsers and no socket/network I/O."""
import asyncio
import unittest

from websockets.client import ClientProtocol
from websockets.exceptions import ConcurrencyError
from websockets.frames import Frame, OP_BINARY, OP_CONT, OP_PING, OP_TEXT
from websockets.datastructures import Headers
from websockets.http11 import Response
from websockets.protocol import OPEN
from websockets.uri import parse_uri

from recorder.ingress import IngressError, StampedClientConnection
from recorder.timebase import SECOND, Stamp


class FixtureClock:
    def __init__(self):
        self.value = Stamp(1_700_000_000*SECOND, 10*SECOND)

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value = Stamp(self.value.utc_ns+seconds*SECOND, self.value.mono_ns+seconds*SECOND)


class MemoryTransport(asyncio.Transport):
    def __init__(self):
        self.writes, self.pauses, self.resumes = [], 0, 0
        self.aborted, self.closed = False, False

    def set_write_buffer_limits(self, high=None, low=None):
        pass

    def write(self, data):
        self.writes.append(data)

    def pause_reading(self):
        self.pauses += 1

    def resume_reading(self):
        self.resumes += 1

    def is_closing(self):
        return self.aborted or self.closed

    def abort(self):
        self.aborted = True

    def close(self):
        self.closed = True

    def get_extra_info(self, name, default=None):
        return default


def packet(opcode, data, *, fin=True):
    return Frame(opcode, data, fin=fin).serialize(mask=False)


class IngressReviewTests(unittest.IsolatedAsyncioTestCase):
    def connection(self, **kwargs):
        clock = FixtureClock()
        protocol = ClientProtocol(parse_uri("wss://fabricated.invalid/"), state=OPEN)
        connection = StampedClientConnection(protocol, clock=clock.now, ping_interval=None, **kwargs)
        transport = MemoryTransport()
        connection.connection_made(transport)
        # The connection is already OPEN: fabricate the completed handshake, too.
        connection.response = Response(101, "Switching Protocols", Headers())
        return connection, clock, protocol, transport

    async def test_callback_boundary_includes_parser_and_control_write_delay(self):
        connection, clock, protocol, _ = self.connection()
        receipt = clock.now()
        original_receive = protocol.receive_data
        def delayed_parse(data):
            original_receive(data)
            clock.advance(6)
        protocol.receive_data = delayed_parse
        original_send = connection.send_data
        def delayed_control_write():
            clock.advance(4)
            original_send()
        connection.send_data = delayed_control_write
        connection.data_received(packet(OP_TEXT, b'first')+packet(OP_TEXT, b'second'))
        for expected in ('first', 'second'):
            raw, ingress, read = await connection.recv_stamped()
            self.assertEqual(raw, expected)
            self.assertEqual(ingress, receipt)
            self.assertEqual(read.mono_ns-receipt.mono_ns, 10*SECOND)

    async def test_cancel_midfragment_preserves_original_stamp_and_fifo(self):
        connection, clock, _, _ = self.connection()
        receipt = clock.now()
        connection.data_received(packet(OP_TEXT, b'first ', fin=False))
        waiting = asyncio.create_task(connection.recv_stamped())
        await asyncio.sleep(0)  # Consume the first fragment and await continuation.
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        clock.advance(6)
        final_receipt = clock.now()
        connection.data_received(packet(OP_PING, b'probe')+packet(OP_CONT, b'event')+packet(OP_TEXT, b'next'))
        raw, ingress, read = await connection.recv_stamped()
        self.assertEqual((raw, ingress), ('first event', receipt))
        self.assertEqual(read.mono_ns-ingress.mono_ns, 6*SECOND)
        raw, ingress, _ = await connection.recv_stamped()
        self.assertEqual((raw, ingress), ('next', final_receipt))

    async def test_control_frames_do_not_consume_message_timestamps(self):
        connection, clock, _, transport = self.connection()
        receipt = clock.now()
        connection.data_received(packet(OP_PING, b'probe')+packet(OP_BINARY, b'one')+packet(OP_TEXT, b'two'))
        first = await connection.recv_stamped()
        second = await connection.recv_stamped()
        self.assertEqual(first[:2], (b'one', receipt))
        self.assertEqual(second[:2], ('two', receipt))
        self.assertTrue(transport.writes)  # Base protocol still sends its pong.

    async def test_concurrent_receive_rejection_does_not_drop_next_timestamp(self):
        connection, clock, _, _ = self.connection()
        first_waiter = asyncio.create_task(connection.recv_stamped())
        await asyncio.sleep(0)
        with self.assertRaises(ConcurrencyError):
            await connection.recv_stamped()
        receipt = clock.now()
        connection.data_received(packet(OP_TEXT, b'first'))
        self.assertEqual((await first_waiter)[:2], ('first', receipt))
        clock.advance(1)
        receipt = clock.now()
        connection.data_received(packet(OP_TEXT, b'second'))
        self.assertEqual((await connection.recv_stamped())[:2], ('second', receipt))

    async def test_partial_frame_uses_callback_where_frame_becomes_available(self):
        connection, clock, _, _ = self.connection()
        encoded = packet(OP_TEXT, b'event')
        connection.data_received(encoded[:1])
        clock.advance(2)
        complete_callback = clock.now()
        connection.data_received(encoded[1:])
        raw, ingress, _ = await connection.recv_stamped()
        self.assertEqual((raw, ingress), ('event', complete_callback))

    async def test_stamp_capacity_overflow_cannot_return_an_unstamped_message(self):
        connection, _, _, transport = self.connection(stamp_capacity=1)
        connection.data_received(packet(OP_TEXT, b'one')+packet(OP_TEXT, b'two'))
        with self.assertRaises(IngressError):
            await connection.recv_stamped()
        self.assertTrue(transport.aborted or transport.closed)


if __name__ == "__main__":
    unittest.main()
