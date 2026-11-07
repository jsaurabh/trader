"""Timestamp WebSocket messages before the library's parse and message queue.

For websockets 16, data_received() parses the received chunk and writes automatic
control responses before process_event() queues data frames in its Assembler.
Capture at callback entry and bind that Stamp to each first TEXT/BINARY frame
completed in the callback. Fragments retain that first-frame callback Stamp.

This includes callback parse, remaining fragments and application queueing. It
doesn't measure NIC arrival, kernel/TLS buffering, time before the transport
callback, or earlier callbacks containing only an incomplete first frame.
"""
from collections import deque
from dataclasses import dataclass

from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConcurrencyError
from websockets.frames import Frame, OP_BINARY, OP_CONT, OP_TEXT

from .timebase import Stamp


RECEIPT_BOUNDARY = "transport data_received callback entry before protocol parse, bound to first data frame completed in callback"
UNMEASURED_BEFORE_RECEIPT = "NIC/kernel/TLS buffering, pre-callback time, and earlier callbacks containing incomplete first-frame bytes"
INGRESS_VERSION = "websockets16-first-data-frame-callback-v1"


class IngressError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass
class _MessageStamp:
    sequence: int
    received: Stamp
    complete: bool


class StampedClientConnection(ClientConnection):
    """FIFO timestamps with the same cancellation semantics as base recv()."""
    def __init__(self, *args, clock=Stamp.now, stamp_capacity=1024, **kwargs):
        if type(stamp_capacity) is not int or not 0 < stamp_capacity <= 65536:
            raise ValueError("invalid_ingress_stamp_capacity")
        super().__init__(*args, **kwargs)
        self._clock, self.stamp_capacity = clock, stamp_capacity
        self._message_stamps = deque()
        self._callback_stamp, self._fragment = None, None
        self._next_sequence, self._last_consumed = 1, 0
        self._receiving, self._ingress_error = False, None

    def _fail(self, code):
        if self._ingress_error is None:
            self._ingress_error = IngressError(code)
            self.set_recv_exc(self._ingress_error)
            if hasattr(self, "recv_messages"):
                self.recv_messages.close()
            if hasattr(self, "transport"):
                self.transport.abort()
        return self._ingress_error

    def data_received(self, data):
        if self._ingress_error is not None:
            return
        if self._callback_stamp is not None:
            self._fail("reentrant_ingress_transport_callback")
            return
        self._callback_stamp = self._clock()
        try:
            super().data_received(data)
        finally:
            self._callback_stamp = None

    def process_event(self, event):
        if self._ingress_error is not None:
            return
        if isinstance(event, Frame) and event.opcode in (OP_TEXT, OP_BINARY, OP_CONT):
            if self._callback_stamp is None:
                self._fail("data_frame_without_transport_callback_stamp")
                return
            if event.opcode in (OP_TEXT, OP_BINARY):
                if self._fragment is not None:
                    self._fail("new_message_before_fragment_completion")
                    return
                if len(self._message_stamps) >= self.stamp_capacity:
                    self._fail("ingress_stamp_capacity_exceeded")
                    return
                record = _MessageStamp(self._next_sequence, self._callback_stamp, event.fin)
                self._next_sequence += 1
                # Enqueue before super.process_event puts the frame in Assembler.
                self._message_stamps.append(record)
                if not event.fin:
                    self._fragment = record
            else:
                if self._fragment is None:
                    self._fail("continuation_without_first_frame_stamp")
                    return
                if event.fin:
                    self._fragment.complete = True
                    self._fragment = None
        try:
            super().process_event(event)
        except (AssertionError, EOFError):
            self._fail("ingress_assembler_frame_mismatch")

    async def recv_stamped(self, decode=None):
        if self._ingress_error is not None:
            raise self._ingress_error
        if self._receiving:
            raise ConcurrencyError("another stamped receive is already running")
        self._receiving = True
        try:
            # Canceling base recv restores fragments. Never remove its timestamp
            # until a complete message returns; there is no await before pop.
            raw = await super().recv(decode)
            read = self._clock()
            if self._ingress_error is not None:
                raise self._ingress_error
            if not self._message_stamps:
                raise self._fail("received_message_without_ingress_stamp")
            record = self._message_stamps[0]
            if record.sequence != self._last_consumed+1 or not record.complete:
                raise self._fail("ingress_message_sequence_mismatch")
            if read.mono_ns < record.received.mono_ns:
                raise self._fail("ingress_monotonic_clock_regressed")
            self._message_stamps.popleft()
            self._last_consumed = record.sequence
            return raw, record.received, read
        finally:
            self._receiving = False

    async def recv(self, decode=None):
        raise self._fail("unstamped_receive_disabled")

    async def recv_streaming(self, decode=None):
        raise self._fail("unstamped_streaming_receive_disabled")
        yield  # Keep the base async-iterator API shape; execution never reaches it.


async def receive_stamped(connection):
    """Require the production hook; never reset a receipt after library queueing."""
    method = getattr(connection, "recv_stamped", None)
    if not callable(method):
        raise IngressError("stamped_receive_hook_missing")
    raw, receipt, read = await method()
    if (not isinstance(receipt, Stamp) or not isinstance(read, Stamp)
            or read.mono_ns < receipt.mono_ns):
        raise IngressError("invalid_stamped_receive_pair")
    return raw, receipt, read


def ingress_metadata(receipt, read):
    return {"ingress_version": INGRESS_VERSION, "receipt_boundary": RECEIPT_BOUNDARY,
        "transport_callback_utc_ns": receipt.utc_ns, "transport_callback_mono_ns": receipt.mono_ns,
        "application_read_utc_ns": read.utc_ns, "application_read_mono_ns": read.mono_ns,
        "library_queue_delay_ns": read.mono_ns-receipt.mono_ns,
        "library_queue_delay_meaning": "callback parse + remaining message fragments + library/application queueing; not isolated queue residence"}
