"""Alpaca transport pinned to the paper host. No live endpoint is accepted."""
import asyncio
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
import json
import os
import re
from uuid import UUID

import httpx
from websockets.asyncio.client import connect

from .decision import decimal
from .timebase import DEADLINE_NS, SECOND, Stamp, iso_utc

PAPER_BASE = "https://paper-api.alpaca.markets"
PAPER_STREAM = "wss://paper-api.alpaca.markets/stream"
TERMINAL = {"filled", "canceled", "expired", "rejected"}
ORDER_KEYS = {"symbol", "side", "qty", "type", "limit_price", "time_in_force", "extended_hours", "client_order_id"}
SYMBOL = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
CLIENT_ID = re.compile(r"^research-[a-f0-9]{32}$")


class PaperError(Exception):
    def __init__(self, code, *, http_status=None, ambiguous=False):
        super().__init__(code)
        self.code, self.http_status, self.ambiguous = code, http_status, ambiguous


@dataclass
class EntryPermit:
    client_order_id: str
    receipt: Stamp
    quote_available_mono_ns: int
    portfolio_available_mono_ns: int
    quote_source_utc_ns: int
    portfolio_observation_started_mono_ns: int
    expires_mono_ns: int
    mode: str = "OBSERVED"

    def check(self, now, killed):
        if self.mode != "OBSERVED":
            raise PaperError("research_input_cannot_submit_to_broker")
        self.check_timing(now, killed)

    def check_timing(self, now, killed):
        """Pure temporal checks, also used by the network-free simulation broker."""
        if killed():
            raise PaperError("manual_kill_switch")
        if not 0 <= now.mono_ns-self.receipt.mono_ns < DEADLINE_NS:
            raise PaperError("five_second_deadline_expired_before_dispatch")
        if now.mono_ns >= self.expires_mono_ns:
            raise PaperError("entry_expired_before_dispatch")
        if not all(0 <= now.mono_ns-ns <= SECOND for ns in (self.quote_available_mono_ns, self.portfolio_available_mono_ns)):
            raise PaperError("quote_or_portfolio_expired_before_dispatch")
        if not 0 <= now.mono_ns-self.portfolio_observation_started_mono_ns <= SECOND:
            raise PaperError("portfolio_observation_expired_before_dispatch")
        if not 0 <= now.utc_ns-self.quote_source_utc_ns <= SECOND:
            raise PaperError("quote_source_expired_before_dispatch")
        if abs((now.utc_ns-self.receipt.utc_ns)-(now.mono_ns-self.receipt.mono_ns)) > SECOND//10:
            raise PaperError("wall_clock_step_during_entry_window")


class PaperBroker:
    simulation = False

    def __init__(self, client, journal=None, *, allow_orders=False, clock=Stamp.now, killed=lambda: True, account_authorized=lambda: False):
        self.client, self.journal, self.allow_orders = client, journal, allow_orders
        self.clock, self.killed, self.requests = clock, killed, deque()
        self.account_authorized, self.stream_ready = account_authorized, False
        # A running instance cannot silently switch paper accounts when env bindings change.
        self._credentials = (os.environ.get("ALPACA_API_KEY"), os.environ.get("ALPACA_API_SECRET"))

    def emit(self, kind, data, stamp=None):
        if self.journal:
            self.journal.emit(kind, {"paper": True, "broker_host": "paper-api.alpaca.markets", **data}, stamp or self.clock())

    def headers(self):
        key, secret = self._credentials
        if not key or not secret:
            raise PaperError("missing_alpaca_bindings")
        return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}

    def mutation_check(self):
        if not self.allow_orders:
            raise PaperError("paper_orders_disabled")
        if self.account_authorized() is not True:
            raise PaperError("paper_account_identity_not_verified")

    def claim(self, *, priority=False):
        now = self.clock().mono_ns
        while self.requests and self.requests[0] <= now-60*SECOND:
            self.requests.popleft()
        # Reserve headroom for cancellation; never queue an entry behind rate limits.
        if len(self.requests) >= (160 if priority else 120):
            raise PaperError("paper_request_budget_exhausted")
        self.requests.append(now)

    async def read(self, path, *, params=None, missing_ok=False):
        if path not in ("/v2/account", "/v2/positions", "/v2/orders", "/v2/clock", "/v2/orders:by_client_order_id", "/v2/account/configurations"):
            raise PaperError("unrecognized_read_path")
        self.claim()
        response = await self.client.get(PAPER_BASE+path, params=params, headers=self.headers(), timeout=2, follow_redirects=False)
        received = self.clock()
        if response.status_code == 404 and missing_ok:
            self.emit("paper_read", {"path": path, "http_status": 404, "missing": True}, received)
            return None
        if response.status_code != 200:
            raise PaperError("paper_read_http_error", http_status=response.status_code)
        body = json.loads(response.text, parse_float=str)
        self.emit("paper_read", {"path": path, "http_status": 200, "body": body}, received)
        return body

    async def snapshot(self):
        started = self.clock()
        values = await asyncio.gather(self.read("/v2/account"), self.read("/v2/positions"),
                    self.read("/v2/orders", params={"status": "open", "limit": 500, "nested": "false"}),
                    self.read("/v2/account/configurations"), return_exceptions=True)
        errors = [v for v in values if isinstance(v, BaseException)]
        if errors:
            raise PaperError("paper_snapshot_incomplete") from None
        account, positions, orders, config = values
        if not isinstance(account, dict) or not isinstance(positions, list) or not isinstance(orders, list) or len(orders) >= 500:
            raise PaperError("paper_snapshot_schema_or_pagination_incomplete")
        return {"account": account, "positions": positions, "open_orders": orders, "config": config, "started": started, "received": self.clock()}

    async def ensure_long_only_config(self):
        """Apply only restrictive settings on the fixed paper account; never unpause it."""
        desired = {"no_shorting": True, "max_margin_multiplier": "1", "max_options_trading_level": 0,
                   "disable_overnight_trading": True}
        current = await self.read("/v2/account/configurations")
        if all(current.get(key) == value for key, value in desired.items()):
            return current
        if not self.allow_orders:
            raise PaperError("paper_long_only_configuration_required")
        self.mutation_check()
        self.claim()
        response = await self.client.patch(PAPER_BASE+"/v2/account/configurations", json=desired,
                                          headers=self.headers(), timeout=2, follow_redirects=False)
        self.emit("paper_configuration", {"requested": desired, "http_status": response.status_code})
        if response.status_code != 200:
            raise PaperError("paper_configuration_http_error", http_status=response.status_code)
        verified = await self.read("/v2/account/configurations")
        if not all(verified.get(key) == value for key, value in desired.items()):
            raise PaperError("paper_long_only_configuration_not_verified")
        return verified

    async def order(self, client_id):
        if not CLIENT_ID.fullmatch(client_id):
            raise PaperError("invalid_client_order_id")
        return await self.read("/v2/orders:by_client_order_id", params={"client_order_id": client_id}, missing_ok=True)

    @staticmethod
    def validate_payload(payload):
        if (not isinstance(payload, dict) or set(payload) != ORDER_KEYS or not SYMBOL.fullmatch(payload.get("symbol", ""))
                or payload.get("side") not in ("buy", "sell") or payload.get("type") != "limit"
                or payload.get("time_in_force") != "day" or type(payload.get("extended_hours")) is not bool
                or not CLIENT_ID.fullmatch(payload.get("client_order_id", ""))):
            raise PaperError("invalid_paper_limit_order")
        try:
            qty, price = decimal(payload["qty"]), decimal(payload["limit_price"])
            if qty <= 0 or qty != qty.to_integral_value() or price <= 0:
                raise ValueError
            if payload["side"] == "buy" and price*qty > Decimal("1000"):
                raise ValueError
        except (ValueError, ArithmeticError, TypeError):
            raise PaperError("invalid_order_quantity_price_or_cap") from None

    async def submit(self, payload, *, permit=None, exit_check=None, on_dispatch=None, reservation_check=None, on_abort=None):
        """Never retry POST. A timeout after dispatch remains ambiguous until reconciled."""
        self.mutation_check()
        payload = dict(payload)
        self.validate_payload(payload)
        if payload["side"] == "buy":
            if not isinstance(permit, EntryPermit) or permit.client_order_id != payload["client_order_id"]:
                raise PaperError("missing_matching_entry_permit")
            permit.check(self.clock(), self.killed)
        elif exit_check is None:
            raise PaperError("missing_long_position_exit_check")
        self.claim()
        sent = False
        async def trace(event, info):
            nonlocal sent
            if event not in ("http11.send_request_headers.started", "http2.send_request_headers.started"):
                return
            request = info.get("request")
            if request is not None and request.method not in (b"POST", "POST"):
                return  # Proxy CONNECT is not an order submission.
            if sent:
                raise PaperError("transport_attempted_duplicate_post", ambiguous=True)
            self.mutation_check()
            if reservation_check:
                reservation_check()
            killed = self.killed() if payload["side"] == "buy" else False
            if payload["side"] == "buy":
                permit.check(self.clock(), lambda: killed)
            else:
                exit_check(self.clock())
            stamp = self.clock()
            if on_dispatch:
                on_dispatch(stamp)
            try:
                # Include callback/audit/reservation work in the final hard ceiling.
                self.mutation_check()
                if reservation_check:
                    reservation_check()
                killed = self.killed() if payload["side"] == "buy" else False
                if payload["side"] == "buy":
                    permit.check(self.clock(), lambda: killed)
                else:
                    exit_check(self.clock())
                stamp = self.clock()
                self.emit("paper_order_dispatch", {"client_order_id": payload["client_order_id"], "payload": payload,
                    "dispatch_boundary": "httpcore before POST request headers, after pool/connection acquisition",
                    "elapsed_from_receipt_ns": stamp.mono_ns-permit.receipt.mono_ns if permit else None}, stamp)
                self.mutation_check()
                if reservation_check:
                    reservation_check()
                killed = self.killed() if payload["side"] == "buy" else False
                if payload["side"] == "buy":
                    permit.check(self.clock(), lambda: killed)
                else:
                    exit_check(self.clock())
            except PaperError as exc:
                self.emit("paper_order_dispatch_aborted", {"client_order_id": payload["client_order_id"], "reason": exc.code})
                if on_abort:
                    on_abort(exc, self.clock())
                raise
            sent = True
        try:
            response = await self.client.post(PAPER_BASE+"/v2/orders", json=payload, headers=self.headers(),
                timeout=httpx.Timeout(connect=2, pool=0.05, read=2, write=0.25), follow_redirects=False, extensions={"trace": trace})
        except PaperError:
            raise
        except BaseException as exc:
            self.emit("paper_order_request_failed", {"client_order_id": payload["client_order_id"], "error_class": type(exc).__name__, "dispatch_observed": sent})
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise PaperError("paper_submission_transport_error", ambiguous=sent) from None
        received = self.clock()
        self.emit("paper_order_ack", {"client_order_id": payload["client_order_id"], "http_status": response.status_code,
                                      "dispatch_observed": sent}, received)
        if not sent:
            raise PaperError("dispatch_trace_unavailable", ambiguous=True)
        if response.status_code not in (200, 201):
            raise PaperError("paper_order_http_error", http_status=response.status_code,
                             ambiguous=response.status_code not in (400, 401, 403))
        body = json.loads(response.text, parse_float=str)
        self.emit("paper_order_snapshot", {"source": "POST_response", "order": body}, received)
        return body

    async def cancel(self, order_id):
        self.mutation_check()
        try:
            canonical = str(UUID(order_id))
        except (ValueError, TypeError, AttributeError):
            raise PaperError("invalid_broker_order_id") from None
        self.claim(priority=True)
        response = await self.client.delete(PAPER_BASE+"/v2/orders/"+canonical, headers=self.headers(), timeout=2, follow_redirects=False)
        self.emit("paper_cancel_requested", {"order_id": canonical, "http_status": response.status_code})
        # 204 is acceptance of a cancel request, not proof that nothing filled.
        if response.status_code not in (204, 404, 422):
            raise PaperError("paper_cancel_http_error", http_status=response.status_code)
        return response.status_code

    async def trade_updates(self, on_update, on_gap):
        failures = 0
        while True:
            try:
                async with connect(PAPER_STREAM, open_timeout=10, close_timeout=2, ping_interval=20, ping_timeout=20, max_size=2**20) as ws:
                    headers = self.headers()
                    await ws.send(json.dumps({"action": "auth", "key": headers["APCA-API-KEY-ID"], "secret": headers["APCA-API-SECRET-KEY"]}))
                    ready = False
                    while True:
                        raw = await ws.recv() if ready else await asyncio.wait_for(ws.recv(), 10)
                        received = self.clock()
                        msg = json.loads(raw)
                        if msg.get("stream") == "authorization":
                            if msg.get("data", {}).get("status") != "authorized":
                                raise PaperError("paper_stream_authentication_failed")
                            await ws.send('{"action":"listen","data":{"streams":["trade_updates"]}}')
                        elif msg.get("stream") == "listening":
                            ready = "trade_updates" in msg.get("data", {}).get("streams", [])
                            if not ready:
                                raise PaperError("paper_trade_updates_not_subscribed")
                            failures = 0
                            self.stream_ready = True
                            self.emit("paper_stream", {"state": "ready"}, received)
                        elif msg.get("stream") == "trade_updates":
                            self.emit("paper_trade_update", {"raw": msg}, received)
                            await on_update(msg["data"], received)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.stream_ready = False
                self.emit("paper_stream", {"state": "disconnected", "error_class": type(exc).__name__})
                try:
                    await on_gap()
                except asyncio.CancelledError:
                    raise
                except Exception as gap_error:
                    # The coordinator sets its in-memory recovery pause first. A
                    # transient persistence error must not kill reconnection forever.
                    self.emit("paper_stream", {"state": "gap_callback_failed", "error_class": type(gap_error).__name__})
            finally:
                self.stream_ready = False
            failures += 1
            await asyncio.sleep(min(60, 2**min(failures, 6)))
