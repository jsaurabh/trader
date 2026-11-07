"""Deadline-bound current quotes with explicit size, condition and status evidence.

The adapter does not place orders, activate subscriptions, infer a halt baseline
from silence, or fall back from denied SIP access to IEX/delayed data.
"""
import asyncio
from collections import deque
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
import re
from zoneinfo import ZoneInfo

import httpx

from .timebase import SECOND, MarketCalendar, Stamp, iso_utc, parse_rfc3339

VERSION = "alpaca-market-inputs-v2"
DATA_BASE = "https://data.alpaca.markets"
SYMBOL = re.compile(r"^[A-Z][A-Z0-9./-]{0,14}$")
SIZE_START_DATE = date(2025, 11, 3)
SIZE_SOURCE = "https://docs.alpaca.markets/us/reference/stocklatestquotesingle-1"
SIZE_CHANGE_SOURCE = "https://docs.alpaca.markets/us/v1.1/changelog/marketdata-bid-and-ask-size-display-change"
STATUS_SOURCE = "https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data"
UTP_WARNING_SOURCE = "https://utpplan.com/DOC/UtpBinaryOutputSpec_Fractional.pdf"
BROKER_SESSION_SOURCE = "https://docs.alpaca.markets/us/docs/orders-at-alpaca"
CONDITION_SOURCE = "https://docs.alpaca.markets/us/reference/stockmetaconditions-1"
REGULAR_NAMES = {"A": "regular market maker open", "B": "regular market maker open", "C": "regular two sided open"}


class MarketDataError(Exception):
    def __init__(self, codes):
        self.codes = tuple(codes) if not isinstance(codes, str) else (codes,)
        super().__init__(";".join(self.codes))


def exact_decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("exact numeric value required")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("finite value required")
    return result


def verified_shares(quote, feed, *, observation_method="rest_latest_snapshot"):
    """Explicit REST contract or dated CTA/UTP stream policy; never a ×100 guess."""
    websocket = observation_method == "websocket_stream"
    result = {"bid_size_shares": None, "ask_size_shares": None, "size_unit_verified": False,
              "size_unit": "unknown", "size_provenance": {"source": SIZE_SOURCE, "change_source": SIZE_CHANGE_SOURCE,
              "effective_date": SIZE_START_DATE.isoformat(), "method": observation_method, "conversion_multiplier": None}}
    if websocket:
        result["size_provenance"].update(source=SIZE_CHANGE_SOURCE,
            policy="alpaca-cta-utp-post-2025-11-03-v1",
            scope="SIP CTA/UTP tape A/B/C WebSocket quotes only",
            source_excerpt="Alpaca will display the CTA/UTP (US stock) quote sizes in shares, instead of round lots, starting November 3, 2025.",
            documentation_conflict="Dated transport-general provider change supersedes the older WebSocket round-lot schema label")
    try:
        source_ns, _ = parse_rfc3339(quote["t"])
        source_date = datetime.fromtimestamp(source_ns//SECOND, ZoneInfo("America/New_York")).date()
        if feed not in ("sip", "iex") or quote["z"] not in ("A", "B", "C") or source_date < SIZE_START_DATE:
            return result
        if observation_method not in ("rest_latest_snapshot", "websocket_stream") or (websocket and feed != "sip"):
            return result
        if any(type(quote[k]) is not int or not 0 <= quote[k] <= 2**32-1 for k in ("bs", "as")):
            return result
        result.update(bid_size_shares=str(quote["bs"]), ask_size_shares=str(quote["as"]), size_unit_verified=True, size_unit="shares")
        result["size_provenance"]["conversion_multiplier"] = 1
    except (KeyError, TypeError, ValueError, OverflowError):
        pass
    return result


class StatusBook:
    """Trading Action state plus current continuity, not a periodically changing status.

    Initial state is unknown. A new connection discards previous state because a
    disconnect may have lost a halt. R quotes alone do not establish trading state.
    """
    def __init__(self, *, max_age_ns=SECOND):
        self.max_age_ns = max_age_ns
        self.feed, self.epoch = None, 0
        self.status_symbols, self.quote_symbols = set(), set()
        self.status_since = {}
        self.states, self.quotes = {}, {}
        self.connected_at = None

    def connected(self, feed, received):
        self.feed, self.epoch, self.connected_at = feed, self.epoch+1, received
        self.status_symbols, self.quote_symbols, self.states, self.quotes = set(), set(), {}, {}
        self.status_since = {}

    def disconnected(self, received=None):
        self.feed, self.connected_at = None, None
        self.status_symbols, self.quote_symbols, self.states, self.quotes = set(), set(), {}, {}
        self.status_since = {}

    def subscribed(self, message, received):
        if self.connected_at is None:
            return
        statuses, quotes = message.get("statuses"), message.get("quotes")
        if (not isinstance(statuses, list) or not isinstance(quotes, list)
                or any(not isinstance(s, str) for s in statuses+quotes)):
            self.status_symbols, self.quote_symbols, self.states, self.quotes = set(), set(), {}, {}
            self.status_since = {}
            return
        previous_since = self.status_since
        self.status_since = {s: previous_since.get(s, previous_since.get("*", received)) for s in statuses}
        self.status_symbols, self.quote_symbols = set(statuses), set(quotes)
        for symbol in list(self.states):
            if symbol not in self.status_symbols and "*" not in self.status_symbols:
                self.states.pop(symbol, None)
        for symbol in list(self.quotes):
            if symbol not in self.quote_symbols and "*" not in self.quote_symbols:
                self.quotes.pop(symbol, None)

    def ingest(self, message, received, feed=None):
        if not isinstance(message, dict) or self.connected_at is None or (feed is not None and feed != self.feed):
            return
        if message.get("T") == "subscription":
            self.subscribed(message, received)
            return
        symbol = message.get("S")
        if not isinstance(symbol, str):
            return
        try:
            source_ns, _ = parse_rfc3339(message["t"])
        except (KeyError, TypeError, ValueError):
            return
        if message.get("T") == "q":
            if symbol in self.quote_symbols or "*" in self.quote_symbols:
                previous = self.quotes.get(symbol)
                if previous is None or source_ns >= previous["source_ns"]:
                    self.quotes[symbol] = {"source_ns": source_ns, "received": received, "epoch": self.epoch,
                                           "raw": dict(message)}
            return
        if message.get("T") != "s" or (symbol not in self.status_symbols and "*" not in self.status_symbols):
            return
        tape, code = message.get("z"), message.get("sc")
        if not isinstance(tape, str) or not isinstance(code, str):
            return
        state = ({"2": "halted", "3": "tradable", "F": "restricted"}.get(code) if tape in ("A", "B") else
                 {"H": "halted", "P": "halted", "Q": "quotation_only", "T": "tradable"}.get(code) if tape in ("C", "O") else None)
        previous = self.states.get(symbol)
        # A replay from before this connection is not documented as an initial
        # current-state snapshot. Do not turn it into a comprehensive baseline.
        subscribed_at = self.status_since.get(symbol, self.status_since.get("*", self.connected_at))
        if source_ns < max(self.connected_at.utc_ns, subscribed_at.utc_ns):
            return
        if previous and source_ns < previous["source_ns"]:
            return
        if state is not None:
            conflict = bool(previous and source_ns == previous["source_ns"] and state != previous["state"])
            if conflict:
                state = "unknown"
            self.states[symbol] = {"state": state, "source_ns": source_ns, "received": received,
                                   "raw": dict(message), "epoch": self.epoch, "conflict": conflict}

    def snapshot(self, ticker, now, feed):
        result = {"market_status": "unknown", "market_status_available_utc": None,
                  "halt_state": "unknown", "status_reason": "initial_trading_action_state_unknown",
                  "status_provenance": {"source": STATUS_SOURCE, "quote_alone_warning": UTP_WARNING_SOURCE,
                                        "connection_epoch": self.epoch, "feed": self.feed}}
        if self.feed != feed or self.connected_at is None:
            result["status_reason"] = "status_stream_not_connected_for_feed"
            return result
        if ticker not in self.status_symbols and "*" not in self.status_symbols:
            result["status_reason"] = "status_channel_not_subscribed"
            return result
        known = self.states.get(ticker)
        if not known:
            return result
        result["status_provenance"].update(last_transition_source_utc=iso_utc(known["source_ns"]),
            last_transition_received_utc=iso_utc(known["received"].utc_ns), last_transition_code=known["raw"].get("sc"),
            last_transition_tape=known["raw"].get("z"), transition_age_ns=now.utc_ns-known["source_ns"])
        result.update(market_status=known["state"], halt_state="halted" if known["state"] == "halted" else known["state"],
                      market_status_available_utc=iso_utc(known["received"].utc_ns))
        if known["state"] != "tradable":
            result["status_reason"] = ("conflicting_trading_actions_at_same_source_time" if known.get("conflict") else
                                       "last_observed_trading_action_"+known["state"])
            return result
        confirmations = [known]
        quote = self.quotes.get(ticker)
        if quote and quote["source_ns"] >= known["source_ns"]:
            confirmations.append(quote)
        fresh = [p for p in confirmations if 0 <= now.utc_ns-p["source_ns"] <= self.max_age_ns
                 and 0 <= now.mono_ns-p["received"].mono_ns <= self.max_age_ns]
        if not fresh:
            result.update(market_status="unknown", status_reason="status_stream_continuity_not_fresh")
            return result
        confirmation = max(fresh, key=lambda p: p["received"].mono_ns)
        result.update(market_status="tradable", halt_state="not_halted_as_last_trading_action",
                      market_status_available_utc=iso_utc(confirmation["received"].utc_ns), status_reason=None)
        result["status_provenance"].update(confirmation="Trading Action baseline plus uninterrupted subscribed stream through a fresh same-symbol quote/status",
            continuity_source_utc=iso_utc(confirmation["source_ns"]), continuity_received_utc=iso_utc(confirmation["received"].utc_ns))
        return result


class MarketInputs:
    def __init__(self, client, journal=None, *, calendar=None, feed="sip", clock=Stamp.now, status=None, requests_per_minute=60):
        if feed not in ("sip", "iex"):
            raise ValueError("Explicit current SIP/IEX only; no delayed or overnight fallback")
        self.client, self.journal, self.feed, self.clock = client, journal, feed, clock
        self.calendar = calendar or MarketCalendar(clock().utc_ns)
        self.status = status or StatusBook()
        self.conditions, self.metadata_at = {}, {}
        self.request_starts, self.requests_per_minute, self.cooldown_ns = deque(), requests_per_minute, 0

    def emit(self, kind, data, stamp=None):
        if self.journal:
            self.journal.emit(kind, {"adapter_version": VERSION, **data}, stamp or self.clock())

    def validate_dispatch(self, ticker, captured_quote, now, *, side="buy", quantity=None):
        """No I/O: known halts/gaps/new adverse quotes supersede a fresh snapshot."""
        reasons = []
        current = self.status.snapshot(ticker, now, self.feed)
        if current.get("market_status") != "tradable":
            reasons.append("current_trading_action_not_tradable")
        original_epoch = captured_quote.get("status_provenance", {}).get("connection_epoch")
        if original_epoch is None or original_epoch != current["status_provenance"]["connection_epoch"]:
            reasons.append("market_connection_changed_since_quote")
        session, supported, _ = self.broker_session(now)
        if not supported or session != captured_quote.get("calendar_session"):
            reasons.append("broker_session_changed_or_unsupported_before_dispatch")
        latest = self.status.quotes.get(ticker)
        try:
            captured_source = parse_rfc3339(captured_quote["source_utc"])[0]
            if latest and latest["source_ns"] >= captured_source and "raw" in latest:
                quote = self.normalize(ticker, {"symbol": ticker, "quote": latest["raw"]}, latest["received"],
                                       observation_method="websocket_stream")
                reasons.extend("current_quote:"+reason for reason in quote["reason_codes"])
                if side == "buy" and exact_decimal(quote["spread_pct_of_mid"]) > Decimal("0.25"):
                    reasons.append("current_quote_spread_above_entry_limit")
                if side == "buy" and exact_decimal(quote["ask"]) < Decimal("5"):
                    reasons.append("current_ask_below_minimum_entry_price")
                if quantity is not None:
                    size = quote["ask_size_shares" if side == "buy" else "bid_size_shares"]
                    if exact_decimal(size) < exact_decimal(quantity):
                        reasons.append("current_displayed_size_below_order_quantity")
        except (KeyError, TypeError, ValueError, InvalidOperation):
            reasons.append("current_quote_not_validated_before_dispatch")
        return list(dict.fromkeys(reasons))

    @staticmethod
    def headers():
        key, secret = os.environ.get("ALPACA_API_KEY"), os.environ.get("ALPACA_API_SECRET")
        if not key or not secret:
            raise MarketDataError("missing_alpaca_bindings")
        return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}

    def claim(self):
        now = self.clock().mono_ns
        if now < self.cooldown_ns:
            raise MarketDataError("market_rate_limit_cooldown")
        while self.request_starts and self.request_starts[0] <= now-60*SECOND:
            self.request_starts.popleft()
        if len(self.request_starts) >= self.requests_per_minute:
            raise MarketDataError("market_request_budget_exhausted")
        self.request_starts.append(now)

    def timeout(self, deadline):
        remaining = min(2.0, (deadline-self.clock().mono_ns)/SECOND) if deadline is not None else 2.0
        if remaining <= 0:
            raise MarketDataError("five_second_deadline_expired_before_market_request")
        return remaining

    async def prewarm(self, *, deadline_mono_ns=None):
        async def get(tape):
            self.claim()
            seconds = self.timeout(deadline_mono_ns)
            async with asyncio.timeout(seconds):
                response = await self.client.get(DATA_BASE+"/v2/stocks/meta/conditions/quote", params={"tape": tape},
                    headers=self.headers(), timeout=seconds, follow_redirects=False)
            available = self.clock()
            if response.status_code != 200:
                return {"tape": tape, "status": "http_error", "http_status": response.status_code}
            if len(response.content) > 65536:
                return {"tape": tape, "status": "metadata_size_limit"}
            mapping = json.loads(response.text)
            if not isinstance(mapping, dict) or not all(isinstance(k, str) and isinstance(v, str) for k,v in mapping.items()):
                return {"tape": tape, "status": "invalid_metadata"}
            self.conditions[tape], self.metadata_at[tape] = mapping, available
            result = {"tape": tape, "status": "prepared", "mapping": mapping, "source": CONDITION_SOURCE,
                      "available_utc": iso_utc(available.utc_ns), "response_sha256": hashlib.sha256(response.content).hexdigest()}
            self.emit("market_condition_metadata", result, available)
            return result
        values = await asyncio.gather(*(get(t) for t in "ABC"), return_exceptions=True)
        return [v if not isinstance(v, BaseException) else {"status": "error", "error_class": type(v).__name__} for v in values]

    def broker_session(self, now):
        session = self.calendar.session(now.utc_ns)
        reason = None
        supported = session in ("pre_market", "regular", "after_hours")
        if session == "after_hours":
            local = datetime.fromtimestamp(now.utc_ns//SECOND, ZoneInfo("America/New_York"))
            closing = self.calendar.calendar.session_close(local.date().isoformat())
            if closing.to_pydatetime().astimezone(ZoneInfo("America/New_York")).hour < 16:
                supported, reason = False, "early_close_extended_broker_window_not_verified"
        if not supported and reason is None:
            reason = "outside_initial_broker_session_coverage"
        return session, supported, reason

    def normalize(self, ticker, body, available, *, observation_method="rest_latest_snapshot"):
        now = self.clock()
        reasons = []
        quote = body.get("quote") if isinstance(body, dict) else None
        result = {"ticker": ticker, "bid": None, "ask": None, "bid_size_shares": None, "ask_size_shares": None,
            "source_utc": None, "available_utc": iso_utc(available.utc_ns), "available_mono_ns": available.mono_ns,
            "coverage_verified": False, "conditions_eligible": False, "size_unit_verified": False,
            "market_status": "unknown", "market_status_available_utc": None, "broker_session_supported": False,
            "feed": self.feed, "quote_scope": "consolidated_SIP" if self.feed == "sip" else "single_venue_IEX_not_NBBO",
            "observation_method": observation_method,
            "adapter_version": VERSION, "status": "invalid_quote", "eligible_for_execution_checks": False, "reason_codes": reasons}
        session, supported, session_reason = self.broker_session(now)
        result.update(calendar_session=session, broker_session_supported=supported, broker_session_source=BROKER_SESSION_SOURCE)
        if session_reason:
            reasons.append(session_reason)
        if not isinstance(quote, dict) or body.get("symbol") != ticker:
            reasons.append("missing_quote_or_symbol_mismatch")
            return result
        result["raw_quote"] = quote
        if body.get("currency", "USD") != "USD":
            reasons.append("unexpected_quote_currency")
        if any(not isinstance(quote.get(k), str) or len(quote[k]) != 1 or not quote[k].strip() for k in ("bx", "ax")):
            reasons.append("quote_exchange_fields_missing_or_invalid")
        try:
            source_ns, precision = parse_rfc3339(quote["t"])
            result.update(source_utc=quote["t"], source_precision_ns=precision, source_age_ns=now.utc_ns-source_ns,
                          local_age_ns=now.mono_ns-available.mono_ns)
            if not 0 <= now.utc_ns-source_ns <= SECOND or not 0 <= now.mono_ns-available.mono_ns <= SECOND:
                reasons.append("quote_not_fresh")
            bid, ask = exact_decimal(quote["bp"]), exact_decimal(quote["ap"])
            result.update(bid=str(bid), ask=str(ask))
            if bid <= 0 or ask <= 0 or ask < bid:
                reasons.append("invalid_crossed_or_empty_quote")
            else:
                result["spread_pct_of_mid"] = str(100*(ask-bid)/((ask+bid)/2))
        except (KeyError, ValueError, TypeError, InvalidOperation, OverflowError):
            reasons.append("invalid_quote_timestamp_or_prices")
        result.update(verified_shares(quote, self.feed, observation_method=observation_method))
        if not result["size_unit_verified"]:
            reasons.append("quote_size_semantics_not_verified")
        elif any(exact_decimal(result[k]) <= 0 for k in ("bid_size_shares", "ask_size_shares")):
            reasons.append("empty_quote_size")
        tape, flags = quote.get("z"), quote.get("c")
        metadata = self.conditions.get(tape, {})
        metadata_stamp = self.metadata_at.get(tape)
        prepared = (metadata_stamp is not None and 0 <= now.utc_ns-metadata_stamp.utc_ns <= 86400*SECOND
                    and metadata.get("R", "").strip().casefold() == REGULAR_NAMES.get(tape))
        if not prepared:
            reasons.append("quote_condition_metadata_not_prepared_or_changed")
        open_condition = isinstance(flags, list) and len(flags) in (1,2) and all(flag == "R" for flag in flags)
        result.update(conditions_eligible=bool(prepared and open_condition), conditions=flags, tape=tape,
                      open_quote_asof_timestamp=bool(prepared and open_condition),
                      condition_provenance={"source": CONDITION_SOURCE, "mapping": metadata,
                          "metadata_available_utc": iso_utc(metadata_stamp.utc_ns) if metadata_stamp else None,
                          "meaning": "R confirms an open quote as of its source time; not a complete halt-state baseline"})
        if not open_condition:
            reasons.append("quote_conditions_not_supported_regular_two_sided_open")
        # An explicit SIP success establishes present account access, not future uptime.
        result["feed_access_verified"] = self.feed == "sip" and tape in ("A", "B", "C")
        result["coverage_verified"] = result["feed_access_verified"] and body.get("currency", "USD") == "USD" and "quote_exchange_fields_missing_or_invalid" not in reasons
        if not result["coverage_verified"]:
            reasons.append("consolidated_current_coverage_not_verified")
        status = self.status.snapshot(ticker, now, self.feed)
        result.update(status)
        if status["status_reason"]:
            reasons.append(status["status_reason"])
        if status["market_status"] == "unknown" and result["open_quote_asof_timestamp"]:
            result["market_status"] = "open_quote_asof_timestamp"
        if result["market_status"] != "tradable":
            reasons.append("trading_action_state_not_verified_tradable")
        result["status"] = "quote_observed" if not reasons else "quote_observed_ineligible"
        result["reason_codes"] = list(dict.fromkeys(reasons))
        result["eligible_for_execution_checks"] = not reasons
        return result

    async def __call__(self, ticker, *, deadline_mono_ns=None):
        now = self.clock()
        base = {"ticker": ticker, "status": "unavailable", "feed": self.feed, "coverage_verified": False,
                "size_unit_verified": False, "conditions_eligible": False, "broker_session_supported": False,
                "market_status": "unknown", "market_status_available_utc": None, "source_utc": None,
                "available_utc": iso_utc(now.utc_ns), "available_mono_ns": now.mono_ns,
                "bid": None, "ask": None, "bid_size_shares": None, "ask_size_shares": None,
                "eligible_for_execution_checks": False, "reason_codes": []}
        if not isinstance(ticker, str) or not SYMBOL.fullmatch(ticker):
            return base | {"reason_codes": ["invalid_market_symbol"]}
        try:
            self.claim()
            seconds = self.timeout(deadline_mono_ns)
            started = self.clock()
            async with asyncio.timeout(seconds):
                response = await self.client.get(DATA_BASE+"/v2/stocks/"+ticker.replace("/", "%2F")+"/quotes/latest",
                    params={"feed": self.feed, "currency": "USD"}, headers=self.headers(),
                    timeout=httpx.Timeout(connect=min(seconds, .5), pool=min(seconds, .05), read=seconds, write=min(seconds, .25)), follow_redirects=False)
            downloaded = self.clock()
            if response.status_code != 200:
                result = base | {"status": "access_denied" if response.status_code in (401,403) else "http_error",
                    "http_status": response.status_code, "available_utc": iso_utc(downloaded.utc_ns),
                    "available_mono_ns": downloaded.mono_ns, "reason_codes": ["current_"+self.feed+"_http_"+str(response.status_code)],
                    "request_started_utc": iso_utc(started.utc_ns), "download_completed_utc": iso_utc(downloaded.utc_ns),
                    "request_duration_ns": downloaded.mono_ns-started.mono_ns}
                if response.status_code == 429:
                    retry = response.headers.get("retry-after", "60")
                    try:
                        cooldown = float(retry)
                        if not 0 <= cooldown < float("inf"):
                            cooldown = 60
                    except ValueError:
                        try:
                            cooldown = max(0, parsedate_to_datetime(retry).timestamp()-downloaded.utc_ns/SECOND)
                        except (ValueError, TypeError, OverflowError):
                            cooldown = 60
                    self.cooldown_ns = downloaded.mono_ns+int(cooldown*SECOND)
                self.emit("market_input_result", result, downloaded)
                return result
            if len(response.content) > 65536:
                raise MarketDataError("market_quote_response_size_limit")
            body = json.loads(response.text, parse_float=str)
            available = self.clock()
            result = self.normalize(ticker, body, available)
            result.update(http_status=200, request_started_utc=iso_utc(started.utc_ns),
                          download_completed_utc=iso_utc(downloaded.utc_ns), request_duration_ns=downloaded.mono_ns-started.mono_ns)
            completed = self.clock()
            if deadline_mono_ns is not None and completed.mono_ns >= deadline_mono_ns:
                result.update(status="deadline_expired", coverage_verified=False, eligible_for_execution_checks=False)
                result["reason_codes"].append("five_second_deadline_expired_during_market_request")
            self.emit("market_input_result", result, completed)
            return result
        except asyncio.CancelledError:
            raise
        except MarketDataError as exc:
            result = base | {"reason_codes": list(exc.codes)}
        except (TimeoutError, httpx.TimeoutException):
            result = base | {"status": "timed_out", "reason_codes": ["market_request_timed_out"]}
        except Exception as exc:
            result = base | {"reason_codes": ["market_request_or_schema_error"], "error_class": type(exc).__name__}
        completed = self.clock()
        result.update(available_utc=iso_utc(completed.utc_ns), available_mono_ns=completed.mono_ns)
        self.emit("market_input_result", result, completed)
        return result


def validate_for_exit(quote, now, *, max_age_ns=SECOND):
    """An exit still needs an executable-side quote and known state; never invent a bid."""
    reasons = []
    try:
        if not (quote["coverage_verified"] is True and quote["conditions_eligible"] is True and quote["size_unit_verified"] is True):
            reasons.append("exit_quote_coverage_conditions_or_sizes_unverified")
        source_ns = parse_rfc3339(quote["source_utc"])[0]
        if not (0 <= now.utc_ns-source_ns <= max_age_ns and 0 <= now.mono_ns-quote["available_mono_ns"] <= max_age_ns):
            reasons.append("exit_quote_not_fresh")
        if (not 0 < exact_decimal(quote["bid"]) <= exact_decimal(quote["ask"])
                or any(exact_decimal(quote[k]) <= 0 for k in ("bid_size_shares", "ask_size_shares"))):
            reasons.append("exit_bid_or_size_invalid")
        status_ns = parse_rfc3339(quote["market_status_available_utc"])[0]
        if quote["market_status"] != "tradable" or not 0 <= now.utc_ns-status_ns <= max_age_ns:
            reasons.append("exit_trading_action_state_not_verified")
        if quote["broker_session_supported"] is not True:
            reasons.append("exit_broker_session_not_supported")
    except (KeyError, TypeError, ValueError, InvalidOperation):
        reasons.append("missing_required_exit_quote")
    return list(dict.fromkeys(reasons))
