"""Deadline-bound admission and recoverable paper order lifecycle; no live transport."""
import asyncio
from copy import deepcopy
from decimal import Decimal, ROUND_FLOOR
import hashlib
from pathlib import Path

from .decision import evaluate_paper_intent, decimal
from .locking import CaptureLock
from .paper_broker import EntryPermit, PaperError, TERMINAL
from .portfolio import LOCAL_TERMINAL
from .timebase import DEADLINE_NS, SECOND, Stamp, iso_utc, parse_rfc3339


class PaperExecution:
    def __init__(self, ledger, broker, journal, calendar, market, *, kill_path, clock=Stamp.now):
        if ledger.mode == "SYNTHETIC" and not broker.simulation:
            raise ValueError("Synthetic state cannot use a real broker transport")
        self.ledger, self.broker, self.journal, self.calendar, self.market = ledger, broker, journal, calendar, market
        self.kill_path, self.clock = Path(kill_path), clock
        self.entry_lock, self.refresh_lock = asyncio.Lock(), asyncio.Lock()
        self.flags = {"reconciled": False, "review_required": False, "daily_pause": False, "kill_switch": False}
        self.last_snapshot, self.portfolio, self.account_lock = None, None, None
        self.identity_ok = False
        self.recovery_queue, self.recovery_needed = [], True
        self.gap_generation = 0
        self.prebound_updates = []
        self.cancel_after, self.exit_after = {}, {}
        self.broker.killed = self.killed
        self.broker.account_authorized = lambda: self.identity_ok

    def killed(self):
        return (self.kill_path.exists() or not self.identity_ok or not self.broker.stream_ready or not self.flags["reconciled"]
                or self.recovery_needed or bool(self.recovery_queue)
                or any(self.flags[k] for k in ("review_required", "daily_pause", "kill_switch")))

    def emit(self, kind, data, stamp=None):
        self.journal.emit(kind, {"paper": True, "mode": self.ledger.mode, **data}, stamp or self.clock())

    async def update(self, data, received=None):
        if not self.identity_ok:
            if len(self.prebound_updates) >= 1000:
                await asyncio.to_thread(self.ledger.pause, "prebinding_trade_update_overflow", review=True)
            else:
                self.prebound_updates.append(data)
            self.emit("paper_update_quarantined", {"reason": "account_identity_not_yet_verified"}, received)
            return
        await asyncio.to_thread(self.ledger.trade_update, data)
        self.flags = await asyncio.to_thread(self.ledger.flags)

    async def gap(self):
        self.gap_generation += 1
        self.recovery_queue = []
        self.recovery_needed = True
        await asyncio.to_thread(self.ledger.pause, "paper_trade_updates_disconnected")
        self.flags = await asyncio.to_thread(self.ledger.flags)

    async def refresh(self):
        async with self.refresh_lock:
            recovery_epoch = self.gap_generation
            snapshot = await self.broker.snapshot()
            self.identity_ok = await asyncio.to_thread(self.ledger.verify_account, snapshot)
            if not self.identity_ok:
                self.last_snapshot, self.portfolio = snapshot, None
                self.flags = await asyncio.to_thread(self.ledger.flags)
                self.emit("paper_identity_quarantined", {"reason": "account_does_not_match_ledger"})
                return None
            if self.account_lock is None and not self.broker.simulation:
                identity = hashlib.sha256(snapshot["account"]["id"].encode()).hexdigest()
                lock_path = Path(__file__).resolve().parent.parent/"data"/"paper-account-locks"/(identity+".lock")
                self.account_lock = CaptureLock(lock_path)
            pending_updates, self.prebound_updates = self.prebound_updates, []
            for update in pending_updates:
                await asyncio.to_thread(self.ledger.trade_update, update)
            rows = await asyncio.to_thread(self.ledger.orders)
            unresolved = [r for r in rows if r["state"] not in LOCAL_TERMINAL]
            reads = await asyncio.gather(*(self.broker.order(r["client_id"]) for r in unresolved), return_exceptions=True)
            for row, value in zip(unresolved, reads):
                if isinstance(value, BaseException):
                    raise PaperError("owned_order_reconciliation_failed") from None
                if value is not None:
                    await asyncio.to_thread(self.ledger.apply_order, value, "client_id_reconciliation")
                elif row["state"] == "reserved" and row["owner_run"] != self.journal.run_id:
                    # The application records 'submitting' durably before any POST.
                    await asyncio.to_thread(self.ledger.mark, row["client_id"], "not_submitted")
                elif row["owner_run"] != self.journal.run_id or row["state"] not in ("reserved", "submitting"):
                    await asyncio.to_thread(self.ledger.mark, row["client_id"], "unknown")
            portfolio = await asyncio.to_thread(self.ledger.reconcile, snapshot)
            mismatch = portfolio is None or any(reason in (portfolio or {}).get("reconciliation_reasons", [])
                for reason in ("broker_and_ledger_positions_differ", "cash_or_equity_not_reconciled"))
            if mismatch or self.recovery_needed or self.recovery_queue:
                if not self.recovery_queue:
                    self.recovery_queue = [r["client_id"] for r in reversed(rows) if r["state"] in TERMINAL]
                self.recovery_needed = True
                # Bounded sweep repairs missed late fills on terminal/canceled orders.
                # Entries remain paused on mismatch while older batches are inspected.
                batch, self.recovery_queue = self.recovery_queue[:2], self.recovery_queue[2:]
                recovered = await asyncio.gather(*(self.broker.order(client_id) for client_id in batch), return_exceptions=True)
                for value in recovered:
                    if isinstance(value, BaseException):
                        raise PaperError("terminal_order_recovery_failed") from None
                    if value:
                        await asyncio.to_thread(self.ledger.apply_order, value, "terminal_order_recovery")
                if batch:
                    snapshot = await self.broker.snapshot()
                    portfolio = await asyncio.to_thread(self.ledger.reconcile, snapshot)
                if not self.recovery_queue and self.broker.stream_ready and recovery_epoch == self.gap_generation:
                    self.recovery_needed = False
            rows = await asyncio.to_thread(self.ledger.orders)
            pending_old = [r for r in rows if r["state"] not in LOCAL_TERMINAL and r["owner_run"] != self.journal.run_id]
            unknown = [r for r in rows if r["state"] == "unknown"]
            if unknown or pending_old:
                await asyncio.to_thread(self.ledger.pause, "unresolved_or_recovered_order", review=any(self.clock().utc_ns-r["created_utc_ns"] > 60*SECOND for r in unknown))
                if portfolio:
                    portfolio["reconciled"] = False
            if self.recovery_needed or self.recovery_queue:
                await asyncio.to_thread(self.ledger.pause, "terminal_order_recovery_in_progress")
                if portfolio:
                    portfolio["reconciled"] = False
            self.last_snapshot, self.portfolio = snapshot, portfolio
            self.flags = await asyncio.to_thread(self.ledger.flags)
            identity = hashlib.sha256(snapshot["account"]["id"].encode()).hexdigest()
            self.identity_ok = self.flags["account_hash"] == identity
            if portfolio:
                portfolio["kill_switch"] = portfolio["kill_switch"] or self.kill_path.exists()
            self.emit("paper_portfolio", {"portfolio": portfolio, "flags": self.flags,
                       "snapshot_started_utc": iso_utc(snapshot["started"].utc_ns),
                       "snapshot_ready_utc": iso_utc(snapshot["received"].utc_ns)})
            return portfolio

    async def initialize(self):
        await self.refresh()
        if not self.identity_ok:
            raise PaperError("paper_account_identity_not_verified")
        await self.broker.ensure_long_only_config()
        await self.refresh()
        # Do not leave pre-restart orders working while rebuilding strategy state.
        await self.maintain_orders(recovering=True)

    async def enter(self, analysis, receipt, published_ns, prepared, on_decision, *, admission_guard=None):
        """on_decision is synchronous, and marks the event decided before dispatch."""
        if self.broker.simulation and self.ledger.mode == "SYNTHETIC" and analysis.get("mode") == "SYNTHETIC":
            analysis = dict(analysis, reasons=[r for r in analysis["reasons"] if r != "research_input_only:SYNTHETIC"])
        result = {"event_id": analysis["event_id"], "ticker": analysis["ticker"], "decision": "skip",
                  "orders_enabled": self.broker.allow_orders, "paper": True, "mode": analysis["mode"],
                  "receipt_mono_ns": receipt.mono_ns, "strategy_version": analysis["strategy_version"], "reasons": list(analysis["reasons"])}
        payload, dispatched = None, False
        if result["reasons"]:
            on_decision(result, self.clock())
            return result
        def check_announcement_clock():
            if admission_guard is None:
                if not self.broker.simulation:
                    raise PaperError("announcement_clock_guard_missing")
            elif admission_guard() is not True:
                raise PaperError("announcement_clock_changed_before_dispatch")
        def check_reservation():
            check_announcement_clock()
            if self.clock().utc_ns >= parse_rfc3339(intent["scheduled_exit_utc"])[0]:
                raise PaperError("scheduled_exit_passed_before_entry_dispatch")
            self.check_current_market(analysis["ticker"], quote, self.clock(), side="buy", quantity=payload["qty"])
            self.ledger.check_dispatch(payload["client_order_id"], self.journal.run_id, "entry")
        try:
            check_announcement_clock()
            if analysis["mode"] != self.ledger.mode or (analysis["mode"] != "OBSERVED" and not self.broker.simulation):
                raise PaperError("analysis_and_paper_state_mode_mismatch")
            if not analysis.get("facts_pass_source_validation"):
                raise PaperError("source_validation_not_passed")
            required = ("company", "liquidity", "reference", "eps", "revenue", "prior_guidance")
            missing = [key for key in required if not isinstance((prepared or {}).get(key), dict)]
            if missing:
                result["reasons"].extend("missing_prepared_"+key for key in missing)
                on_decision(result, self.clock())
                return result
            issuer = analysis.get("issuer_identity", {})
            if (issuer.get("verified") is not True or issuer.get("analysis_source_sha256") != analysis.get("source_sha256")
                    or not analysis.get("source_sha256") or issuer.get("canonical_company_id") != prepared["company"].get("canonical_company_id")):
                raise PaperError("primary_issuer_identity_not_verified")
            remaining = (receipt.mono_ns+DEADLINE_NS-self.clock().mono_ns)/SECOND
            if remaining <= 0:
                raise PaperError("five_second_deadline_expired")
            async with asyncio.timeout(remaining):
                async with self.entry_lock:
                    portfolio, quote = await asyncio.gather(self.refresh(), self.market(analysis["ticker"]))
                    check_announcement_clock()
                    inputs = {key: prepared[key] for key in ("company", "liquidity", "reference")}
                    inputs.update(portfolio=portfolio, quote=quote)
                    market_reasons = ["market:"+code for code in quote.get("reason_codes", [])]
                    portfolio_reasons = ["portfolio:"+code for code in (portfolio or {}).get("reconciliation_reasons", [])]
                    candidate = dict(analysis, reasons=list(analysis["reasons"])+market_reasons+portfolio_reasons)
                    decision = evaluate_paper_intent(candidate, inputs, receipt, published_ns, self.calendar, self.clock)
                    result.update(decision_trace=decision, checks=decision["checks"], reasons=decision["reasons"])
                    if decision["paper_intent"] is None:
                        on_decision(result, self.clock())
                        return result
                    if self.killed():
                        raise PaperError("paper_entries_paused")
                    # A prepared reporting-period identity conservatively groups corrected
                    # vendor IDs for one earnings announcement. It is not inferred by an LLM.
                    canonical = prepared.get("announcement_key")
                    if not isinstance(canonical, str) or not canonical.strip():
                        raise PaperError("missing_canonical_announcement_identity")
                    intent = decision["paper_intent"]
                    payload = await asyncio.to_thread(self.ledger.reserve_entry, analysis, intent, receipt, canonical, self.journal.run_id)
                    permit = EntryPermit(payload["client_order_id"], receipt, quote["available_mono_ns"], portfolio["available_mono_ns"],
                        parse_rfc3339(quote["source_utc"])[0], portfolio["snapshot_started_mono_ns"], intent["expires_mono_ns"], mode=analysis["mode"])
                    # Simulation broker is an in-process fixture and never uses EntryPermit
                    # to authorize HTTP. Real PaperBroker independently rejects research input.
                    transitioned = await asyncio.to_thread(self.ledger.mark, payload["client_order_id"], "submitting", "reserved")
                    if not transitioned:
                        raise PaperError("entry_reservation_transition_rejected")
                    self.emit("paper_admission_audit", {"event_id": analysis["event_id"],
                        "client_order_id": payload["client_order_id"], "canonical_announcement_key": canonical,
                        "original_receipt": {"utc_ns": receipt.utc_ns, "mono_ns": receipt.mono_ns},
                        "original_deadline_mono_ns": receipt.mono_ns+DEADLINE_NS,
                        "analysis": deepcopy(analysis), "prepared_context": deepcopy(prepared),
                        "checked_inputs": deepcopy(inputs), "decision_trace": deepcopy(decision),
                        "reserved_payload": deepcopy(payload)})
                    barrier = getattr(self.journal, "wait_durable", None)
                    if not callable(barrier):
                        if not self.broker.simulation:
                            raise PaperError("paper_admission_durability_barrier_missing")
                    else:
                        try:
                            committed = await barrier(receipt.mono_ns+DEADLINE_NS, clock=self.clock)
                        except Exception:
                            # No broker request has started; persistence failure
                            # must not turn this reservation into an unknown POST.
                            raise PaperError("paper_admission_audit_persistence_failed") from None
                        if not committed:
                            raise PaperError("paper_admission_audit_not_durable_before_deadline")
                        result["admission_audit_committed"] = True
            def dispatch(stamp):
                nonlocal dispatched
                dispatched = True
                result.update(decision="synthetic_entry_ready" if self.broker.simulation else "paper_entry_ready", reasons=[], decision_ready_utc=iso_utc(stamp.utc_ns),
                              elapsed_from_receipt_ns=stamp.mono_ns-receipt.mono_ns,
                              completed_within_deadline=stamp.mono_ns < receipt.mono_ns+DEADLINE_NS,
                              client_order_id=payload["client_order_id"])
                on_decision(result, stamp)
            def abort(exc, stamp):
                nonlocal dispatched
                dispatched = False
                result.update(decision="skip", reasons=[exc.code], pre_dispatch_abort=True,
                    supersedes_candidate_client_id=payload["client_order_id"],
                    elapsed_from_receipt_ns=stamp.mono_ns-receipt.mono_ns,
                    completed_within_deadline=stamp.mono_ns < receipt.mono_ns+DEADLINE_NS)
                on_decision(result, stamp)
            # Ack/fill latency is separate. The transport rechecks the original deadline
            # after pool/connection queueing, immediately before POST headers are sent.
            order = await self.broker.submit(payload, permit=permit, on_dispatch=dispatch,
                reservation_check=check_reservation, on_abort=abort)
            await asyncio.to_thread(self.ledger.apply_order, order, "POST_response")
            result["broker_acknowledged"] = True
        except (TimeoutError, PaperError) as exc:
            code = exc.code if isinstance(exc, PaperError) else "five_second_deadline_expired_during_admission"
            if payload:
                uncertain = exc.ambiguous if isinstance(exc, PaperError) else dispatched
                state = "unknown" if uncertain else "rejected" if isinstance(exc, PaperError) and exc.http_status in (400, 401, 403) else "not_submitted"
                await asyncio.to_thread(self.ledger.mark, payload["client_order_id"], state)
                if uncertain:
                    await asyncio.to_thread(self.ledger.pause, "ambiguous_paper_submission")
                    self.flags = await asyncio.to_thread(self.ledger.flags)
            self.emit("paper_entry_outcome", {"event_id": analysis["event_id"], "reason": code, "dispatch_observed": dispatched})
            if not dispatched:
                result["reasons"].append(code)
                on_decision(result, self.clock())
        except asyncio.CancelledError:
            if payload:
                await asyncio.to_thread(self.ledger.mark, payload["client_order_id"], "unknown")
            raise
        except Exception as exc:
            if payload:
                await asyncio.to_thread(self.ledger.mark, payload["client_order_id"], "unknown")
            await asyncio.to_thread(self.ledger.pause, "unexpected_paper_admission_error")
            self.flags = await asyncio.to_thread(self.ledger.flags)
            self.emit("paper_entry_outcome", {"event_id": analysis["event_id"], "reason": "admission_error", "error_class": type(exc).__name__})
            if not dispatched:
                result["reasons"].append("paper_admission_error")
                on_decision(result, self.clock())
        return result

    def check_current_market(self, ticker, quote, now, *, side, quantity):
        validator = getattr(self.market, "validate_dispatch", None)
        if not callable(validator):
            if not self.broker.simulation:
                raise PaperError("live_market_dispatch_guard_missing")
            return
        reasons = validator(ticker, quote, now, side=side, quantity=quantity)
        if reasons:
            raise PaperError("market_changed_before_dispatch:"+",".join(reasons))

    async def maintain_orders(self, *, recovering=False):
        now = self.clock()
        rows = await asyncio.to_thread(self.ledger.orders)
        for row in rows:
            if row["state"] in LOCAL_TERMINAL:
                continue
            expired = row["expiry_mono_ns"] is None or now.mono_ns >= row["expiry_mono_ns"]
            should_cancel = (recovering or row["owner_run"] != self.journal.run_id or expired
                             or (row["role"] == "entry" and self.killed()))
            if not should_cancel or now.mono_ns < self.cancel_after.get(row["client_id"], 0):
                continue
            self.cancel_after[row["client_id"]] = now.mono_ns+5*SECOND
            if row["state"] == "reserved":
                await asyncio.to_thread(self.ledger.mark, row["client_id"], "not_submitted")
            elif row["broker_id"]:
                try:
                    if not self.identity_ok:
                        raise PaperError("paper_account_identity_not_verified")
                    await self.broker.cancel(row["broker_id"])
                    await asyncio.to_thread(self.ledger.mark, row["client_id"], "cancel_requested")
                except Exception as exc:
                    self.emit("paper_cancel_gap", {"client_order_id": row["client_id"], "error_class": type(exc).__name__})
            else:
                self.emit("paper_cancel_waiting_for_identity", {"client_order_id": row["client_id"], "state": row["state"]})
        for parent in rows:
            if parent["role"] != "entry" or decimal(parent["filled_qty"]) <= 0 or now.utc_ns < parent["exit_utc_ns"]:
                continue
            children = [r for r in rows if r["parent_id"] == parent["client_id"]]
            remaining = decimal(parent["filled_qty"])-sum((decimal(r["filled_qty"]) for r in children), Decimal(0))
            if remaining <= 0 or any(r["state"] not in LOCAL_TERMINAL for r in children):
                continue
            if parent["state"] not in LOCAL_TERMINAL:
                continue  # Cancel an unfilled entry remainder before beginning its exit.
            if now.mono_ns < self.exit_after.get(parent["client_id"], 0):
                continue
            self.exit_after[parent["client_id"]] = now.mono_ns+5*SECOND
            await self.exit_position(parent, remaining, len(children)+1)

    async def exit_position(self, parent, remaining, attempt):
        client_id = "research-"+hashlib.sha256((parent["client_id"]+":exit:"+str(attempt)).encode()).hexdigest()[:32]
        reserved = False
        try:
            if self.calendar.session(self.clock().utc_ns) != "regular":
                raise PaperError("scheduled_exit_waiting_for_regular_session")
            async with self.entry_lock:
                _, quote = await asyncio.gather(self.refresh(), self.market(parent["company"]))
                now = self.clock()
                snapshot = self.last_snapshot
                if not self.identity_ok:
                    raise PaperError("paper_account_identity_not_verified")
                positions = [p for p in snapshot["positions"] if p["symbol"] == parent["company"] and p.get("side") == "long"]
                if len(positions) != 1:
                    raise PaperError("scheduled_exit_long_position_not_reconciled")
                p = positions[0]
                available = min(decimal(p["qty"]), decimal(p.get("qty_available", p["qty"])))
                bid = decimal(quote["bid"])
                size = decimal(quote["bid_size_shares"])
                if remaining != remaining.to_integral_value():
                    raise PaperError("fractional_exit_requires_verified_broker_support")
                qty = min(remaining, available, size.to_integral_value(rounding=ROUND_FLOOR))
                if qty <= 0 or qty != qty.to_integral_value():
                    raise PaperError("scheduled_exit_no_whole_share_capacity")
                quantum = Decimal("0.01") if bid >= 1 else Decimal("0.0001")
                limit = (bid/quantum).to_integral_value(rounding=ROUND_FLOOR)*quantum
                if limit <= 0:
                    raise PaperError("invalid_exit_bid")
                expiry_mono = now.mono_ns+SECOND
                def check(stamp):
                    self.check_current_market(parent["company"], quote, stamp, side="sell", quantity=str(qty))
                    if (not 0 <= stamp.utc_ns-parse_rfc3339(quote["source_utc"])[0] <= SECOND
                            or not 0 <= stamp.mono_ns-quote["available_mono_ns"] <= SECOND
                            or not 0 <= stamp.mono_ns-snapshot["started"].mono_ns <= SECOND
                            or stamp.mono_ns >= expiry_mono or self.calendar.session(stamp.utc_ns) != "regular"):
                        raise PaperError("exit_state_expired_before_dispatch")
                    if (quote.get("coverage_verified") is not True or quote.get("conditions_eligible") is not True
                            or quote.get("size_unit_verified") is not True or quote.get("market_status") != "tradable"):
                        raise PaperError("exit_market_state_not_verified")
                    config = snapshot["config"]
                    if config.get("no_shorting") is not True:
                        raise PaperError("broker_long_only_exit_protection_not_verified")
                check(now)
                payload = {"symbol": parent["company"], "side": "sell", "qty": str(qty), "type": "limit", "limit_price": str(limit),
                           "time_in_force": "day", "extended_hours": False, "client_order_id": client_id}
                await asyncio.to_thread(self.ledger.reserve_exit, parent, payload, self.journal.run_id, now.utc_ns+SECOND, expiry_mono)
                reserved = True
                if not await asyncio.to_thread(self.ledger.mark, client_id, "submitting", "reserved"):
                    raise PaperError("exit_reservation_transition_rejected")
                self.emit("paper_exit_audit", {"client_order_id": client_id, "parent_client_id": parent["client_id"],
                    "scheduled_exit_utc": iso_utc(parent["exit_utc_ns"]), "quote": deepcopy(quote),
                    "portfolio": deepcopy(self.portfolio), "reserved_payload": deepcopy(payload),
                    "expiry_mono_ns": expiry_mono})
                barrier = getattr(self.journal, "wait_durable", None)
                if not callable(barrier):
                    if not self.broker.simulation:
                        raise PaperError("paper_exit_durability_barrier_missing")
                else:
                    try:
                        committed = await barrier(expiry_mono, clock=self.clock)
                    except Exception:
                        raise PaperError("paper_exit_audit_persistence_failed") from None
                    if not committed:
                        raise PaperError("paper_exit_audit_not_durable_before_order_expiry")
            def dispatch(stamp):
                self.emit("paper_exit_decision", {"client_order_id": client_id, "parent_client_id": parent["client_id"],
                    "scheduled_exit_utc": iso_utc(parent["exit_utc_ns"]), "decision_utc": iso_utc(stamp.utc_ns),
                    "lateness_ns": stamp.utc_ns-parent["exit_utc_ns"], "payload": payload}, stamp)
            body = await self.broker.submit(payload, exit_check=check, on_dispatch=dispatch,
                reservation_check=lambda: self.ledger.check_dispatch(client_id, self.journal.run_id, "exit"))
            await asyncio.to_thread(self.ledger.apply_order, body, "exit_POST_response")
        except asyncio.CancelledError:
            if reserved:
                await asyncio.to_thread(self.ledger.mark, client_id, "unknown")
            raise
        except Exception as exc:
            if reserved:
                await asyncio.to_thread(self.ledger.mark, client_id, "unknown" if not isinstance(exc, PaperError) or exc.ambiguous else "not_submitted")
            self.emit("paper_exit_overdue", {"parent_client_id": parent["client_id"], "scheduled_exit_utc": iso_utc(parent["exit_utc_ns"]),
                "reason": exc.code if isinstance(exc, PaperError) else "exit_error", "error_class": type(exc).__name__})

    async def run(self):
        stream = asyncio.create_task(self.broker.trade_updates(self.update, self.gap))
        next_refresh, initialized = 0, False
        try:
            while True:
                now = self.clock()
                try:
                    if not initialized:
                        await self.initialize()
                        initialized = True
                    if now.mono_ns >= next_refresh:
                        await self.refresh()
                        next_refresh = self.clock().mono_ns+5*SECOND
                    await self.maintain_orders()
                except Exception as exc:
                    self.identity_ok = False
                    await asyncio.to_thread(self.ledger.pause, "paper_lifecycle_operation_failed")
                    self.flags = await asyncio.to_thread(self.ledger.flags)
                    self.emit("paper_reconciliation_gap", {"error_class": type(exc).__name__})
                    await asyncio.sleep(5)
                await asyncio.sleep(0.1)
        finally:
            stream.cancel()
            await asyncio.gather(stream, return_exceptions=True)
            if self.account_lock:
                self.account_lock.close()
