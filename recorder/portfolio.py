"""Private durable paper ledger. Broker snapshots and simulation fixtures stay separate."""
from contextlib import contextmanager
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo
from datetime import datetime

from .decision import decimal
from .paper_broker import PaperError, TERMINAL
from .timebase import DEADLINE_NS, Stamp, iso_utc, parse_rfc3339

LOCAL_TERMINAL = TERMINAL | {"not_submitted"}


class PaperLedger:
    def __init__(self, path, *, mode="OBSERVED", initial_cash="100000", clock=Stamp.now):
        if mode not in ("OBSERVED", "SYNTHETIC") or decimal(initial_cash) != 100000:
            raise ValueError("Separate observed/synthetic ledgers start at the agreed $100,000")
        self.path, self.mode, self.clock = Path(path), mode, clock
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        os.chmod(self.path, 0o600)
        with self.transaction() as c:
            c.executescript("""CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS orders (
                client_id TEXT PRIMARY KEY,event_key TEXT NOT NULL,canonical_key TEXT NOT NULL,company TEXT NOT NULL,
                role TEXT NOT NULL,parent_id TEXT,payload TEXT NOT NULL,owner_run TEXT NOT NULL,
                receipt_utc_ns INTEGER,receipt_mono_ns INTEGER,expiry_utc_ns INTEGER NOT NULL,exit_utc_ns INTEGER NOT NULL,
                state TEXT NOT NULL,broker_id TEXT,filled_qty TEXT NOT NULL DEFAULT '0',filled_notional TEXT NOT NULL DEFAULT '0',
                source_updated_ns INTEGER,created_utc_ns INTEGER NOT NULL,updated_utc_ns INTEGER NOT NULL,
                created_mono_ns INTEGER,expiry_mono_ns INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS company_event_once ON orders(company,event_key) WHERE role='entry';
                CREATE UNIQUE INDEX IF NOT EXISTS canonical_event_once ON orders(canonical_key) WHERE role='entry';
                CREATE TABLE IF NOT EXISTS executions (execution_key TEXT PRIMARY KEY,client_id TEXT NOT NULL,data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS ledger_events (seq INTEGER PRIMARY KEY,utc_ns INTEGER NOT NULL,kind TEXT NOT NULL,data TEXT NOT NULL);""")
            columns = {row[1] for row in c.execute("PRAGMA table_info(orders)")}
            for column in ("created_mono_ns", "expiry_mono_ns"):
                if column not in columns:
                    c.execute("ALTER TABLE orders ADD COLUMN "+column+" INTEGER")
            if self.get(c, "mode") not in (None, mode):
                raise ValueError("Observed and synthetic paper state must be separate")
            self.put(c, "mode", mode)
            for key, value in {"initial_cash": initial_cash, "peak_equity": initial_cash, "account_hash": None,
                               "review_required": False, "daily_pause": False, "day": None, "kill_switch": False}.items():
                if self.get(c, key) is None:
                    self.put(c, key, value)
            self.put(c, "reconciled", False)
            self.event(c, "process_start", {"mode": mode, "reconciliation_required": True})

    @contextmanager
    def transaction(self):
        c = sqlite3.connect(self.path, timeout=0.1)
        c.row_factory = sqlite3.Row
        try:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=FULL")
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    @staticmethod
    def get(c, key):
        row = c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    @contextmanager
    def read(self):
        c = sqlite3.connect(self.path.resolve().as_uri()+"?mode=ro", uri=True, timeout=0.1)
        try:
            c.row_factory = sqlite3.Row
            c.execute("BEGIN")
            yield c
        finally:
            c.close()

    def _verify_account(self, c, snapshot):
        try:
            account = snapshot["account"]
            identity = hashlib.sha256(account["id"].encode()).hexdigest()
            bound = self.get(c, "account_hash")
            if bound is None:
                if (self.rows(c) or snapshot["positions"] or snapshot["open_orders"] or decimal(account["cash"]) != 100000
                        or decimal(account["equity"]) != 100000 or account["status"] != "ACTIVE" or account["currency"] != "USD"):
                    raise ValueError
                self.put(c, "account_hash", identity)
            elif bound != identity:
                raise ValueError
            return True
        except (KeyError, TypeError, ValueError, ArithmeticError):
            self.put(c, "review_required", True)
            self.put(c, "reconciled", False)
            self.event(c, "account_identity_quarantined", {"reason": "identity_or_initial_state_mismatch"})
            return False

    def verify_account(self, snapshot):
        with self.transaction() as c:
            return self._verify_account(c, snapshot)

    @staticmethod
    def put(c, key, value):
        c.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, json.dumps(value)))

    def event(self, c, kind, data):
        c.execute("INSERT INTO ledger_events(utc_ns,kind,data) VALUES(?,?,?)", (self.clock().utc_ns, kind, json.dumps(data)))

    def flags(self):
        with self.read() as c:
            return {key: self.get(c, key) for key in ("reconciled", "review_required", "daily_pause", "kill_switch", "day", "account_hash")}

    def pause(self, reason, *, review=False):
        with self.transaction() as c:
            self.put(c, "reconciled", False)
            if review:
                self.put(c, "review_required", True)
            self.event(c, "entries_paused", {"reason": reason, "review": review})

    def kill(self, enabled):
        with self.transaction() as c:
            self.put(c, "kill_switch", bool(enabled))
            self.event(c, "manual_kill_switch", {"enabled": bool(enabled)})

    @staticmethod
    def rows(c):
        return [dict(row) | {"payload": json.loads(row["payload"])} for row in c.execute("SELECT * FROM orders ORDER BY created_utc_ns,client_id")]

    def orders(self):
        with self.read() as c:
            return self.rows(c)

    @staticmethod
    def holdings(rows):
        positions = {}
        for row in rows:
            sign = 1 if row["role"] == "entry" else -1
            positions[row["company"]] = positions.get(row["company"], Decimal(0)) + sign*decimal(row["filled_qty"])
        return {s: q for s, q in positions.items() if q != 0}

    def _apply_order(self, c, order, source):
        client_id = order.get("client_order_id")
        row = c.execute("SELECT * FROM orders WHERE client_id=?", (client_id,)).fetchone()
        if row is None:
            return False
        payload = json.loads(row["payload"])
        try:
            qty, filled = decimal(str(order["qty"])), decimal(str(order["filled_qty"]))
            average = decimal(str(order["filled_avg_price"])) if filled else Decimal(0)
            valid = (order["symbol"] == row["company"] and order["side"] == payload["side"] and order["type"] == "limit"
                     and qty == decimal(payload["qty"]) and decimal(str(order["limit_price"])) == decimal(payload["limit_price"])
                     and 0 <= filled <= qty and (not filled or average > 0) and isinstance(order["id"], str))
            updated = parse_rfc3339(order["updated_at"])[0] if order.get("updated_at") else None
            if not valid:
                raise ValueError
        except (KeyError, ValueError, TypeError, ArithmeticError):
            self.put(c, "review_required", True)
            self.put(c, "reconciled", False)
            self.event(c, "invalid_broker_order_snapshot", {"client_id": client_id, "source": source})
            return False
        if row["source_updated_ns"] and updated and updated < row["source_updated_ns"]:
            self.event(c, "out_of_order_snapshot_ignored", {"client_id": client_id, "source": source})
            return True
        if filled < decimal(row["filled_qty"]):
            self.put(c, "review_required", True)
            self.put(c, "reconciled", False)
            self.event(c, "cumulative_fill_regressed", {"client_id": client_id, "source": source})
            return False
        status = order.get("status")
        if not isinstance(status, str) or (row["state"] in TERMINAL and status not in TERMINAL and filled == decimal(row["filled_qty"])):
            self.event(c, "nonmonotone_status_ignored", {"client_id": client_id, "source": source})
            return True
        if row["state"] == "not_submitted":
            self.put(c, "review_required", True)
            self.event(c, "unexpected_order_after_no_submission", {"client_id": client_id})
        c.execute("UPDATE orders SET state=?,broker_id=?,filled_qty=?,filled_notional=?,source_updated_ns=?,updated_utc_ns=? WHERE client_id=?",
                  (status, order["id"], str(filled), str(filled*average), updated, self.clock().utc_ns, client_id))
        self.event(c, "broker_order_snapshot", {"client_id": client_id, "source": source, "order": order})
        return True

    def apply_order(self, order, source="REST"):
        with self.transaction() as c:
            return self._apply_order(c, order, source)

    def trade_update(self, update):
        with self.transaction() as c:
            order = update.get("order", {})
            client_id = order.get("client_order_id")
            owned = c.execute("SELECT 1 FROM orders WHERE client_id=?", (client_id,)).fetchone()
            if not owned:
                self.put(c, "review_required", True)
                self.put(c, "reconciled", False)
                self.event(c, "unowned_trade_update", {"update": update})
                return False
            if update.get("event") in ("fill", "partial_fill"):
                execution_id = update.get("execution_id") or "unverified:"+hashlib.sha256(json.dumps(update, sort_keys=True).encode()).hexdigest()
                c.execute("INSERT OR IGNORE INTO executions VALUES(?,?,?)", (execution_id, client_id, json.dumps(update)))
            if update.get("event") in ("trade_bust", "trade_correct"):
                self.put(c, "review_required", True)
                self.put(c, "reconciled", False)
            return self._apply_order(c, order, "trade_updates")

    def reconcile(self, snapshot):
        """Do not infer positions from intended quantities or assume cancellation filled zero."""
        now = self.clock()
        account, broker_positions, open_orders = snapshot["account"], snapshot["positions"], snapshot["open_orders"]
        with self.transaction() as c:
            if not self._verify_account(c, snapshot):
                return None
            reasons = []
            known = {r["client_id"] for r in self.rows(c)}
            for order in open_orders:
                if order.get("client_order_id") not in known:
                    reasons.append("unowned_open_order")
                    self.put(c, "review_required", True)
                elif not self._apply_order(c, order, "open_orders_snapshot"):
                    reasons.append("invalid_owned_order")
            rows = self.rows(c)
            try:
                cash, equity, previous_close = (decimal(account[k]) for k in ("cash", "equity", "last_equity"))
                identity = hashlib.sha256(account["id"].encode()).hexdigest()
                if account["status"] != "ACTIVE" or account["currency"] != "USD" or account["trading_blocked"] or account["account_blocked"]:
                    reasons.append("paper_account_not_eligible")
                config = snapshot["config"]
                if (config.get("no_shorting") is not True or config.get("max_margin_multiplier") != "1"
                        or config.get("max_options_trading_level") != 0 or config.get("disable_overnight_trading") is not True
                        or config.get("suspend_trade") is True):
                    reasons.append("paper_long_only_configuration_not_verified")
                bound = self.get(c, "account_hash")
                if bound is None:
                    if rows or broker_positions or open_orders or cash != 100000 or equity != 100000:
                        reasons.append("initial_paper_account_not_empty_100k")
                        self.put(c, "review_required", True)
                    else:
                        self.put(c, "account_hash", identity)
                elif bound != identity:
                    reasons.append("paper_account_identity_changed")
                    self.put(c, "review_required", True)
                held = self.holdings(rows)
                actual, positions = {}, []
                for p in broker_positions:
                    qty, value = decimal(p["qty"]), decimal(p["market_value"])
                    if p["symbol"] in actual or qty <= 0 or value < 0 or p.get("side") != "long":
                        raise ValueError
                    actual[p["symbol"]] = qty
                    positions.append({"ticker": p["symbol"], "qty": str(qty), "market_value": str(value)})
                if held != actual:
                    reasons.append("broker_and_ledger_positions_differ")
                    if any(symbol not in {r["company"] for r in rows} for symbol in actual):
                        self.put(c, "review_required", True)
                ledger_cash = Decimal(self.get(c, "initial_cash")) + sum((decimal(r["filled_notional"])*(1 if r["role"] == "exit" else -1) for r in rows), Decimal(0))
                if abs(ledger_cash-cash) > Decimal("0.01") or abs(cash+sum((decimal(p["market_value"]) for p in positions), Decimal(0))-equity) > Decimal("0.01"):
                    reasons.append("cash_or_equity_not_reconciled")
                if previous_close <= 0 or equity <= 0 or cash < 0:
                    reasons.append("invalid_unlevered_account_values")
                peak = max(Decimal(self.get(c, "peak_equity")), equity)
                self.put(c, "peak_equity", str(peak))
                if peak > 0 and 100*(peak-equity)/peak >= 5:
                    self.put(c, "review_required", True)
                day = datetime.fromtimestamp(now.utc_ns//1_000_000_000, ZoneInfo("America/New_York")).date().isoformat()
                if self.get(c, "day") != day:
                    self.put(c, "day", day)
                    self.put(c, "daily_pause", False)
                daily_pnl = equity-previous_close
                if daily_pnl <= -500:
                    self.put(c, "daily_pause", True)
                reservations = [{"ticker": r["company"], "notional": str((decimal(r["payload"]["qty"])-decimal(r["filled_qty"]))*decimal(r["payload"]["limit_price"]))}
                                for r in rows if r["role"] == "entry" and r["state"] not in LOCAL_TERMINAL]
                available = snapshot["received"]
                portfolio = {"paper": True, "reconciled": not reasons, "available_utc": iso_utc(available.utc_ns),
                    "available_mono_ns": available.mono_ns, "snapshot_started_mono_ns": snapshot["started"].mono_ns,
                    "kill_switch": self.get(c, "kill_switch"), "review_required": self.get(c, "review_required"),
                    "daily_pause": self.get(c, "daily_pause"), "daily_pnl": str(daily_pnl), "cash": str(cash),
                    "equity": str(equity), "peak_equity": str(peak), "positions": positions,
                    "entry_reservations": reservations, "entered_company_events": [[r["company"], r["event_key"]] for r in rows if r["role"] == "entry"],
                    "currency": "USD", "source": "SYNTHETIC fabricated broker + durable fixture ledger" if self.mode == "SYNTHETIC"
                    else "fixed Alpaca paper REST snapshot + durable owned-order ledger", "reconciliation_reasons": reasons}
            except (KeyError, TypeError, ValueError, ArithmeticError):
                reasons.append("invalid_paper_snapshot")
                portfolio = None
            self.put(c, "reconciled", not reasons)
            self.event(c, "account_reconciled", {"reasons": reasons, "portfolio": portfolio})
            return portfolio

    def reserve_entry(self, analysis, intent, receipt, canonical_key, owner_run):
        """A durable unique reservation precedes any POST. Expired transactions roll back."""
        with self.transaction() as c:
            if any(self.get(c, key) for key in ("kill_switch", "review_required", "daily_pause")) or not self.get(c, "reconciled"):
                raise PaperError("portfolio_paused_before_reservation")
            if self.clock().mono_ns >= receipt.mono_ns+DEADLINE_NS:
                raise PaperError("five_second_deadline_expired_before_reservation")
            rows = self.rows(c)
            companies = set(self.holdings(rows)) | {r["company"] for r in rows if r["role"] == "entry" and r["state"] not in LOCAL_TERMINAL}
            if len(companies) >= 3 or intent["symbol"] in companies or not canonical_key:
                raise PaperError("atomic_position_capacity_or_identity_rejected")
            cash = Decimal(self.get(c, "initial_cash")) + sum((decimal(r["filled_notional"])*(1 if r["role"] == "exit" else -1) for r in rows), Decimal(0))
            reserved = sum(((decimal(r["payload"]["qty"])-decimal(r["filled_qty"]))*decimal(r["payload"]["limit_price"]) for r in rows if r["role"] == "entry" and r["state"] not in LOCAL_TERMINAL), Decimal(0))
            requested = decimal(intent["qty"])*decimal(intent["limit_price"])
            if not 0 < requested <= min(Decimal("1000"), cash-reserved):
                raise PaperError("atomic_cash_or_position_cap_rejected")
            payload = {key: intent[key] for key in ("symbol", "side", "qty", "type", "limit_price", "time_in_force", "extended_hours", "client_order_id")}
            self._insert(c, analysis["event_id"], canonical_key, payload, owner_run, "entry", None,
                         receipt, parse_rfc3339(intent["expires_utc"])[0], parse_rfc3339(intent["scheduled_exit_utc"])[0], intent.get("expires_mono_ns"))
            if self.clock().mono_ns >= receipt.mono_ns+DEADLINE_NS:
                raise PaperError("five_second_deadline_expired_during_reservation")
            self.event(c, "entry_reserved", {"event_id": analysis["event_id"], "canonical_key": canonical_key,
                       "client_id": payload["client_order_id"], "analysis": analysis})
            return payload

    def _insert(self, c, event_key, canonical_key, payload, owner, role, parent, receipt, expiry, exit_ns, expiry_mono=None):
        now = self.clock()
        try:
            c.execute("INSERT INTO orders(client_id,event_key,canonical_key,company,role,parent_id,payload,owner_run,receipt_utc_ns,receipt_mono_ns,expiry_utc_ns,exit_utc_ns,state,created_utc_ns,updated_utc_ns,created_mono_ns,expiry_mono_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (payload["client_order_id"], event_key, canonical_key, payload["symbol"], role, parent, json.dumps(payload), owner,
                 receipt.utc_ns if receipt else None, receipt.mono_ns if receipt else None, expiry, exit_ns, "reserved", now.utc_ns, now.utc_ns,
                 now.mono_ns, expiry_mono if expiry_mono is not None else now.mono_ns+max(0, expiry-now.utc_ns)))
        except sqlite3.IntegrityError:
            raise PaperError("duplicate_company_announcement_or_client_id") from None

    def reserve_exit(self, parent, payload, owner_run, expiry_ns, expiry_mono_ns=None):
        with self.transaction() as c:
            rows = self.rows(c)
            source = next((r for r in rows if r["client_id"] == parent["client_id"] and r["role"] == "entry"), None)
            if source is None or source["company"] != payload["symbol"]:
                raise PaperError("unknown_exit_parent")
            children = [r for r in rows if r["parent_id"] == parent["client_id"]]
            lot_remaining = decimal(source["filled_qty"]) - sum((decimal(r["filled_qty"]) for r in children), Decimal(0))
            lot_pending = sum((decimal(r["payload"]["qty"])-decimal(r["filled_qty"]) for r in children if r["state"] not in LOCAL_TERMINAL), Decimal(0))
            qty = min(lot_remaining-lot_pending, self.holdings(rows).get(payload["symbol"], Decimal(0)))
            pending = sum((decimal(r["payload"]["qty"])-decimal(r["filled_qty"]) for r in rows if r["company"] == payload["symbol"] and r["role"] == "exit" and r["state"] not in LOCAL_TERMINAL), Decimal(0))
            company_available = self.holdings(rows).get(payload["symbol"], Decimal(0))-pending
            if decimal(payload["qty"]) > min(qty, company_available) or decimal(payload["qty"]) <= 0:
                raise PaperError("exit_would_exceed_owned_long_quantity")
            self._insert(c, parent["event_key"], parent["canonical_key"], payload, owner_run, "exit", parent["client_id"], None, expiry_ns, parent["exit_utc_ns"], expiry_mono_ns)
            self.event(c, "exit_reserved", {"parent": parent["client_id"], "payload": payload})

    def mark(self, client_id, state, expected=None):
        with self.transaction() as c:
            row = c.execute("SELECT state FROM orders WHERE client_id=?", (client_id,)).fetchone()
            if not row:
                raise PaperError("unknown_local_order")
            if row["state"] in LOCAL_TERMINAL or (expected is not None and row["state"] != expected):
                return False
            c.execute("UPDATE orders SET state=?,updated_utc_ns=? WHERE client_id=?", (state, self.clock().utc_ns, client_id))
            self.event(c, "local_order_state", {"client_id": client_id, "state": state})
            return True

    def check_dispatch(self, client_id, owner_run, role):
        with self.read() as c:
            row = c.execute("SELECT state,owner_run,role FROM orders WHERE client_id=?", (client_id,)).fetchone()
            if not row or row["state"] != "submitting" or row["owner_run"] != owner_run or row["role"] != role:
                raise PaperError("durable_dispatch_reservation_not_live")
            if role == "entry" and (not self.get(c, "reconciled") or any(self.get(c, k) for k in ("kill_switch", "review_required", "daily_pause"))):
                raise PaperError("durable_portfolio_paused_before_dispatch")
