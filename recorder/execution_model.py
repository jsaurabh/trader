"""Conservative quote-based execution scenarios, never broker or observed fills.

Positive fills remain assumptions about displayed liquidity, not guaranteed executable
opportunities. Unknown sizes/conditions/continuity produce no modeled fill.
"""
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
from pathlib import Path

from .timebase import SECOND, iso_utc

VERSION = "displayed-liquidity-limit-scenario-v1"
IMPLEMENTATION_SOURCE = Path(__file__).read_text()


def amount(value):
    if not isinstance(value, str):
        raise ValueError("exact decimal strings required")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("finite amount required")
    return result


@dataclass(frozen=True)
class QuoteObservation:
    symbol: str
    source_utc_ns: int
    available_utc_ns: int
    bid: str
    ask: str
    bid_size_shares: str | None
    ask_size_shares: str | None
    mode: str
    availability_kind: str
    quote_id: str
    feed: str
    size_unit_verified: bool = False
    conditions_eligible: bool = False
    continuity_verified: bool = False
    session_supported: bool = False
    market_tradable: bool = False
    ambiguous: bool = False
    retrieved_at_utc_ns: int | None = None
    conditions: tuple[str, ...] = ()
    verification_refs: tuple[str, ...] = ()
    source_precision_ns: int | None = None
    source_timestamp_raw: str | None = None

    def __post_init__(self):
        for key in ("size_unit_verified", "conditions_eligible", "continuity_verified", "session_supported", "market_tradable", "ambiguous"):
            if type(getattr(self, key)) is not bool:
                raise ValueError("quote capability flags must be explicit booleans")
        for key in ("conditions", "verification_refs"):
            value = getattr(self, key)
            if not isinstance(value, (tuple, list)) or any(not isinstance(item, str) for item in value):
                raise ValueError("quote provenance lists must contain strings")
            object.__setattr__(self, key, tuple(value))
        if self.mode not in ("OBSERVED_REPLAY", "HISTORICAL", "SYNTHETIC"):
            raise ValueError("explicit replay provenance required")
        expected = "observed_receipt" if self.mode == "OBSERVED_REPLAY" else "assumed_from_source_plus_delay"
        if self.availability_kind != expected or self.available_utc_ns < self.source_utc_ns:
            raise ValueError("availability cannot silently substitute source or retrieval time")
        if self.mode == "HISTORICAL" and (self.retrieved_at_utc_ns is None or self.retrieved_at_utc_ns < self.source_utc_ns):
            raise ValueError("historical quotes retain their actual later retrieval time")

    def issues(self, at_ns, *, max_age_ns=SECOND, max_source_precision_ns=1_000_000):
        reasons = []
        if self.mode != "SYNTHETIC" and not self.verification_refs:
            reasons.append("missing_quote_verification_provenance")
        if self.mode != "SYNTHETIC" and (type(self.source_precision_ns) is not int or not 0 < self.source_precision_ns <= max_source_precision_ns):
            reasons.append("source_timestamp_resolution_insufficient_or_unknown")
        if not self.size_unit_verified:
            reasons.append("unverified_share_size")
        if not self.conditions_eligible:
            reasons.append("quote_conditions_not_verified")
        if not self.continuity_verified:
            reasons.append("quote_window_continuity_not_verified")
        if not self.session_supported or not self.market_tradable:
            reasons.append("session_or_market_status_not_verified")
        if self.ambiguous:
            reasons.append("ambiguous_quote_state")
        if not 0 <= at_ns-self.available_utc_ns <= max_age_ns or not 0 <= at_ns-self.source_utc_ns <= max_age_ns:
            reasons.append("quote_not_available_or_fresh")
        try:
            bid, ask = amount(self.bid), amount(self.ask)
            bs, asks = amount(self.bid_size_shares), amount(self.ask_size_shares)
            if not 0 < bid <= ask or min(bs, asks) <= 0:
                raise ValueError
        except (ValueError, TypeError, ArithmeticError):
            reasons.append("invalid_crossed_empty_quote")
        return reasons

    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionAssumptions:
    participation_fraction: str = "0.10"
    max_quote_age_ns: int = SECOND
    max_source_precision_ns: int = 1_000_000
    each_side_cost_bps: str = "0"
    per_share_cost: str = "0"
    per_filled_order_cost: str = "0"
    initial_cash: str = "100000"
    maximum_position_dollars: str = "1000"
    maximum_companies: int = 3

    def __post_init__(self):
        if (not 0 < amount(self.participation_fraction) <= 1 or self.max_quote_age_ns <= 0
                or not 0 < self.max_source_precision_ns <= self.max_quote_age_ns):
            raise ValueError("invalid execution participation/freshness hypothesis")
        if any(amount(value) < 0 for value in (self.each_side_cost_bps, self.per_share_cost, self.per_filled_order_cost)):
            raise ValueError("cost assumptions cannot improve results")
        if (amount(self.initial_cash) != 100000 or not 0 < amount(self.maximum_position_dollars) <= 1000
                or type(self.maximum_companies) is not int or not 1 <= self.maximum_companies <= 3):
            raise ValueError("scenario cannot enlarge the agreed paper risk envelope")


@dataclass
class ModelOrder:
    order_id: str
    event_id: str
    symbol: str
    side: str
    qty: int
    limit_price: str
    submitted_utc_ns: int
    arrives_utc_ns: int
    expires_utc_ns: int
    mode: str
    parent_order_id: str | None = None
    state: str = "open"
    filled_qty: int = 0
    filled_notional: str = "0"
    costs: str = "0"
    first_fill_utc_ns: int | None = None
    last_fill_utc_ns: int | None = None

    @property
    def remaining_qty(self):
        return self.qty-self.filled_qty


class ConservativeExecutionModel:
    """Whole shares, cash only, capped participation, and fills at the limit price.

    Quotes must arrive after the modeled order reaches the market. Unchanged price
    levels do not replenish already consumed capacity merely because another quote
    message repeats them. All fills are MODEL assumptions and never broker paper fills.
    """
    def __init__(self, assumptions=None):
        self.assumptions = assumptions or ExecutionAssumptions()
        self.cash = amount(self.assumptions.initial_cash)
        self.orders = {}
        self.fills, self.events = [], []
        self.entry_keys = set()
        self.liquidity = {}
        self.realized_pnl = Decimal(0)
        self.clock_ns = None

    def holdings(self):
        held = {}
        for order in self.orders.values():
            held[order.symbol] = held.get(order.symbol, 0)+(order.filled_qty if order.side == "buy" else -order.filled_qty)
        return {symbol: qty for symbol, qty in held.items() if qty}

    def lot_available(self, parent_order_id):
        parent = self.orders[parent_order_id]
        exits = [order for order in self.orders.values() if order.parent_order_id == parent_order_id]
        filled = sum(order.filled_qty for order in exits)
        pending = sum(order.remaining_qty for order in exits if order.state == "open")
        return parent.filled_qty-filled-pending

    def advance(self, utc_ns):
        if self.clock_ns is not None and utc_ns < self.clock_ns:
            raise ValueError("execution clock cannot run backward")
        self.clock_ns = utc_ns
        for order in self.orders.values():
            if order.state == "open" and utc_ns >= order.expires_utc_ns:
                order.state = "expired"
                self.events.append({"kind": "model_order_expired", "order_id": order.order_id, "utc": iso_utc(utc_ns),
                                    "filled_qty": order.filled_qty, "unfilled_qty": order.remaining_qty})

    def submit(self, order):
        self.advance(order.submitted_utc_ns)
        if (order.order_id in self.orders or order.side not in ("buy", "sell") or type(order.qty) is not int or order.qty <= 0
                or amount(order.limit_price) <= 0 or order.arrives_utc_ns < order.submitted_utc_ns
                or order.expires_utc_ns <= order.arrives_utc_ns or order.mode not in ("HISTORICAL", "SYNTHETIC", "OBSERVED_REPLAY")):
            raise ValueError("invalid or duplicate model limit order")
        if order.side == "buy":
            key = (order.symbol, order.event_id)
            if key in self.entry_keys:
                raise ValueError("one entry attempt per company and announcement")
            notional = order.qty*amount(order.limit_price)
            reserved = sum((other.remaining_qty*amount(other.limit_price) for other in self.orders.values() if other.side == "buy" and other.state == "open"), Decimal(0))
            companies = set(self.holdings()) | {other.symbol for other in self.orders.values() if other.side == "buy" and other.state == "open"}
            if (order.symbol in companies or len(companies) >= self.assumptions.maximum_companies
                    or notional > amount(self.assumptions.maximum_position_dollars) or notional > self.cash-reserved):
                raise ValueError("model position/cash capacity exceeded")
            self.entry_keys.add(key)
        else:
            parent = self.orders.get(order.parent_order_id)
            if (parent is None or parent.side != "buy" or parent.symbol != order.symbol or parent.state == "open"
                    or order.qty > self.lot_available(parent.order_id)):
                raise ValueError("model sell would exceed its owned parent lot")
        self.orders[order.order_id] = order
        self.events.append({"kind": "model_order_submitted", **asdict(order)})

    def on_quote(self, quote):
        self.advance(quote.available_utc_ns)
        issues = quote.issues(quote.available_utc_ns, max_age_ns=self.assumptions.max_quote_age_ns,
                              max_source_precision_ns=self.assumptions.max_source_precision_ns)
        if issues:
            self.events.append({"kind": "quote_not_fill_eligible", "quote_id": quote.quote_id, "reasons": issues})
            return []
        new_fills = []
        for side, price, visible in (("buy", amount(quote.ask), amount(quote.ask_size_shares)),
                                     ("sell", amount(quote.bid), amount(quote.bid_size_shares))):
            level_key = (quote.symbol, side)
            level = self.liquidity.get(level_key)
            if level is None or level["price"] != price:
                level = self.liquidity[level_key] = {"price": price, "consumed": 0}
            capacity = max(0, int((visible*amount(self.assumptions.participation_fraction)).to_integral_value(rounding=ROUND_FLOOR))-level["consumed"])
            if not capacity:
                continue
            for order in self.orders.values():
                if (order.state != "open" or order.side != side or order.symbol != quote.symbol or order.mode != quote.mode
                        or quote.source_utc_ns <= order.arrives_utc_ns or quote.available_utc_ns < order.arrives_utc_ns):
                    continue
                limit = amount(order.limit_price)
                if (side == "buy" and price > limit) or (side == "sell" and price < limit):
                    continue
                qty = min(order.remaining_qty, capacity)
                if side == "sell":
                    parent = self.orders[order.parent_order_id]
                    sold = sum(other.filled_qty for other in self.orders.values() if other.parent_order_id == parent.order_id)
                    qty = min(qty, parent.filled_qty-sold)
                notional = qty*limit
                cost = notional*amount(self.assumptions.each_side_cost_bps)/10000 + qty*amount(self.assumptions.per_share_cost)
                if not order.filled_qty:
                    cost += amount(self.assumptions.per_filled_order_cost)
                if side == "buy" and notional+cost > self.cash:
                    # Costs cannot create leverage. Leave the order unfilled instead.
                    continue
                if qty <= 0:
                    continue
                self.cash += (notional if side == "sell" else -notional)-cost
                order.filled_qty += qty
                order.filled_notional = str(amount(order.filled_notional)+notional)
                order.costs = str(amount(order.costs)+cost)
                order.first_fill_utc_ns = order.first_fill_utc_ns or quote.available_utc_ns
                order.last_fill_utc_ns = quote.available_utc_ns
                order.state = "filled" if not order.remaining_qty else "open"
                level["consumed"] += qty
                capacity -= qty
                realized = None
                if side == "sell":
                    parent = self.orders[order.parent_order_id]
                    allocated_basis = qty*(amount(parent.filled_notional)+amount(parent.costs))/parent.filled_qty
                    realized = notional-cost-allocated_basis
                    self.realized_pnl += realized
                fill = {"kind": "MODEL_FILL", "model_version": VERSION, "order_id": order.order_id, "event_id": order.event_id,
                        "symbol": order.symbol, "side": side, "qty": qty, "price": str(limit), "notional": str(notional),
                        "cost": str(cost), "realized_pnl": str(realized) if realized is not None else None,
                        "fill_utc": iso_utc(quote.available_utc_ns), "quote_source_utc": iso_utc(quote.source_utc_ns),
                        "quote_available_utc": iso_utc(quote.available_utc_ns), "quote_id": quote.quote_id,
                        "quote_source_precision_ns": quote.source_precision_ns, "quote_source_timestamp_raw": quote.source_timestamp_raw,
                        "quote_retrieved_at_utc": iso_utc(quote.retrieved_at_utc_ns) if quote.retrieved_at_utc_ns else None,
                        "quote_verification_refs": quote.verification_refs,
                        "mode": order.mode, "observed_broker_fill": False,
                        "fill_assumption": "limit price; capped displayed participation; no unchanged-level replenishment"}
                self.fills.append(fill); new_fills.append(fill)
                if not capacity:
                    break
        return new_fills

    def summary(self):
        return {"model_version": VERSION, "assumptions": asdict(self.assumptions), "cash": str(self.cash),
                "open_positions": self.holdings(), "realized_pnl_after_modeled_costs": str(self.realized_pnl),
                "orders": [asdict(order) for order in self.orders.values()], "fills": self.fills,
                "events": self.events, "observed_broker_fills": [],
                "limitations": ["Scenario fills are assumptions, not observed execution or guaranteed lower bounds",
                    "Displayed size does not establish queue position, persistence, hidden liquidity or market impact",
                    "Unknown quote sizes/conditions/continuity/session status cannot generate modeled fills",
                    "Broker paper fills, actual fees and conservative-model outcomes must remain separate"]}
