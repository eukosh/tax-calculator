from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import polars as pl

from src.broker_history import round_money, round_qty

EVENT_TYPE_AUSTRIAN_BASIS_RESET = "austrian_basis_reset"
EVENT_TYPE_BUY = "buy"
EVENT_TYPE_SELL = "sell"
EVENT_TYPE_OEKB_BASIS_ADJUSTMENT = "oekb_basis_adjustment"
EVENT_TYPE_SPLIT = "split"
EVENT_TYPE_REVERSE_SPLIT = "reverse_split"
EVENT_TYPE_MANUAL_CORRECTION = "manual_correction"


@dataclass
class PositionState:
    broker: str
    ticker: str
    isin: str
    currency: str
    asset_class: str = ""
    quantity: float = 0.0
    base_cost_total_eur: float = 0.0
    basis_adjustment_total_eur: float = 0.0
    last_event_date: str = ""
    basis_method: str = ""
    snapshot_date: str = ""
    source_file: str = ""
    notes: str = ""

    @property
    def total_basis_eur(self) -> float:
        return round_money(self.base_cost_total_eur + self.basis_adjustment_total_eur)

    @property
    def average_basis_eur(self) -> float:
        if self.quantity <= 0:
            return 0.0
        return round_money(self.total_basis_eur / self.quantity)

    @property
    def status(self) -> str:
        return "closed" if self.quantity == 0 else "open"

    def to_record(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "isin": self.isin,
            "asset_class": self.asset_class,
            "currency": self.currency,
            "quantity": round_qty(self.quantity),
            "base_cost_total_eur": round_money(self.base_cost_total_eur),
            "basis_adjustment_total_eur": round_money(self.basis_adjustment_total_eur),
            "total_basis_eur": self.total_basis_eur,
            "average_basis_eur": self.average_basis_eur,
            "status": self.status,
            "last_event_date": self.last_event_date,
            "basis_method": self.basis_method,
            "broker": self.broker,
            "source_file": self.source_file,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class PositionEvent:
    event_type: str
    broker: str
    ticker: str
    isin: str
    currency: str
    event_date: date
    effective_date: date
    eligibility_date: date | None = None
    asset_class: str = ""
    quantity: float = 0.0
    quantity_delta: float = 0.0
    price_ccy: float | None = None
    fx_to_eur: float | None = None
    base_cost_delta_eur: float = 0.0
    basis_adjustment_delta_eur: float = 0.0
    proceeds_eur: float = 0.0
    realized_basis_eur: float = 0.0
    realized_gain_loss_eur: float = 0.0
    realized_base_cost_eur: float = 0.0
    realized_oekb_adjustment_eur: float = 0.0
    split_ratio: float | None = None
    basis_method: str = ""
    source_id: str = ""
    source_file: str = ""
    notes: str = ""
    sequence_key: int = 0


@dataclass
class EventApplicationResult:
    event_record: dict[str, object]
    sale_record: dict[str, object] | None = None


def position_key(*, broker: str, isin: str) -> str:
    del broker
    return isin


def _append_unique_note_text(existing: str, incoming: str) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for part in (piece.strip() for piece in existing.split(";")):
        if not part or part in seen:
            continue
        merged.append(part)
        seen.add(part)
    for part in (piece.strip() for piece in incoming.split(";")):
        if not part or part in seen:
            continue
        merged.append(part)
        seen.add(part)
    return "; ".join(merged)


def add_state_note(state: PositionState, note: str) -> None:
    if not note:
        return
    state.notes = _append_unique_note_text(state.notes, note)


def build_buy_event(
    *,
    broker: str,
    ticker: str,
    isin: str,
    currency: str,
    asset_class: str,
    trade_date: date,
    quantity: float,
    price_ccy: float,
    fx_to_eur: float,
    source_id: str,
    source_file: str,
    sequence_key: int = 0,
) -> PositionEvent:
    quantity = round_qty(quantity)
    base_cost_delta_eur = round_money((quantity * price_ccy) / fx_to_eur)
    return PositionEvent(
        event_type=EVENT_TYPE_BUY,
        broker=broker,
        ticker=ticker,
        isin=isin,
        currency=currency,
        asset_class=asset_class,
        event_date=trade_date,
        effective_date=trade_date,
        quantity=quantity,
        quantity_delta=quantity,
        price_ccy=round_money(price_ccy),
        fx_to_eur=round_money(fx_to_eur),
        base_cost_delta_eur=base_cost_delta_eur,
        source_id=source_id,
        source_file=source_file,
        sequence_key=sequence_key,
    )


def build_sell_event(
    *,
    broker: str,
    ticker: str,
    isin: str,
    currency: str,
    asset_class: str,
    trade_date: date,
    quantity: float,
    price_ccy: float,
    fx_to_eur: float,
    source_id: str,
    source_file: str,
    notes: str = "",
    sequence_key: int = 0,
) -> PositionEvent:
    quantity = round_qty(quantity)
    proceeds_eur = round_money((quantity * price_ccy) / fx_to_eur)
    return PositionEvent(
        event_type=EVENT_TYPE_SELL,
        broker=broker,
        ticker=ticker,
        isin=isin,
        currency=currency,
        asset_class=asset_class,
        event_date=trade_date,
        effective_date=trade_date,
        quantity=quantity,
        quantity_delta=round_qty(-quantity),
        price_ccy=round_money(price_ccy),
        fx_to_eur=round_money(fx_to_eur),
        proceeds_eur=proceeds_eur,
        source_id=source_id,
        source_file=source_file,
        notes=notes,
        sequence_key=sequence_key,
    )


def build_basis_reset_event(
    *,
    broker: str,
    ticker: str,
    isin: str,
    currency: str,
    asset_class: str,
    event_date: date,
    quantity: float,
    base_cost_total_eur: float,
    basis_adjustment_total_eur: float = 0.0,
    basis_method: str = "",
    source_file: str = "",
    notes: str = "",
    sequence_key: int = 0,
) -> PositionEvent:
    return PositionEvent(
        event_type=EVENT_TYPE_AUSTRIAN_BASIS_RESET,
        broker=broker,
        ticker=ticker,
        isin=isin,
        currency=currency,
        asset_class=asset_class,
        event_date=event_date,
        effective_date=event_date,
        quantity=round_qty(quantity),
        quantity_delta=round_qty(quantity),
        base_cost_delta_eur=round_money(base_cost_total_eur),
        basis_adjustment_delta_eur=round_money(basis_adjustment_total_eur),
        basis_method=basis_method,
        source_file=source_file,
        notes=notes,
        sequence_key=sequence_key,
    )


def build_basis_adjustment_event(
    *,
    broker: str,
    ticker: str,
    isin: str,
    currency: str,
    asset_class: str,
    eligibility_date: date,
    effective_date: date,
    basis_adjustment_eur: float,
    quantity: float,
    source_id: str,
    source_file: str,
    notes: str = "",
    sequence_key: int = 0,
) -> PositionEvent:
    return PositionEvent(
        event_type=EVENT_TYPE_OEKB_BASIS_ADJUSTMENT,
        broker=broker,
        ticker=ticker,
        isin=isin,
        currency=currency,
        asset_class=asset_class,
        event_date=effective_date,
        effective_date=effective_date,
        eligibility_date=eligibility_date,
        quantity=round_qty(quantity),
        basis_adjustment_delta_eur=round_money(basis_adjustment_eur),
        source_id=source_id,
        source_file=source_file,
        notes=notes,
        sequence_key=sequence_key,
    )


def build_split_event(
    *,
    broker: str,
    ticker: str,
    isin: str,
    currency: str,
    asset_class: str,
    event_date: date,
    split_ratio: float,
    source_id: str,
    source_file: str,
    notes: str = "",
    reverse: bool = False,
    sequence_key: int = 0,
) -> PositionEvent:
    event_type = EVENT_TYPE_REVERSE_SPLIT if reverse else EVENT_TYPE_SPLIT
    return PositionEvent(
        event_type=event_type,
        broker=broker,
        ticker=ticker,
        isin=isin,
        currency=currency,
        asset_class=asset_class,
        event_date=event_date,
        effective_date=event_date,
        split_ratio=float(split_ratio),
        source_id=source_id,
        source_file=source_file,
        notes=notes,
        sequence_key=sequence_key,
    )


def state_from_record(row: dict[str, object]) -> PositionState:
    if "quantity" in row and "base_cost_total_eur" in row:
        return PositionState(
            broker=str(row.get("broker") or "ibkr"),
            ticker=str(row["ticker"]),
            isin=str(row["isin"]),
            currency=str(row["currency"]),
            asset_class=str(row.get("asset_class") or ""),
            quantity=round_qty(float(row.get("quantity") or 0.0)),
            base_cost_total_eur=round_money(float(row.get("base_cost_total_eur") or 0.0)),
            basis_adjustment_total_eur=round_money(float(row.get("basis_adjustment_total_eur") or 0.0)),
            last_event_date=str(row.get("last_event_date") or ""),
            basis_method=str(row.get("basis_method") or ""),
            snapshot_date=str(row.get("snapshot_date") or ""),
            source_file=str(row.get("source_file") or ""),
            notes=str(row.get("notes") or ""),
        )

    # Legacy lot-based snapshots are aggregated into a single moving-average state.
    return PositionState(
        broker=str(row.get("broker") or "ibkr"),
        ticker=str(row["ticker"]),
        isin=str(row["isin"]),
        currency=str(row["currency"]),
        asset_class=str(row.get("asset_class") or ""),
        quantity=round_qty(float(row.get("remaining_quantity") or 0.0)),
        base_cost_total_eur=round_money(float(row.get("original_cost_eur") or 0.0)),
        basis_adjustment_total_eur=round_money(float(row.get("cumulative_oekb_stepup_eur") or 0.0)),
        last_event_date=str(row.get("last_sale_date") or row.get("last_event_date") or ""),
        basis_method=str(row.get("austrian_basis_method") or row.get("basis_method") or ""),
        snapshot_date=str(row.get("snapshot_date") or ""),
        source_file=str(row.get("source_statement_file") or row.get("source_file") or ""),
        notes=str(row.get("notes") or ""),
    )


def aggregate_state_rows(rows: Iterable[dict[str, object]]) -> list[PositionState]:
    aggregated: dict[str, PositionState] = {}
    for row in rows:
        state = state_from_record(row)
        if state.quantity == 0 and state.base_cost_total_eur == 0 and state.basis_adjustment_total_eur == 0:
            continue
        key = position_key(broker=state.broker, isin=state.isin)
        existing = aggregated.get(key)
        if existing is None:
            aggregated[key] = state
            continue
        existing.quantity = round_qty(existing.quantity + state.quantity)
        existing.base_cost_total_eur = round_money(existing.base_cost_total_eur + state.base_cost_total_eur)
        existing.basis_adjustment_total_eur = round_money(
            existing.basis_adjustment_total_eur + state.basis_adjustment_total_eur
        )
        existing.last_event_date = max(existing.last_event_date, state.last_event_date)
        existing.asset_class = existing.asset_class or state.asset_class
        existing.basis_method = existing.basis_method or state.basis_method
        existing.snapshot_date = existing.snapshot_date or state.snapshot_date
        existing.source_file = existing.source_file or state.source_file
        existing.notes = _append_unique_note_text(existing.notes, state.notes)
    return sorted(aggregated.values(), key=lambda item: (item.asset_class, item.ticker, item.isin))


def load_position_states(path: str | Path) -> list[PositionState]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    df = pl.read_csv(path)
    return aggregate_state_rows(df.to_dicts())


def position_states_to_df(states: Iterable[PositionState]) -> pl.DataFrame:
    rows = [state.to_record() for state in states]
    if not rows:
        return pl.DataFrame(
            schema={
                "broker": pl.String,
                "ticker": pl.String,
                "isin": pl.String,
                "currency": pl.String,
                "asset_class": pl.String,
                "quantity": pl.Float64,
                "base_cost_total_eur": pl.Float64,
                "basis_adjustment_total_eur": pl.Float64,
                "total_basis_eur": pl.Float64,
                "average_basis_eur": pl.Float64,
                "status": pl.String,
                "last_event_date": pl.String,
                "basis_method": pl.String,
                "source_file": pl.String,
                "notes": pl.String,
            }
        )
    return pl.DataFrame(rows).sort(["asset_class", "ticker", "isin"])


def position_events_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "broker": pl.String,
                "ticker": pl.String,
                "isin": pl.String,
                "currency": pl.String,
                "asset_class": pl.String,
                "event_type": pl.String,
                "event_date": pl.String,
                "effective_date": pl.String,
                "eligibility_date": pl.String,
                "source_id": pl.String,
                "source_file": pl.String,
                "sequence_key": pl.Int64,
                "quantity": pl.Float64,
                "quantity_delta": pl.Float64,
                "price_ccy": pl.Float64,
                "fx_to_eur": pl.Float64,
                "proceeds_eur": pl.Float64,
                "base_cost_delta_eur": pl.Float64,
                "basis_adjustment_delta_eur": pl.Float64,
                "realized_basis_eur": pl.Float64,
                "realized_gain_loss_eur": pl.Float64,
                "realized_base_cost_eur": pl.Float64,
                "realized_oekb_adjustment_eur": pl.Float64,
                "split_ratio": pl.Float64,
                "quantity_after": pl.Float64,
                "base_cost_total_eur_after": pl.Float64,
                "basis_adjustment_total_eur_after": pl.Float64,
                "total_basis_eur_after": pl.Float64,
                "average_basis_eur_after": pl.Float64,
                "notes": pl.String,
            }
        )
    return pl.DataFrame(rows).sort(["isin", "effective_date", "sequence_key", "event_type"])


def clone_states(states: Iterable[PositionState]) -> dict[str, PositionState]:
    cloned: dict[str, PositionState] = {}
    for state in states:
        cloned[position_key(broker=state.broker, isin=state.isin)] = PositionState(
            broker=state.broker,
            ticker=state.ticker,
            isin=state.isin,
            currency=state.currency,
            asset_class=state.asset_class,
            quantity=round_qty(state.quantity),
            base_cost_total_eur=round_money(state.base_cost_total_eur),
            basis_adjustment_total_eur=round_money(state.basis_adjustment_total_eur),
            last_event_date=state.last_event_date,
            basis_method=state.basis_method,
            snapshot_date=state.snapshot_date,
            source_file=state.source_file,
            notes=state.notes,
        )
    return cloned


def sort_position_events(events: Iterable[PositionEvent]) -> list[PositionEvent]:
    return sorted(
        events,
        key=lambda event: (
            event.broker,
            event.isin,
            event.effective_date,
            event.sequence_key,
            event.event_type,
            event.source_id,
        ),
    )


def _ensure_state_for_event(states: dict[str, PositionState], event: PositionEvent) -> PositionState:
    key = position_key(broker=event.broker, isin=event.isin)
    if key not in states:
        states[key] = PositionState(
            broker=event.broker,
            ticker=event.ticker,
            isin=event.isin,
            currency=event.currency,
            asset_class=event.asset_class,
            basis_method=event.basis_method,
            source_file=event.source_file,
        )
    return states[key]


def apply_event(states: dict[str, PositionState], event: PositionEvent) -> EventApplicationResult:
    state = _ensure_state_for_event(states, event)
    sale_record: dict[str, object] | None = None

    if event.event_type == EVENT_TYPE_AUSTRIAN_BASIS_RESET:
        if state.quantity != 0 or state.total_basis_eur != 0:
            raise ValueError(f"Cannot apply austrian basis reset to non-empty position {state.ticker} ({state.isin})")
        state.quantity = round_qty(event.quantity)
        state.base_cost_total_eur = round_money(event.base_cost_delta_eur)
        state.basis_adjustment_total_eur = round_money(event.basis_adjustment_delta_eur)
        state.basis_method = event.basis_method
        state.snapshot_date = event.event_date.isoformat()
    elif event.event_type == EVENT_TYPE_BUY:
        state.quantity = round_qty(state.quantity + event.quantity)
        state.base_cost_total_eur = round_money(state.base_cost_total_eur + event.base_cost_delta_eur)
    elif event.event_type == EVENT_TYPE_SELL:
        if event.quantity <= 0:
            raise ValueError("Sell quantity must be positive")
        quantity_before = state.quantity
        if quantity_before <= 0 or quantity_before < event.quantity:
            raise ValueError(f"Sell of {event.ticker} on {event.event_date} exceeds available quantity")
        realized_base = round_money(state.base_cost_total_eur * (event.quantity / quantity_before))
        realized_adjustment = round_money(state.basis_adjustment_total_eur * (event.quantity / quantity_before))
        realized_basis = round_money(realized_base + realized_adjustment)
        state.quantity = round_qty(state.quantity - event.quantity)
        state.base_cost_total_eur = round_money(state.base_cost_total_eur - realized_base)
        state.basis_adjustment_total_eur = round_money(state.basis_adjustment_total_eur - realized_adjustment)
        if state.quantity == 0:
            state.base_cost_total_eur = 0.0
            state.basis_adjustment_total_eur = 0.0

        sale_record = {
            "sale_date": event.event_date.isoformat(),
            "ticker": event.ticker,
            "isin": event.isin,
            "quantity_sold": round_qty(event.quantity),
            "sale_price_ccy": round_money(event.price_ccy or 0.0),
            "sale_fx": round_money(event.fx_to_eur or 0.0),
            "taxable_proceeds_eur": round_money(event.proceeds_eur),
            "realized_base_cost_eur": realized_base,
            "taxable_original_basis_eur": realized_base,
            "realized_oekb_adjustment_eur": realized_adjustment,
            "taxable_stepup_basis_eur": realized_adjustment,
            "taxable_total_basis_eur": realized_basis,
            "taxable_gain_loss_eur": round_money(event.proceeds_eur - realized_basis),
            "notes": event.notes,
            "sale_trade_id": event.source_id,
        }
        event = PositionEvent(
            **{**event.__dict__,
               "base_cost_delta_eur": round_money(-realized_base),
               "basis_adjustment_delta_eur": round_money(-realized_adjustment),
               "realized_basis_eur": realized_basis,
               "realized_gain_loss_eur": round_money(event.proceeds_eur - realized_basis),
               "realized_base_cost_eur": realized_base,
               "realized_oekb_adjustment_eur": realized_adjustment}
        )
    elif event.event_type == EVENT_TYPE_OEKB_BASIS_ADJUSTMENT:
        state.basis_adjustment_total_eur = round_money(
            state.basis_adjustment_total_eur + event.basis_adjustment_delta_eur
        )
    elif event.event_type in {EVENT_TYPE_SPLIT, EVENT_TYPE_REVERSE_SPLIT}:
        if event.split_ratio in (None, 0):
            raise ValueError("Split event requires non-zero split_ratio")
        state.quantity = round_qty(state.quantity * float(event.split_ratio))
    elif event.event_type == EVENT_TYPE_MANUAL_CORRECTION:
        state.quantity = round_qty(state.quantity + event.quantity_delta)
        state.base_cost_total_eur = round_money(state.base_cost_total_eur + event.base_cost_delta_eur)
        state.basis_adjustment_total_eur = round_money(
            state.basis_adjustment_total_eur + event.basis_adjustment_delta_eur
        )
    else:
        raise ValueError(f"Unsupported position event type: {event.event_type}")

    state.last_event_date = event.effective_date.isoformat()
    state.asset_class = state.asset_class or event.asset_class
    state.source_file = state.source_file or event.source_file
    add_state_note(state, event.notes)

    event_record = {
        "broker": event.broker,
        "ticker": event.ticker,
        "isin": event.isin,
        "currency": event.currency,
        "asset_class": event.asset_class,
        "event_type": event.event_type,
        "event_date": event.event_date.isoformat(),
        "effective_date": event.effective_date.isoformat(),
        "eligibility_date": event.eligibility_date.isoformat() if event.eligibility_date else "",
        "source_id": event.source_id,
        "source_file": event.source_file,
        "sequence_key": event.sequence_key,
        "quantity": round_qty(event.quantity),
        "quantity_delta": round_qty(event.quantity_delta),
        "price_ccy": round_money(event.price_ccy or 0.0) if event.price_ccy is not None else None,
        "fx_to_eur": round_money(event.fx_to_eur or 0.0) if event.fx_to_eur is not None else None,
        "proceeds_eur": round_money(event.proceeds_eur),
        "base_cost_delta_eur": round_money(event.base_cost_delta_eur),
        "basis_adjustment_delta_eur": round_money(event.basis_adjustment_delta_eur),
        "realized_basis_eur": round_money(event.realized_basis_eur),
        "realized_gain_loss_eur": round_money(event.realized_gain_loss_eur),
        "realized_base_cost_eur": round_money(event.realized_base_cost_eur),
        "realized_oekb_adjustment_eur": round_money(event.realized_oekb_adjustment_eur),
        "split_ratio": float(event.split_ratio) if event.split_ratio is not None else None,
        "quantity_after": round_qty(state.quantity),
        "base_cost_total_eur_after": round_money(state.base_cost_total_eur),
        "basis_adjustment_total_eur_after": round_money(state.basis_adjustment_total_eur),
        "total_basis_eur_after": state.total_basis_eur,
        "average_basis_eur_after": state.average_basis_eur,
        "notes": event.notes,
    }
    return EventApplicationResult(event_record=event_record, sale_record=sale_record)


def replay_events(
    opening_states: Iterable[PositionState],
    events: Iterable[PositionEvent],
) -> tuple[list[PositionState], list[dict[str, object]], list[dict[str, object]]]:
    states = clone_states(opening_states)
    event_rows: list[dict[str, object]] = []
    sale_rows: list[dict[str, object]] = []
    for event in sort_position_events(events):
        result = apply_event(states, event)
        event_rows.append(result.event_record)
        if result.sale_record is not None:
            sale_rows.append(result.sale_record)
    final_states = sorted(states.values(), key=lambda item: (item.asset_class, item.ticker, item.isin))
    return final_states, event_rows, sale_rows
