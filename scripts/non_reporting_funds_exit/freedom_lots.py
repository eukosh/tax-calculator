from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

TARGET_TICKERS = ("SCHD.US", "TLT.US")


def _to_decimal(value: object) -> Decimal:
    if value in (None, "", "-"):
        return Decimal("0")
    return Decimal(str(value))


@dataclass(frozen=True)
class NormalizedTrade:
    trade_datetime: datetime
    trade_date: date
    ticker: str
    isin: str
    operation: str
    quantity: Decimal
    price_ccy: Decimal
    gross_amount_ccy: Decimal
    commission_ccy: Decimal
    trade_currency: str
    trade_id: str


@dataclass(frozen=True)
class SplitEvent:
    event_date: date
    ticker: str
    isin: str
    factor: Decimal
    corporate_action_id: str


def load_statement(statement_path: str | Path) -> dict:
    with Path(statement_path).open() as handle:
        return json.load(handle)


def load_target_trades(statement_path: str | Path, tickers: tuple[str, ...] = TARGET_TICKERS) -> list[NormalizedTrade]:
    statement = load_statement(statement_path)
    trades_raw = ((statement.get("trades") or {}).get("detailed") or [])
    normalized: list[NormalizedTrade] = []
    for row in trades_raw:
        ticker = str(row.get("instr_nm") or "").strip()
        operation = str(row.get("operation") or "").strip().lower()
        if ticker not in tickers or operation not in {"buy", "sell"}:
            continue
        normalized.append(
            NormalizedTrade(
                trade_datetime=datetime.strptime(str(row["date"]), "%Y-%m-%d %H:%M:%S"),
                trade_date=datetime.strptime(str(row["short_date"]), "%Y-%m-%d").date(),
                ticker=ticker,
                isin=str(row.get("isin") or "").strip(),
                operation=operation,
                quantity=_to_decimal(row.get("q")).copy_abs(),
                price_ccy=_to_decimal(row.get("p")),
                gross_amount_ccy=_to_decimal(row.get("summ")).copy_abs(),
                commission_ccy=_to_decimal(row.get("commission")).copy_abs(),
                trade_currency=str(row.get("curr_c") or "").strip(),
                trade_id=str(row.get("trade_id") or row.get("id") or "").strip(),
            )
        )
    return sorted(normalized, key=lambda trade: (trade.trade_datetime, trade.trade_id))


def load_split_events(statement_path: str | Path, tickers: tuple[str, ...] = TARGET_TICKERS) -> list[SplitEvent]:
    statement = load_statement(statement_path)
    actions = ((statement.get("corporate_actions") or {}).get("detailed") or [])
    grouped: dict[tuple[str, str, str, str], list[dict]] = {}
    for row in actions:
        if str(row.get("type_id") or "").strip() != "split":
            continue
        ticker = str(row.get("ticker") or "").strip()
        if ticker not in tickers:
            continue
        key = (
            ticker,
            str(row.get("isin") or "").strip(),
            str(row.get("ex_date") or "").strip(),
            str(row.get("corporate_action_id") or "").strip(),
        )
        grouped.setdefault(key, []).append(row)

    split_events: list[SplitEvent] = []
    for (ticker, isin, ex_date_raw, corporate_action_id), rows in grouped.items():
        positive_quantity = sum(_to_decimal(row.get("amount")) for row in rows if _to_decimal(row.get("amount")) > 0)
        negative_quantity = sum((-_to_decimal(row.get("amount"))) for row in rows if _to_decimal(row.get("amount")) < 0)
        if positive_quantity <= 0 or negative_quantity <= 0:
            raise ValueError(f"Split event {corporate_action_id} has invalid quantity legs")
        split_events.append(
            SplitEvent(
                event_date=datetime.strptime(ex_date_raw, "%Y-%m-%d").date(),
                ticker=ticker,
                isin=isin,
                factor=positive_quantity / negative_quantity,
                corporate_action_id=corporate_action_id,
            )
        )
    return sorted(split_events, key=lambda event: (event.event_date, event.corporate_action_id))
