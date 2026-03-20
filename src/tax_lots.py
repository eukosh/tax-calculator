from __future__ import annotations

import glob
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import lxml.etree as etree
import polars as pl

from src.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET

MONEY_DIGITS = 6
QTY_DIGITS = 8
RAW_IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def round_money(value: float) -> float:
    return round(float(value), MONEY_DIGITS)


def round_qty(value: float) -> float:
    return round(float(value), QTY_DIGITS)


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


@dataclass(frozen=True)
class RawBrokerTrade:
    ticker: str
    isin: str
    trade_date: date
    trade_datetime: datetime
    operation: str
    quantity: float
    price_ccy: float
    currency: str
    trade_id: str
    account_id: str = ""
    source_statement_file: str = ""
    asset_class: str = ""
    net_cash_ccy: float | None = None
    fee_ccy: float = 0.0


@dataclass
class TaxLot:
    ticker: str
    isin: str
    lot_id: str
    buy_date: date
    original_quantity: float
    remaining_quantity: float
    currency: str
    buy_price_ccy: float
    buy_fx_to_eur: float
    original_cost_eur: float
    initial_original_cost_eur: float | None = None
    buy_fee_eur_total: float = 0.0
    cumulative_oekb_stepup_eur: float = 0.0
    status: str = "open"
    broker: str = "ibkr"
    account_id: str = ""
    notes: str = ""
    last_adjustment_year: str = ""
    last_adjustment_reference: str = ""
    last_sale_date: str = ""
    sold_quantity_ytd: float = 0.0
    source_trade_id: str = ""
    source_statement_file: str = ""
    snapshot_date: str = ""
    asset_class: str = ""
    broker_buy_date: str = ""
    broker_buy_price_ccy: float | None = None
    broker_buy_fx_to_eur: float | None = None
    broker_original_cost_eur: float | None = None
    broker_buy_fee_eur: float | None = None
    austrian_basis_method: str = ""
    austrian_basis_price_ccy: float | None = None
    austrian_basis_fx_to_eur: float | None = None
    basis_origin: str = ""
    buy_datetime: datetime | None = None

    @property
    def adjusted_basis_eur(self) -> float:
        return round_money(self.original_cost_eur + self.cumulative_oekb_stepup_eur)

    def add_note(self, note: str) -> None:
        if not note:
            return
        self.notes = _append_unique_note_text(self.notes, note)

    def to_record(self, *, include_provenance: bool = True) -> dict[str, object]:
        record = {
            "ticker": self.ticker,
            "isin": self.isin,
            "lot_id": self.lot_id,
            "buy_date": self.buy_date.isoformat(),
            "original_quantity": round_qty(self.original_quantity),
            "remaining_quantity": round_qty(self.remaining_quantity),
            "currency": self.currency,
            "buy_price_ccy": round_money(self.buy_price_ccy),
            "buy_fx_to_eur": round_money(self.buy_fx_to_eur),
            "original_cost_eur": round_money(self.original_cost_eur),
            "initial_original_cost_eur": round_money(
                self.initial_original_cost_eur if self.initial_original_cost_eur is not None else self.original_cost_eur
            ),
            "buy_fee_eur_total": round_money(self.buy_fee_eur_total),
            "cumulative_oekb_stepup_eur": round_money(self.cumulative_oekb_stepup_eur),
            "adjusted_basis_eur": self.adjusted_basis_eur,
            "status": self.status,
            "broker": self.broker,
            "account_id": self.account_id,
            "notes": self.notes,
            "last_adjustment_year": self.last_adjustment_year,
            "last_adjustment_reference": self.last_adjustment_reference,
            "last_sale_date": self.last_sale_date,
            "sold_quantity_ytd": round_qty(self.sold_quantity_ytd),
            "source_trade_id": self.source_trade_id,
            "source_statement_file": self.source_statement_file,
        }
        if not include_provenance:
            return record

        record.update(
            {
                "snapshot_date": self.snapshot_date,
                "asset_class": self.asset_class,
                "broker_buy_date": self.broker_buy_date,
                "broker_buy_price_ccy": (
                    round_money(self.broker_buy_price_ccy) if self.broker_buy_price_ccy is not None else None
                ),
                "broker_buy_fx_to_eur": (
                    round_money(self.broker_buy_fx_to_eur) if self.broker_buy_fx_to_eur is not None else None
                ),
                "broker_original_cost_eur": (
                    round_money(self.broker_original_cost_eur) if self.broker_original_cost_eur is not None else None
                ),
                "broker_buy_fee_eur": (
                    round_money(self.broker_buy_fee_eur) if self.broker_buy_fee_eur is not None else None
                ),
                "austrian_basis_method": self.austrian_basis_method,
                "austrian_basis_price_ccy": (
                    round_money(self.austrian_basis_price_ccy) if self.austrian_basis_price_ccy is not None else None
                ),
                "austrian_basis_fx_to_eur": (
                    round_money(self.austrian_basis_fx_to_eur) if self.austrian_basis_fx_to_eur is not None else None
                ),
                "basis_origin": self.basis_origin,
                "buy_datetime": self.buy_datetime.isoformat(sep=" ") if self.buy_datetime else "",
            }
        )
        return record


def tax_lots_to_df(lots: Iterable[TaxLot], *, include_provenance: bool = True) -> pl.DataFrame:
    records = [lot.to_record(include_provenance=include_provenance) for lot in lots]
    if records:
        return pl.DataFrame(records).sort(["ticker", "buy_date", "lot_id"])

    empty_columns = [
        "ticker",
        "isin",
        "lot_id",
        "buy_date",
        "original_quantity",
        "remaining_quantity",
        "currency",
        "buy_price_ccy",
        "buy_fx_to_eur",
        "original_cost_eur",
        "initial_original_cost_eur",
        "buy_fee_eur_total",
        "cumulative_oekb_stepup_eur",
        "adjusted_basis_eur",
        "status",
        "broker",
        "account_id",
        "notes",
        "last_adjustment_year",
        "last_adjustment_reference",
        "last_sale_date",
        "sold_quantity_ytd",
        "source_trade_id",
        "source_statement_file",
    ]
    if include_provenance:
        empty_columns.extend(
            [
                "snapshot_date",
                "asset_class",
                "broker_buy_date",
                "broker_buy_price_ccy",
                "broker_buy_fx_to_eur",
                "broker_original_cost_eur",
                "broker_buy_fee_eur",
                "austrian_basis_method",
                "austrian_basis_price_ccy",
                "austrian_basis_fx_to_eur",
                "basis_origin",
                "buy_datetime",
            ]
        )
    return pl.DataFrame({column: [] for column in empty_columns})


def _resolve_file_paths(file_path: str) -> list[str]:
    path = Path(file_path)
    if path.exists():
        if path.is_dir():
            return sorted(str(candidate) for candidate in path.glob("*.xml"))
        return [str(path)]
    return sorted(glob.glob(file_path))


def _parse_trade_datetime(row: etree._Element) -> datetime:
    raw_value = row.get("dateTime") or row.get("tradeDate")
    if raw_value is None:
        raise ValueError("IBKR trade row is missing dateTime/tradeDate")
    if len(raw_value) == 10:
        raw_value = f"{raw_value} 00:00:00"
    return datetime.strptime(raw_value, RAW_IBKR_DATETIME_FORMAT)


def load_ibkr_stock_like_trades(
    xml_file_path: str,
    *,
    allowed_asset_classes: set[str],
    cutoff_date: date | None = None,
) -> list[RawBrokerTrade]:
    file_paths = _resolve_file_paths(xml_file_path)
    if not file_paths:
        raise FileNotFoundError(f"No files matched the pattern: {xml_file_path}")

    seen_rows: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    trades: list[RawBrokerTrade] = []

    for path in file_paths:
        root = etree.parse(path).getroot()
        for parent_tag in (".//TradeConfirms", ".//Trades"):
            for parent in root.findall(parent_tag):
                for row in parent:
                    if row.tag not in {"TradeConfirm", "Trade"}:
                        continue
                    row_key = (row.tag, tuple(sorted(row.items())))
                    if row_key in seen_rows:
                        continue
                    seen_rows.add(row_key)

                    if (row.get("assetCategory") or "").strip() != "STK":
                        continue
                    asset_class = (row.get("subCategory") or "").strip()
                    if asset_class not in allowed_asset_classes:
                        continue

                    buy_sell = (row.get("buySell") or "").strip().upper()
                    if buy_sell not in {"BUY", "SELL"}:
                        continue

                    trade_datetime = _parse_trade_datetime(row)
                    if cutoff_date is not None and trade_datetime.date() >= cutoff_date:
                        continue

                    trade_price = row.get("tradePrice") or row.get("price")
                    quantity = row.get("quantity")
                    currency = row.get("currency")
                    symbol = row.get("symbol")
                    isin = row.get("isin") or row.get("securityID")
                    if not all([trade_price, quantity, currency, symbol, isin]):
                        raise ValueError("IBKR raw stock-like trade row is missing required fields")

                    trade_id = (
                        row.get("tradeID")
                        or row.get("transactionID")
                        or row.get("ibOrderID")
                        or f"{symbol}:{trade_datetime.isoformat()}:{buy_sell}"
                    )

                    trades.append(
                        RawBrokerTrade(
                            ticker=symbol.strip(),
                            isin=isin.strip(),
                            trade_date=trade_datetime.date(),
                            trade_datetime=trade_datetime,
                            operation=buy_sell.lower(),
                            quantity=round_qty(abs(float(quantity))),
                            price_ccy=round_money(float(trade_price)),
                            currency=currency.strip(),
                            trade_id=trade_id.strip(),
                            account_id=(row.get("accountId") or "").strip(),
                            source_statement_file=path,
                            asset_class=asset_class,
                            net_cash_ccy=(
                                round_money(abs(float(row.get("netCash"))))
                                if row.get("netCash") not in (None, "")
                                else None
                            ),
                            fee_ccy=(
                                round_money(abs(float(row.get("commission"))))
                                if row.get("commission") not in (None, "")
                                else round_money(
                                    max(
                                        0.0,
                                        (
                                            abs(float(row.get("netCash")))
                                            if row.get("netCash") not in (None, "")
                                            else abs(float(quantity)) * float(trade_price)
                                        )
                                        - (abs(float(quantity)) * float(trade_price))
                                        if buy_sell == "BUY"
                                        else (abs(float(quantity)) * float(trade_price))
                                        - (
                                            abs(float(row.get("netCash")))
                                            if row.get("netCash") not in (None, "")
                                            else abs(float(quantity)) * float(trade_price)
                                        ),
                                    )
                                )
                            ),
                        )
                    )

    trades.sort(key=lambda item: (item.trade_datetime, item.trade_id, item.operation))
    return trades


def load_opening_tax_lots(
    path: str | Path,
    *,
    allowed_asset_classes: set[str] | None = None,
) -> tuple[list[TaxLot], date]:
    snapshot_df = pl.read_csv(path)
    if snapshot_df.is_empty():
        raise ValueError(f"Opening lot snapshot is empty: {path}")
    if "snapshot_date" not in snapshot_df.columns:
        raise ValueError(f"Opening lot snapshot is missing required column 'snapshot_date': {path}")

    snapshot_dates = {date.fromisoformat(str(value)) for value in snapshot_df["snapshot_date"].to_list()}
    if len(snapshot_dates) != 1:
        raise ValueError(
            f"Opening lot snapshot must contain exactly one snapshot_date value, found {len(snapshot_dates)} in {path}"
        )
    snapshot_date = next(iter(snapshot_dates))

    if allowed_asset_classes is not None and "asset_class" in snapshot_df.columns:
        snapshot_df = snapshot_df.filter(pl.col("asset_class").is_in(sorted(allowed_asset_classes)))
    if snapshot_df.is_empty():
        raise ValueError(f"Opening lot snapshot contains no matching lots after asset-class filtering: {path}")

    lots: list[TaxLot] = []
    for row in snapshot_df.to_dicts():
        original_cost_eur = round_money(float(row["original_cost_eur"]))
        cumulative_stepup_eur = round_money(float(row.get("cumulative_oekb_stepup_eur") or 0.0))
        adjusted_basis_eur = round_money(float(row.get("adjusted_basis_eur") or (original_cost_eur + cumulative_stepup_eur)))
        expected_adjusted_basis_eur = round_money(original_cost_eur + cumulative_stepup_eur)
        if adjusted_basis_eur != expected_adjusted_basis_eur:
            raise ValueError(
                f"Opening lot {row['lot_id']} has inconsistent adjusted_basis_eur {adjusted_basis_eur} "
                f"!= original_cost_eur + cumulative_oekb_stepup_eur {expected_adjusted_basis_eur}"
            )

        lots.append(
            TaxLot(
                ticker=str(row["ticker"]),
                isin=str(row["isin"]),
                lot_id=str(row["lot_id"]),
                buy_date=date.fromisoformat(str(row["buy_date"])),
                original_quantity=round_qty(float(row["original_quantity"])),
                remaining_quantity=round_qty(float(row["remaining_quantity"])),
                currency=str(row["currency"]),
                buy_price_ccy=round_money(float(row["buy_price_ccy"])),
                buy_fx_to_eur=round_money(float(row["buy_fx_to_eur"])),
                original_cost_eur=original_cost_eur,
                initial_original_cost_eur=_optional_money(row.get("initial_original_cost_eur")) or original_cost_eur,
                buy_fee_eur_total=round_money(float(row.get("buy_fee_eur_total") or 0.0)),
                cumulative_oekb_stepup_eur=cumulative_stepup_eur,
                status=str(row.get("status") or "open"),
                broker=str(row.get("broker") or "ibkr"),
                account_id=str(row.get("account_id") or ""),
                notes=str(row.get("notes") or ""),
                last_adjustment_year=str(row.get("last_adjustment_year") or ""),
                last_adjustment_reference=str(row.get("last_adjustment_reference") or ""),
                last_sale_date=str(row.get("last_sale_date") or ""),
                sold_quantity_ytd=round_qty(float(row.get("sold_quantity_ytd") or 0.0)),
                source_trade_id=str(row.get("source_trade_id") or ""),
                source_statement_file=str(row.get("source_statement_file") or ""),
                snapshot_date=str(row.get("snapshot_date") or ""),
                asset_class=str(row.get("asset_class") or ""),
                broker_buy_date=str(row.get("broker_buy_date") or ""),
                broker_buy_price_ccy=_optional_money(row.get("broker_buy_price_ccy")),
                broker_buy_fx_to_eur=_optional_money(row.get("broker_buy_fx_to_eur")),
                broker_original_cost_eur=_optional_money(row.get("broker_original_cost_eur")),
                broker_buy_fee_eur=_optional_money(row.get("broker_buy_fee_eur")),
                austrian_basis_method=str(row.get("austrian_basis_method") or ""),
                austrian_basis_price_ccy=_optional_money(row.get("austrian_basis_price_ccy")),
                austrian_basis_fx_to_eur=_optional_money(row.get("austrian_basis_fx_to_eur")),
                basis_origin="snapshot",
            )
        )

    return lots, snapshot_date


def _optional_money(raw_value: object) -> float | None:
    if raw_value in (None, ""):
        return None
    return round_money(float(raw_value))


def build_tax_lot_from_trade(
    trade: RawBrokerTrade,
    *,
    buy_fx: float,
    basis_origin: str = "post_move_buy",
) -> TaxLot:
    quantity = round_qty(trade.quantity)
    original_cost_eur = round_money((quantity * trade.price_ccy) / buy_fx)
    return TaxLot(
        ticker=trade.ticker,
        isin=trade.isin,
        lot_id=f"{trade.ticker}:{trade.trade_date.isoformat()}:{trade.trade_id}",
        buy_date=trade.trade_date,
        original_quantity=quantity,
        remaining_quantity=quantity,
        currency=trade.currency,
        buy_price_ccy=round_money(trade.price_ccy),
        buy_fx_to_eur=round_money(buy_fx),
        original_cost_eur=original_cost_eur,
        initial_original_cost_eur=original_cost_eur,
        buy_fee_eur_total=round_money((getattr(trade, "fee_ccy", 0.0) or 0.0) / buy_fx),
        account_id=trade.account_id,
        source_trade_id=trade.trade_id,
        source_statement_file=trade.source_statement_file,
        asset_class=trade.asset_class,
        basis_origin=basis_origin,
        buy_datetime=trade.trade_datetime,
    )


def consume_fifo_sell(
    lots: list,
    trade,
    *,
    sale_fx: float,
    track_ytd: bool,
) -> list[dict[str, object]]:
    allocations: list[dict[str, object]] = []
    quantity_left = round_qty(trade.quantity)

    for lot in lots:
        if quantity_left <= 0:
            break
        if lot.isin != trade.isin or lot.remaining_quantity <= 0:
            continue

        quantity_from_lot = min(lot.remaining_quantity, quantity_left)
        fraction = quantity_from_lot / lot.remaining_quantity
        original_basis_eur = round_money(lot.original_cost_eur * fraction)
        stepup_basis_eur = round_money(lot.cumulative_oekb_stepup_eur * fraction)
        taxable_basis_eur = round_money(original_basis_eur + stepup_basis_eur)
        taxable_proceeds_eur = round_money(((trade.quantity * trade.price_ccy) * (quantity_from_lot / trade.quantity)) / sale_fx)
        basis_origin = getattr(lot, "basis_origin", "") or ("snapshot" if getattr(lot, "snapshot_date", "") else "post_move_buy")
        allocated_buy_fee_eur = round_money(
            (getattr(lot, "buy_fee_eur_total", 0.0) or 0.0) * (quantity_from_lot / getattr(lot, "original_quantity", quantity_from_lot))
        )
        allocated_sale_fee_eur = round_money((getattr(trade, "fee_ccy", 0.0) / sale_fx) * (quantity_from_lot / trade.quantity))

        allocations.append(
            {
                "sale_date": trade.trade_date.isoformat(),
                "sale_datetime": trade.trade_datetime.isoformat(sep=" "),
                "sale_trade_id": getattr(trade, "trade_id", ""),
                "ticker": trade.ticker,
                "isin": trade.isin,
                "quantity_sold": round_qty(trade.quantity),
                "sale_price_ccy": round_money(trade.price_ccy),
                "sale_fx": round_money(sale_fx),
                "lot_id": lot.lot_id,
                "lot_buy_date": lot.buy_date.isoformat(),
                "lot_buy_datetime": (
                    lot.buy_datetime.isoformat(sep=" ")
                    if getattr(lot, "buy_datetime", None)
                    else ""
                ),
                "lot_source_trade_id": getattr(lot, "source_trade_id", ""),
                "quantity_from_lot": round_qty(quantity_from_lot),
                "taxable_proceeds_eur": taxable_proceeds_eur,
                "taxable_original_basis_eur": original_basis_eur,
                "taxable_stepup_basis_eur": stepup_basis_eur,
                "taxable_total_basis_eur": taxable_basis_eur,
                "taxable_gain_loss_eur": round_money(taxable_proceeds_eur - taxable_basis_eur),
                "allocated_buy_fee_eur": allocated_buy_fee_eur,
                "allocated_sale_fee_eur": allocated_sale_fee_eur,
                "basis_origin": basis_origin,
            }
        )

        lot.remaining_quantity = round_qty(lot.remaining_quantity - quantity_from_lot)
        lot.original_cost_eur = round_money(lot.original_cost_eur - original_basis_eur)
        lot.cumulative_oekb_stepup_eur = round_money(lot.cumulative_oekb_stepup_eur - stepup_basis_eur)
        lot.status = "closed" if lot.remaining_quantity == 0 else "partially_sold"
        if track_ytd:
            lot.sold_quantity_ytd = round_qty(lot.sold_quantity_ytd + quantity_from_lot)
            lot.last_sale_date = trade.trade_date.isoformat()

        quantity_left = round_qty(quantity_left - quantity_from_lot)

    if quantity_left > 0:
        raise ValueError(f"Sell of {trade.ticker} on {trade.trade_date} exceeds available quantity")

    return allocations


def build_fx_table_from_rates_df(
    rates_df: pl.DataFrame,
    *,
    currencies: Iterable[str],
) -> dict[str, tuple[list[date], list[float]]]:
    fx_table: dict[str, tuple[list[date], list[float]]] = {}
    for currency in sorted({currency for currency in currencies if currency != "EUR"}):
        currency_df = rates_df.filter(pl.col("currency") == currency).sort("rate_date")
        if currency_df.is_empty():
            raise ValueError(f"Missing FX series for currency {currency}")
        fx_table[currency] = (currency_df["rate_date"].to_list(), currency_df["exchange_rate"].to_list())
    return fx_table


def get_fx_rate(fx_table: dict[str, tuple[list[date], list[float]]], currency: str, event_date: date) -> float:
    if currency == "EUR":
        return 1.0

    if currency not in fx_table:
        raise ValueError(f"Missing FX series for currency {currency}")

    available_dates, available_rates = fx_table[currency]
    index = bisect_right(available_dates, event_date) - 1
    if index < 0:
        raise ValueError(f"No FX rate available for {currency} on or before {event_date}")

    matched_date = available_dates[index]
    if (event_date - matched_date).days > EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET:
        raise ValueError(
            f"Closest FX rate for {currency} on {event_date} is too old: {matched_date} "
            f"(>{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET} days)"
        )

    return float(available_rates[index])
