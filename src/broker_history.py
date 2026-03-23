from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import lxml.etree as etree
import polars as pl

from src.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET
from src.utils import resolve_input_file_paths

MONEY_DIGITS = 6
QTY_DIGITS = 8
RAW_IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def round_money(value: float) -> float:
    return round(float(value), MONEY_DIGITS)


def round_qty(value: float) -> float:
    return round(float(value), QTY_DIGITS)


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


def _resolve_file_paths(file_path: str) -> list[str]:
    return resolve_input_file_paths(file_path, suffix=".xml")


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
