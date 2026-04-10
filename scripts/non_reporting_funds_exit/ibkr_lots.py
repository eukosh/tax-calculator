from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from scripts.non_reporting_funds_exit.freedom_lots import NormalizedTrade
from scripts.non_reporting_funds_exit.workflow import Lot, round_money, round_qty
from tax_automation.broker_history import load_ibkr_stock_like_trades

IBKR_REIT_TICKERS = ("CHCT", "CTRE", "MPW", "O")


def _load_lots_from_opening_state_rows(
    rows: list[dict[str, str]],
    *,
    target_tickers: tuple[str, ...],
    asset_class_filter: str,
) -> list[Lot]:
    lots: list[Lot] = []
    for row in rows:
        ticker = row["ticker"].strip()
        asset_class = row["asset_class"].strip()
        if asset_class != asset_class_filter or ticker not in target_tickers:
            continue

        snapshot_date = datetime.strptime(row["snapshot_date"].strip(), "%Y-%m-%d").date()
        quantity = float(row["quantity"])
        total_basis_eur = float(row["total_basis_eur"])

        lots.append(
            Lot(
                ticker=ticker,
                isin=row["isin"].strip(),
                lot_id=f"{ticker}:opening:{snapshot_date.isoformat()}",
                buy_date=snapshot_date,
                original_quantity=round_qty(quantity),
                remaining_quantity=round_qty(quantity),
                trade_currency=row["currency"].strip(),
                buy_price_ccy=0.0,
                buy_commission_ccy=0.0,
                total_cost_ccy=0.0,
                buy_fx=0.0,
                original_cost_eur=round_money(total_basis_eur),
                source_statement_file=row.get("source_file", "").strip(),
                notes=row.get("notes", "").strip() or f"FMV reset opening lot as of {snapshot_date.isoformat()}",
            )
        )
    return lots


def _load_lots_from_working_ledger_rows(
    rows: list[dict[str, str]],
    *,
    target_tickers: tuple[str, ...],
) -> list[Lot]:
    lots: list[Lot] = []
    for row in rows:
        ticker = row["ticker"].strip()
        if ticker not in target_tickers:
            continue

        remaining_quantity = float(row["remaining_quantity"])
        if remaining_quantity <= 0:
            continue

        buy_date = datetime.strptime(row["buy_date"].strip(), "%Y-%m-%d").date()
        lots.append(
            Lot(
                ticker=ticker,
                isin=row["isin"].strip(),
                lot_id=row["lot_id"].strip(),
                buy_date=buy_date,
                # Carry forward the unsold fraction as the new opening lot.
                original_quantity=round_qty(remaining_quantity),
                remaining_quantity=round_qty(remaining_quantity),
                trade_currency=row["trade_currency"].strip(),
                buy_price_ccy=round_money(float(row["buy_price_ccy"])),
                buy_commission_ccy=round_money(float(row["buy_commission_ccy"])),
                total_cost_ccy=round_money(float(row["total_cost_ccy"])),
                buy_fx=round_money(float(row["buy_fx"])),
                original_cost_eur=round_money(float(row["original_cost_eur"])),
                cumulative_stepup_eur=round_money(float(row.get("cumulative_stepup_eur") or 0.0)),
                status="open",
                source_trade_id=row.get("source_trade_id", "").strip(),
                source_statement_file=row.get("source_statement_file", "").strip(),
                notes=row.get("notes", "").strip(),
                last_adjustment_year=row.get("last_adjustment_year", "").strip(),
                last_adjustment_type=row.get("last_adjustment_type", "").strip(),
                last_adjustment_amount_eur=round_money(float(row.get("last_adjustment_amount_eur") or 0.0)),
            )
        )
    return lots


def load_opening_lots(
    opening_state_path: str | Path,
    target_tickers: tuple[str, ...] = IBKR_REIT_TICKERS,
    asset_class_filter: str = "REIT",
) -> list[Lot]:
    """Load REIT lots from the IBKR Austrian opening state CSV.

    Supported inputs:
    - Austrian opening-state CSV (move-in FMV reset)
    - prior `ibkr_reit_working_ledger.csv` for year-over-year carryforward
    """
    with Path(opening_state_path).open() as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        return []

    row_keys = set(rows[0].keys())
    if {"snapshot_date", "asset_class", "quantity", "total_basis_eur"}.issubset(row_keys):
        lots = _load_lots_from_opening_state_rows(
            rows, target_tickers=target_tickers, asset_class_filter=asset_class_filter,
        )
    elif {"lot_id", "buy_date", "remaining_quantity", "original_cost_eur", "cumulative_stepup_eur"}.issubset(row_keys):
        lots = _load_lots_from_working_ledger_rows(rows, target_tickers=target_tickers)
    else:
        raise ValueError(
            f"Unsupported opening-lots schema at {opening_state_path}. "
            "Expected Austrian opening state CSV or prior ibkr_reit_working_ledger.csv"
        )

    return sorted(lots, key=lambda lot: (lot.ticker, lot.lot_id))


def load_ibkr_reit_trades(
    xml_file_path: str | Path,
    target_tickers: tuple[str, ...] = IBKR_REIT_TICKERS,
    after_date: date | None = None,
) -> list[NormalizedTrade]:
    """Load IBKR REIT trades from XML and convert to NormalizedTrade.

    Uses load_ibkr_stock_like_trades() from src/broker_history.py with
    allowed_asset_classes={"REIT"}, filters to target tickers, and
    optionally filters to trades strictly after after_date.
    """
    raw_trades = load_ibkr_stock_like_trades(
        str(xml_file_path),
        allowed_asset_classes={"REIT"},
    )

    normalized: list[NormalizedTrade] = []
    for raw in raw_trades:
        if raw.ticker not in target_tickers:
            continue
        if after_date is not None and raw.trade_date <= after_date:
            continue

        normalized.append(
            NormalizedTrade(
                trade_datetime=raw.trade_datetime,
                trade_date=raw.trade_date,
                ticker=raw.ticker,
                isin=raw.isin,
                operation=raw.operation,
                quantity=Decimal(str(raw.quantity)),
                price_ccy=Decimal(str(raw.price_ccy)),
                gross_amount_ccy=Decimal(str(raw.quantity * raw.price_ccy)),
                commission_ccy=Decimal(str(raw.fee_ccy)),
                trade_currency=raw.currency,
                trade_id=raw.trade_id,
            )
        )

    return sorted(normalized, key=lambda trade: (trade.trade_datetime, trade.trade_id))
