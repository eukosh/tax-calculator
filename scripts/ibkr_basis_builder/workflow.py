from __future__ import annotations

import glob
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import lxml.etree as etree
import polars as pl

from scripts.reporting_funds.models import IbkrTrade, Lot, round_money, round_qty
from scripts.reporting_funds.workflow import apply_trade, build_fx_table, get_fx_rate
from src.tax_lots import RawBrokerTrade, load_ibkr_stock_like_trades

RAW_IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SUPPORTED_ASSET_CLASSES = {"ETF", "COMMON", "REIT", "ADR"}


@dataclass(frozen=True)
class BasisTrade:
    trade: IbkrTrade
    asset_class: str
    fee_ccy: float = 0.0


@dataclass(frozen=True)
class SnapshotHolding:
    ticker: str
    isin: str
    asset_class: str
    currency: str
    remaining_quantity: float


def _resolve_file_paths(xml_file_path: str) -> list[str]:
    path = Path(xml_file_path)
    if path.exists():
        if path.is_dir():
            return sorted(str(candidate) for candidate in path.glob("*.xml"))
        return [str(path)]
    return sorted(glob.glob(xml_file_path))


def _parse_trade_datetime(row: etree._Element) -> datetime:
    raw_value = row.get("dateTime") or row.get("tradeDate")
    if raw_value is None:
        raise ValueError("IBKR trade row is missing dateTime/tradeDate")
    if len(raw_value) == 10:
        raw_value = f"{raw_value} 00:00:00"
    return datetime.strptime(raw_value, RAW_IBKR_DATETIME_FORMAT)


def load_ibkr_stock_and_etf_trades(xml_file_path: str, *, cutoff_date: date) -> list[BasisTrade]:
    raw_trades = load_ibkr_stock_like_trades(
        xml_file_path,
        allowed_asset_classes=SUPPORTED_ASSET_CLASSES,
        cutoff_date=cutoff_date,
    )
    trades: list[BasisTrade] = [
        BasisTrade(
            trade=IbkrTrade(
                ticker=trade.ticker,
                isin=trade.isin,
                trade_date=trade.trade_date,
                trade_datetime=trade.trade_datetime,
                operation=trade.operation,
                quantity=trade.quantity,
                price_ccy=trade.price_ccy,
                currency=trade.currency,
                trade_id=trade.trade_id,
                account_id=trade.account_id,
                source_statement_file=trade.source_statement_file,
            ),
            asset_class=trade.asset_class,
            fee_ccy=trade.fee_ccy,
        )
        for trade in raw_trades
    ]
    if not trades:
        raise ValueError("No pre-cutoff IBKR stock/ETF BUY/SELL trades were found.")
    return trades


def _load_price_rows(price_csv_path: str | Path, *, cutoff_date: date) -> dict[tuple[str, str], tuple[float, str]]:
    price_df = pl.read_csv(price_csv_path)
    required_cols = {"cutoff_date", "price_ccy", "currency"}
    if not required_cols.issubset(price_df.columns):
        missing = ", ".join(sorted(required_cols - set(price_df.columns)))
        raise ValueError(f"Move-in price CSV is missing required columns: {missing}")

    prices: dict[tuple[str, str], tuple[float, str]] = {}
    for row in price_df.to_dicts():
        row_cutoff = date.fromisoformat(str(row["cutoff_date"]))
        if row_cutoff != cutoff_date:
            continue
        isin = str(row.get("isin") or "").strip()
        ticker = str(row.get("ticker") or "").strip()
        if not isin and not ticker:
            raise ValueError("Each move-in price CSV row must define at least isin or ticker.")
        price = round_money(float(row["price_ccy"]))
        currency = str(row["currency"]).strip()
        if isin:
            prices[("isin", isin)] = (price, currency)
        if ticker:
            prices[("ticker", ticker)] = (price, currency)

    if not prices:
        raise ValueError(f"No move-in prices were found for cutoff date {cutoff_date.isoformat()}")
    return prices


def _lookup_price(
    price_rows: dict[tuple[str, str], tuple[float, str]],
    *,
    isin: str,
    ticker: str,
) -> tuple[float, str]:
    if ("isin", isin) in price_rows:
        return price_rows[("isin", isin)]
    if ("ticker", ticker) in price_rows:
        return price_rows[("ticker", ticker)]
    raise ValueError(f"Missing move-in price for {ticker} ({isin})")


def _build_snapshot_holdings(lots: list[Lot], asset_class_by_lot_id: dict[str, str]) -> list[SnapshotHolding]:
    aggregated: dict[tuple[str, str, str, str], float] = {}
    for lot in lots:
        if lot.remaining_quantity <= 0:
            continue
        key = (
            lot.ticker,
            lot.isin,
            asset_class_by_lot_id.get(lot.lot_id, ""),
            lot.currency,
        )
        aggregated[key] = round_qty(aggregated.get(key, 0.0) + lot.remaining_quantity)

    holdings = [
        SnapshotHolding(
            ticker=ticker,
            isin=isin,
            asset_class=asset_class,
            currency=currency,
            remaining_quantity=round_qty(quantity),
        )
        for (ticker, isin, asset_class, currency), quantity in aggregated.items()
    ]
    holdings.sort(key=lambda item: (item.asset_class, item.ticker, item.isin))
    return holdings


def write_move_in_price_template(
    path: str | Path,
    *,
    cutoff_date: date,
    holdings: list[SnapshotHolding],
) -> Path:
    template_path = Path(path)
    template_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "ticker": holding.ticker,
                "isin": holding.isin,
                "asset_class": holding.asset_class,
                "currency": holding.currency,
                "remaining_quantity": holding.remaining_quantity,
                "cutoff_date": cutoff_date.isoformat(),
                "price_ccy": "",
            }
            for holding in holdings
        ]
    ).write_csv(template_path)
    return template_path


def _snapshot_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "snapshot_date": pl.String,
        "asset_class": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "lot_id": pl.String,
        "buy_date": pl.String,
        "original_quantity": pl.Float64,
        "remaining_quantity": pl.Float64,
        "currency": pl.String,
        "buy_price_ccy": pl.Float64,
        "buy_fx_to_eur": pl.Float64,
        "original_cost_eur": pl.Float64,
        "buy_fee_eur_total": pl.Float64,
        "cumulative_oekb_stepup_eur": pl.Float64,
        "adjusted_basis_eur": pl.Float64,
        "status": pl.String,
        "broker": pl.String,
        "account_id": pl.String,
        "notes": pl.String,
        "last_adjustment_year": pl.String,
        "last_adjustment_reference": pl.String,
        "last_sale_date": pl.String,
        "sold_quantity_ytd": pl.Float64,
        "source_trade_id": pl.String,
        "source_statement_file": pl.String,
        "broker_buy_date": pl.String,
        "broker_buy_price_ccy": pl.Float64,
        "broker_buy_fx_to_eur": pl.Float64,
        "broker_original_cost_eur": pl.Float64,
        "broker_buy_fee_eur": pl.Float64,
        "austrian_basis_method": pl.String,
        "austrian_basis_price_ccy": pl.Float64,
        "austrian_basis_fx_to_eur": pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows).sort(["asset_class", "ticker", "isin", "lot_id"])


def build_opening_lot_snapshot(
    *,
    person: str,
    cutoff_date: date,
    ibkr_trade_history_path: str | Path,
    raw_exchange_rates_path: str | Path,
    move_in_price_csv_path: str | Path,
    output_path: str | Path,
    move_in_price_template_path: str | Path | None = None,
) -> Path:
    basis_trades = load_ibkr_stock_and_etf_trades(str(ibkr_trade_history_path), cutoff_date=cutoff_date)
    currencies = tuple(
        sorted({basis_trade.trade.currency for basis_trade in basis_trades if basis_trade.trade.currency != "EUR"} or {"USD"})
    )
    fx_table = build_fx_table(
        start_date=min(basis_trade.trade.trade_date for basis_trade in basis_trades),
        end_date=cutoff_date,
        raw_exchange_rates_path=raw_exchange_rates_path,
        currencies=currencies,
    )

    lots: list[Lot] = []
    asset_class_by_lot_id: dict[str, str] = {}
    broker_buy_fee_eur_by_lot_id: dict[str, float] = {}
    for basis_trade in basis_trades:
        apply_trade(lots, basis_trade.trade, fx_table=fx_table, sale_rows=None, track_ytd=False)
        if basis_trade.trade.operation == "buy":
            created_lot = lots[-1]
            asset_class_by_lot_id[created_lot.lot_id] = basis_trade.asset_class
            broker_buy_fee_eur_by_lot_id[created_lot.lot_id] = round_money(basis_trade.fee_ccy / created_lot.buy_fx_to_eur)

    holdings = _build_snapshot_holdings(lots, asset_class_by_lot_id)
    try:
        price_rows = _load_price_rows(move_in_price_csv_path, cutoff_date=cutoff_date)
    except FileNotFoundError as exc:
        if move_in_price_template_path is None:
            raise
        template_path = write_move_in_price_template(
            move_in_price_template_path,
            cutoff_date=cutoff_date,
            holdings=holdings,
        )
        raise ValueError(
            f"Move-in price CSV not found: {move_in_price_csv_path}. "
            f"Wrote template with required holdings to {template_path}"
        ) from exc

    snapshot_rows: list[dict[str, object]] = []
    missing_holdings: list[SnapshotHolding] = []

    for lot in lots:
        if lot.remaining_quantity <= 0:
            continue
        try:
            basis_price_ccy, basis_currency = _lookup_price(price_rows, isin=lot.isin, ticker=lot.ticker)
        except ValueError:
            missing_holdings.append(
                SnapshotHolding(
                    ticker=lot.ticker,
                    isin=lot.isin,
                    asset_class=asset_class_by_lot_id.get(lot.lot_id, ""),
                    currency=lot.currency,
                    remaining_quantity=round_qty(lot.remaining_quantity),
                )
            )
            continue
        if basis_currency != lot.currency:
            raise ValueError(
                f"Move-in price currency {basis_currency} does not match lot currency {lot.currency} for {lot.ticker} ({lot.isin})"
            )
        basis_fx = get_fx_rate(fx_table, basis_currency, cutoff_date)
        original_quantity = round_qty(lot.remaining_quantity)
        original_cost_eur = round_money((original_quantity * basis_price_ccy) / basis_fx)
        notes = (
            f"Bootstrap Austrian opening lot as of {cutoff_date.isoformat()} from pre-cutoff economic lot {lot.lot_id}"
        )
        snapshot_rows.append(
            {
                "snapshot_date": cutoff_date.isoformat(),
                "asset_class": asset_class_by_lot_id.get(lot.lot_id, ""),
                "ticker": lot.ticker,
                "isin": lot.isin,
                "lot_id": lot.lot_id,
                "buy_date": cutoff_date.isoformat(),
                "original_quantity": original_quantity,
                "remaining_quantity": original_quantity,
                "currency": lot.currency,
                "buy_price_ccy": round_money(basis_price_ccy),
                "buy_fx_to_eur": round_money(basis_fx),
                "original_cost_eur": original_cost_eur,
                "buy_fee_eur_total": 0.0,
                "cumulative_oekb_stepup_eur": 0.0,
                "adjusted_basis_eur": original_cost_eur,
                "status": "open",
                "broker": "ibkr",
                "account_id": lot.account_id,
                "notes": notes,
                "last_adjustment_year": "",
                "last_adjustment_reference": "",
                "last_sale_date": "",
                "sold_quantity_ytd": 0.0,
                "source_trade_id": lot.source_trade_id,
                "source_statement_file": lot.source_statement_file,
                "broker_buy_date": lot.buy_date.isoformat(),
                "broker_buy_price_ccy": round_money(lot.buy_price_ccy),
                "broker_buy_fx_to_eur": round_money(lot.buy_fx_to_eur),
                "broker_original_cost_eur": round_money(lot.original_cost_eur),
                "broker_buy_fee_eur": round_money(broker_buy_fee_eur_by_lot_id.get(lot.lot_id, 0.0)),
                "austrian_basis_method": "move_in_fmv_reset",
                "austrian_basis_price_ccy": round_money(basis_price_ccy),
                "austrian_basis_fx_to_eur": round_money(basis_fx),
            }
        )

    if missing_holdings:
        missing_names = ", ".join(f"{holding.ticker} ({holding.isin})" for holding in missing_holdings)
        if move_in_price_template_path is None:
            raise ValueError(f"Missing move-in prices for: {missing_names}")
        template_path = write_move_in_price_template(
            move_in_price_template_path,
            cutoff_date=cutoff_date,
            holdings=holdings,
        )
        raise ValueError(f"Missing move-in prices for: {missing_names}. Wrote template with required holdings to {template_path}")

    output_file_path = Path(output_path)
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    _snapshot_to_df(snapshot_rows).write_csv(output_file_path)
    return output_file_path
