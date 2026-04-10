from __future__ import annotations

import glob
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import lxml.etree as etree
import polars as pl

from scripts.reporting_funds.models import IbkrTrade, round_money, round_qty
from scripts.reporting_funds.workflow import build_fx_table, get_fx_rate
from tax_automation.moving_average import build_buy_event, build_sell_event, replay_events
from tax_automation.broker_history import RawBrokerTrade, load_ibkr_stock_like_trades

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
    quantity: float


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
                "quantity": holding.quantity,
                "cutoff_date": cutoff_date.isoformat(),
                "price_ccy": "",
            }
            for holding in holdings
        ]
    ).write_csv(template_path)
    return template_path


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

    replayable_events = []
    for basis_trade in basis_trades:
        trade_fx = get_fx_rate(fx_table, basis_trade.trade.currency, basis_trade.trade.trade_date)
        if basis_trade.trade.operation == "buy":
            replayable_events.append(
                build_buy_event(
                    broker="ibkr",
                    ticker=basis_trade.trade.ticker,
                    isin=basis_trade.trade.isin,
                    currency=basis_trade.trade.currency,
                    asset_class=basis_trade.asset_class,
                    trade_date=basis_trade.trade.trade_date,
                    quantity=basis_trade.trade.quantity,
                    price_ccy=basis_trade.trade.price_ccy,
                    fx_to_eur=trade_fx,
                    source_id=basis_trade.trade.trade_id,
                    source_file=basis_trade.trade.source_statement_file,
                    sequence_key=len(replayable_events),
                )
            )
            continue
        replayable_events.append(
            build_sell_event(
                broker="ibkr",
                ticker=basis_trade.trade.ticker,
                isin=basis_trade.trade.isin,
                currency=basis_trade.trade.currency,
                asset_class=basis_trade.asset_class,
                trade_date=basis_trade.trade.trade_date,
                quantity=basis_trade.trade.quantity,
                price_ccy=basis_trade.trade.price_ccy,
                fx_to_eur=trade_fx,
                source_id=basis_trade.trade.trade_id,
                source_file=basis_trade.trade.source_statement_file,
                sequence_key=len(replayable_events),
            )
        )

    states, _, _ = replay_events([], replayable_events)
    asset_class_by_position = {(state.ticker, state.isin, state.currency): state.asset_class for state in states}
    holdings = [
        SnapshotHolding(
            ticker=state.ticker,
            isin=state.isin,
            asset_class=state.asset_class,
            currency=state.currency,
            quantity=round_qty(state.quantity),
        )
        for state in states
        if state.quantity > 0
    ]
    holdings.sort(key=lambda item: (item.asset_class, item.ticker, item.isin))
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

    for state in states:
        if state.quantity <= 0:
            continue
        try:
            basis_price_ccy, basis_currency = _lookup_price(price_rows, isin=state.isin, ticker=state.ticker)
        except ValueError:
            missing_holdings.append(
                SnapshotHolding(
                    ticker=state.ticker,
                    isin=state.isin,
                    asset_class=state.asset_class,
                    currency=state.currency,
                    quantity=round_qty(state.quantity),
                )
            )
            continue
        if basis_currency != state.currency:
            raise ValueError(
                f"Move-in price currency {basis_currency} does not match position currency {state.currency} for {state.ticker} ({state.isin})"
            )
        basis_fx = get_fx_rate(fx_table, basis_currency, cutoff_date)
        original_quantity = round_qty(state.quantity)
        original_cost_eur = round_money((original_quantity * basis_price_ccy) / basis_fx)
        notes = (
            f"Bootstrap Austrian opening position as of {cutoff_date.isoformat()} from pre-cutoff broker history"
        )
        snapshot_rows.append(
            {
                "snapshot_date": cutoff_date.isoformat(),
                "broker": "ibkr",
                "ticker": state.ticker,
                "isin": state.isin,
                "currency": state.currency,
                "asset_class": asset_class_by_position.get((state.ticker, state.isin, state.currency), ""),
                "quantity": original_quantity,
                "base_cost_total_eur": original_cost_eur,
                "basis_adjustment_total_eur": 0.0,
                "total_basis_eur": original_cost_eur,
                "average_basis_eur": round_money(original_cost_eur / original_quantity) if original_quantity else 0.0,
                "status": "open",
                "last_event_date": cutoff_date.isoformat(),
                "basis_method": "move_in_fmv_reset",
                "notes": notes,
                "source_file": state.source_file,
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
    pl.DataFrame(snapshot_rows).sort(["asset_class", "ticker", "isin"]).write_csv(output_file_path)
    return output_file_path
