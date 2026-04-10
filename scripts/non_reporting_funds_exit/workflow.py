from __future__ import annotations

import csv
from bisect import bisect_right
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

import polars as pl

from scripts.non_reporting_funds_exit.freedom_lots import TARGET_TICKERS, load_split_events, load_target_trades
from tax_automation.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET
from tax_automation.currencies import ExchangeRates, ExchangeRatesCacheError

MONEY_DIGITS = 6
QTY_DIGITS = 8


def round_money(value: float) -> float:
    return round(float(value), MONEY_DIGITS)


def round_qty(value: float) -> float:
    return round(float(value), QTY_DIGITS)


@dataclass
class Lot:
    ticker: str
    isin: str
    lot_id: str
    buy_date: date
    original_quantity: float
    remaining_quantity: float
    trade_currency: str
    buy_price_ccy: float
    buy_commission_ccy: float
    total_cost_ccy: float
    buy_fx: float
    original_cost_eur: float
    cumulative_stepup_eur: float = 0.0
    status: str = "open"
    source_trade_id: str = ""
    source_statement_file: str = ""
    notes: str = ""
    last_adjustment_year: str = ""
    last_adjustment_type: str = ""
    last_adjustment_amount_eur: float = 0.0

    @property
    def adjusted_basis_eur(self) -> float:
        return round_money(self.original_cost_eur + self.cumulative_stepup_eur)

    def add_note(self, note: str) -> None:
        if not note:
            return
        self.notes = note if not self.notes else f"{self.notes}; {note}"

    def to_record(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "isin": self.isin,
            "lot_id": self.lot_id,
            "buy_date": self.buy_date.isoformat(),
            "original_quantity": round_qty(self.original_quantity),
            "remaining_quantity": round_qty(self.remaining_quantity),
            "trade_currency": self.trade_currency,
            "buy_price_ccy": round_money(self.buy_price_ccy),
            "buy_commission_ccy": round_money(self.buy_commission_ccy),
            "total_cost_ccy": round_money(self.total_cost_ccy),
            "buy_fx": round_money(self.buy_fx),
            "original_cost_eur": round_money(self.original_cost_eur),
            "cumulative_stepup_eur": round_money(self.cumulative_stepup_eur),
            "adjusted_basis_eur": self.adjusted_basis_eur,
            "status": self.status,
            "source_trade_id": self.source_trade_id,
            "source_statement_file": self.source_statement_file,
            "last_adjustment_year": self.last_adjustment_year,
            "last_adjustment_type": self.last_adjustment_type,
            "last_adjustment_amount_eur": round_money(self.last_adjustment_amount_eur),
            "notes": self.notes,
        }


def load_price_rows(
    price_input_path: str | Path,
    tax_year: int,
    target_tickers: tuple[str, ...] | None = None,
) -> dict[str, dict[str, str]]:
    with Path(price_input_path).open() as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("ticker")]

    filtered_rows = {
        row["ticker"].strip(): row
        for row in rows
        if int(row["tax_year"]) == tax_year
        and (target_tickers is None or row["ticker"].strip() in target_tickers)
    }
    if not filtered_rows:
        raise ValueError(f"No supported price rows found for tax year {tax_year}")
    return filtered_rows


def load_sale_rows(sale_plan_path: str | Path | None) -> list[dict[str, str]]:
    if sale_plan_path is None or not Path(sale_plan_path).exists():
        return []
    with Path(sale_plan_path).open() as handle:
        return [row for row in csv.DictReader(handle) if row.get("ticker")]


def build_fx_table(
    start_date: date, end_date: date, raw_exchange_rates_path: str | Path
) -> dict[str, tuple[list[date], list[float]]]:
    try:
        exchange_rates = ExchangeRates(
            start_date=start_date,
            end_date=end_date,
            overwrite=False,
            raw_file_path=str(raw_exchange_rates_path),
        )
    except ExchangeRatesCacheError:
        exchange_rates = ExchangeRates(
            start_date=start_date,
            end_date=end_date,
            overwrite=True,
            raw_file_path=str(raw_exchange_rates_path),
        )

    rates_df = exchange_rates.get_rates()
    fx_table: dict[str, tuple[list[date], list[float]]] = {}
    for currency in sorted(set(rates_df["currency"].to_list())):
        currency_df = rates_df.filter(pl.col("currency") == currency).sort("rate_date")
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


def build_buy_lot(trade, fx_table: dict[str, tuple[list[date], list[float]]], statement_path: str) -> Lot:
    buy_fx = get_fx_rate(fx_table, trade.trade_currency, trade.trade_date)
    return Lot(
        ticker=trade.ticker,
        isin=trade.isin,
        lot_id=f"{trade.ticker}:{trade.trade_date.isoformat()}:{trade.trade_id}",
        buy_date=trade.trade_date,
        original_quantity=round_qty(float(trade.quantity)),
        remaining_quantity=round_qty(float(trade.quantity)),
        trade_currency=trade.trade_currency,
        buy_price_ccy=round_money(float(trade.price_ccy)),
        buy_commission_ccy=round_money(float(trade.commission_ccy)),
        total_cost_ccy=round_money(float(trade.gross_amount_ccy)),
        buy_fx=round_money(buy_fx),
        original_cost_eur=round_money(float(trade.gross_amount_ccy) / buy_fx),
        source_trade_id=trade.trade_id,
        source_statement_file=statement_path,
    )


def apply_split(lots: list[Lot], split_event) -> None:
    for lot in lots:
        if lot.ticker != split_event.ticker or lot.buy_date > split_event.event_date:
            continue
        lot.original_quantity = round_qty(lot.original_quantity * float(split_event.factor))
        lot.remaining_quantity = round_qty(lot.remaining_quantity * float(split_event.factor))
        lot.buy_price_ccy = round_money(lot.buy_price_ccy / float(split_event.factor))
        lot.add_note(f"Applied split factor {split_event.factor} on {split_event.event_date.isoformat()}")


def consume_sell(lots: list[Lot], trade) -> None:
    remaining_to_sell = float(trade.quantity)
    for lot in lots:
        if remaining_to_sell <= 0:
            break
        if lot.ticker != trade.ticker or lot.remaining_quantity <= 0:
            continue

        consumed_quantity = min(lot.remaining_quantity, remaining_to_sell)
        fraction = consumed_quantity / lot.remaining_quantity

        lot.remaining_quantity = round_qty(lot.remaining_quantity - consumed_quantity)
        lot.total_cost_ccy = round_money(lot.total_cost_ccy * (1 - fraction))
        lot.buy_commission_ccy = round_money(lot.buy_commission_ccy * (1 - fraction))
        lot.original_cost_eur = round_money(lot.original_cost_eur * (1 - fraction))
        lot.cumulative_stepup_eur = round_money(lot.cumulative_stepup_eur * (1 - fraction))
        lot.status = "closed" if lot.remaining_quantity == 0 else "partially_sold"

        remaining_to_sell -= consumed_quantity

    if remaining_to_sell > 0:
        raise ValueError(f"Sell of {trade.ticker} on {trade.trade_date} exceeds available quantity")


def process_events_into_lots(
    initial_lots: list[Lot],
    trades: list,
    split_events: list,
    fx_table: dict[str, tuple[list[date], list[float]]],
    tax_year: int,
    source_label: str = "",
    *,
    up_to_year_end: bool = True,
) -> list[Lot]:
    """Process trades and splits against initial lots.

    When up_to_year_end=True, processes events up to year-end cutoff (for building year-end lots).
    When up_to_year_end=False, processes only events AFTER year-end (for continuing lot history).
    """
    year_end_cutoff = datetime.combine(date(tax_year, 12, 31), time(23, 59, 59), tzinfo=UTC)

    events: list[tuple[datetime, str, object]] = []
    for trade in trades:
        events.append((trade.trade_datetime.replace(tzinfo=UTC), "trade", trade))
    for split_event in split_events:
        events.append((datetime.combine(split_event.event_date, time(23, 0), tzinfo=UTC), "split", split_event))
    events.sort(key=lambda item: item[0])

    lots = list(initial_lots)
    for event_dt, event_type, payload in events:
        if up_to_year_end and event_dt > year_end_cutoff:
            break
        if not up_to_year_end and event_dt <= year_end_cutoff:
            continue
        if event_type == "trade":
            if payload.operation == "buy":
                lots.append(build_buy_lot(payload, fx_table, source_label))
            else:
                consume_sell(lots, payload)
        else:
            apply_split(lots, payload)

    return lots


def build_lots(
    statement_path: str | Path,
    fx_table: dict[str, tuple[list[date], list[float]]],
    tax_year: int,
) -> tuple[list[Lot], date]:
    statement_path = str(statement_path)
    trades = load_target_trades(statement_path)
    split_events = load_split_events(statement_path)
    lots = process_events_into_lots([], trades, split_events, fx_table, tax_year, source_label=statement_path)
    return lots, date(tax_year, 12, 31)


def continue_lot_history(
    lots: list[Lot],
    statement_path: str | Path,
    fx_table: dict[str, tuple[list[date], list[float]]],
    tax_year: int,
) -> list[Lot]:
    trades = load_target_trades(statement_path)
    split_events = load_split_events(statement_path)
    return process_events_into_lots(
        deepcopy(lots), trades, split_events, fx_table, tax_year,
        source_label=str(statement_path), up_to_year_end=False,
    )


def apply_year_end_stepup(
    year_end_lots: list[Lot],
    price_rows: dict[str, dict[str, str]],
    tax_year: int,
    year_end_date: date,
    fx_table: dict[str, tuple[list[date], list[float]]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    calc_rows: list[dict[str, object]] = []
    adjustment_rows: list[dict[str, object]] = []

    for ticker, row in sorted(price_rows.items()):
        open_lots = [lot for lot in year_end_lots if lot.ticker == ticker and lot.remaining_quantity > 0]
        shares_held = round_qty(sum(lot.remaining_quantity for lot in open_lots))
        if shares_held <= 0:
            continue

        first_price = float(row["first_price_ccy"])
        last_price = float(row["last_price_ccy"])
        deemed_per_share_ccy = max(0.9 * (last_price - first_price), 0.1 * last_price)
        deemed_amount_ccy = round_money(deemed_per_share_ccy * shares_held)
        year_end_fx = get_fx_rate(fx_table, row["trade_currency"].strip(), year_end_date)
        deemed_amount_eur = round_money(deemed_amount_ccy / year_end_fx)
        per_share_stepup_eur = deemed_amount_eur / shares_held

        calc_rows.append(
            {
                "tax_year": tax_year,
                "event_date": year_end_date.isoformat(),
                "ticker": ticker,
                "isin": row["isin"].strip(),
                "trade_currency": row["trade_currency"].strip(),
                "first_price_ccy": round_money(first_price),
                "last_price_ccy": round_money(last_price),
                "shares_held_year_end": shares_held,
                "deemed_amount_per_share_ccy": round_money(deemed_per_share_ccy),
                "deemed_amount_ccy": deemed_amount_ccy,
                "year_end_fx": round_money(year_end_fx),
                "deemed_amount_eur": deemed_amount_eur,
                "per_share_stepup_eur": round_money(per_share_stepup_eur),
                "notes": row.get("notes", ""),
            }
        )

        allocated_total = 0.0
        for index, lot in enumerate(open_lots):
            if index == len(open_lots) - 1:
                stepup_eur = round_money(deemed_amount_eur - allocated_total)
            else:
                stepup_eur = round_money(lot.remaining_quantity * per_share_stepup_eur)
                allocated_total += stepup_eur

            lot.cumulative_stepup_eur = round_money(lot.cumulative_stepup_eur + stepup_eur)
            lot.last_adjustment_year = str(tax_year)
            lot.last_adjustment_type = "deemed_income_stepup"
            lot.last_adjustment_amount_eur = stepup_eur
            lot.add_note(f"{tax_year} deemed-income step-up allocated on {year_end_date.isoformat()}")

            adjustment_rows.append(
                {
                    "tax_year": tax_year,
                    "event_date": year_end_date.isoformat(),
                    "ticker": lot.ticker,
                    "lot_id": lot.lot_id,
                    "eligible_quantity": round_qty(lot.remaining_quantity),
                    "per_share_stepup_eur": round_money(per_share_stepup_eur),
                    "stepup_eur": stepup_eur,
                    "year_end_fx": round_money(year_end_fx),
                    "notes": lot.notes,
                }
            )

    return calc_rows, adjustment_rows


def carry_stepups_forward(year_end_lots: list[Lot], full_lots: list[Lot]) -> list[Lot]:
    stepup_by_lot_id = {lot.lot_id: lot for lot in year_end_lots}
    for lot in full_lots:
        if lot.lot_id not in stepup_by_lot_id:
            continue
        source_lot = stepup_by_lot_id[lot.lot_id]
        lot.cumulative_stepup_eur = source_lot.cumulative_stepup_eur
        lot.last_adjustment_year = source_lot.last_adjustment_year
        lot.last_adjustment_type = source_lot.last_adjustment_type
        lot.last_adjustment_amount_eur = source_lot.last_adjustment_amount_eur
        lot.notes = source_lot.notes
    return full_lots


def empty_sales_df() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "sale_date": pl.String,
            "ticker": pl.String,
            "quantity_sold": pl.Float64,
            "sale_price_ccy": pl.Float64,
            "sale_fx": pl.Float64,
            "lot_id": pl.String,
            "lot_buy_date": pl.String,
            "quantity_from_lot": pl.Float64,
            "taxable_proceeds_eur": pl.Float64,
            "taxable_original_basis_eur": pl.Float64,
            "taxable_stepup_basis_eur": pl.Float64,
            "taxable_total_basis_eur": pl.Float64,
            "taxable_gain_loss_eur": pl.Float64,
            "informational_sale_proceeds_ccy": pl.Float64,
            "informational_buy_cost_ccy_excl_fees": pl.Float64,
            "informational_buy_commission_ccy_allocated": pl.Float64,
            "informational_buy_cost_ccy_incl_fees": pl.Float64,
            "notes": pl.String,
        }
    )


def simulate_sales(
    lots: list[Lot],
    sale_rows: list[dict[str, str]],
    fx_table: dict[str, tuple[list[date], list[float]]],
) -> pl.DataFrame:
    if not sale_rows:
        return empty_sales_df()

    simulated_lots = deepcopy(lots)
    sale_lines: list[dict[str, object]] = []

    for row in sorted(sale_rows, key=lambda value: (value["sale_date"], value["ticker"])):
        sale_date = datetime.strptime(row["sale_date"], "%Y-%m-%d").date()
        ticker = row["ticker"].strip()
        quantity_left = float(row["quantity"])
        sale_price_ccy = float(row["sale_price_ccy"])
        trade_currency = next(lot.trade_currency for lot in simulated_lots if lot.ticker == ticker)
        sale_fx = get_fx_rate(fx_table, trade_currency, sale_date)

        for lot in simulated_lots:
            if quantity_left <= 0:
                break
            if lot.ticker != ticker or lot.remaining_quantity <= 0:
                continue

            quantity_from_lot = min(lot.remaining_quantity, quantity_left)
            fraction = quantity_from_lot / lot.remaining_quantity
            original_basis_eur = round_money(lot.original_cost_eur * fraction)
            stepup_basis_eur = round_money(lot.cumulative_stepup_eur * fraction)
            taxable_basis_eur = round_money(original_basis_eur + stepup_basis_eur)
            taxable_proceeds_eur = round_money((quantity_from_lot * sale_price_ccy) / sale_fx)
            buy_cost_ccy = round_money(lot.total_cost_ccy * fraction)
            buy_commission_ccy = round_money(lot.buy_commission_ccy * fraction)

            sale_lines.append(
                {
                    "sale_date": sale_date.isoformat(),
                    "ticker": ticker,
                    "quantity_sold": round_qty(float(row["quantity"])),
                    "sale_price_ccy": round_money(sale_price_ccy),
                    "sale_fx": round_money(sale_fx),
                    "lot_id": lot.lot_id,
                    "lot_buy_date": lot.buy_date.isoformat(),
                    "quantity_from_lot": round_qty(quantity_from_lot),
                    "taxable_proceeds_eur": taxable_proceeds_eur,
                    "taxable_original_basis_eur": original_basis_eur,
                    "taxable_stepup_basis_eur": stepup_basis_eur,
                    "taxable_total_basis_eur": taxable_basis_eur,
                    "taxable_gain_loss_eur": round_money(taxable_proceeds_eur - taxable_basis_eur),
                    "informational_sale_proceeds_ccy": round_money(quantity_from_lot * sale_price_ccy),
                    "informational_buy_cost_ccy_excl_fees": buy_cost_ccy,
                    "informational_buy_commission_ccy_allocated": buy_commission_ccy,
                    "informational_buy_cost_ccy_incl_fees": round_money(buy_cost_ccy + buy_commission_ccy),
                    "notes": "Taxable result excludes buy and sell fees under Austrian private-investor rules.",
                }
            )

            lot.remaining_quantity = round_qty(lot.remaining_quantity - quantity_from_lot)
            lot.total_cost_ccy = round_money(lot.total_cost_ccy - buy_cost_ccy)
            lot.buy_commission_ccy = round_money(lot.buy_commission_ccy - buy_commission_ccy)
            lot.original_cost_eur = round_money(lot.original_cost_eur - original_basis_eur)
            lot.cumulative_stepup_eur = round_money(lot.cumulative_stepup_eur - stepup_basis_eur)
            lot.status = "closed" if lot.remaining_quantity == 0 else "partially_sold"
            quantity_left -= quantity_from_lot

        if quantity_left > 0:
            raise ValueError(f"Sale plan for {ticker} on {sale_date} exceeds available quantity")

    return pl.DataFrame(sale_lines) if sale_lines else empty_sales_df()


def write_csv(df: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)


def write_summary(
    summary_path: Path,
    ledger_df: pl.DataFrame,
    calc_df: pl.DataFrame,
    sales_df: pl.DataFrame,
    tax_year: int,
    source_label: str = "Non-Reporting Funds",
) -> None:
    total_deemed_amount_eur = calc_df["deemed_amount_eur"].sum() if calc_df.height else 0.0
    calc_lines = [
        f"- `{row['ticker']}`: shares at year-end `{row['shares_held_year_end']}`, deemed amount `{row['deemed_amount_eur']:.4f} EUR`"
        for row in calc_df.to_dicts()
    ]
    ledger_lines = []
    tickers = sorted(ledger_df["ticker"].unique().to_list()) if ledger_df.height else []
    for ticker in tickers:
        ticker_ledger_df = ledger_df.filter(pl.col("ticker") == ticker)
        quantity = ticker_ledger_df["remaining_quantity"].sum() if ticker_ledger_df.height else 0.0
        adjusted_basis = ticker_ledger_df["adjusted_basis_eur"].sum() if ticker_ledger_df.height else 0.0
        ledger_lines.append(f"- `{ticker}`: open quantity `{quantity}`, adjusted basis `{adjusted_basis:.4f} EUR`")

    sales_total = sales_df["taxable_gain_loss_eur"].sum() if sales_df.height else 0.0
    sale_line = (
        f"- simulated taxable sale result: `{sales_total:.4f} EUR`"
        if sales_df.height
        else "- no sale rows were provided; sale simulation skipped"
    )

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        "\n".join(
            [
                f"# {source_label} Exit Summary",
                "",
                f"## {tax_year} Step-Up",
                *calc_lines,
                f"- `total_deemed_amount_eur`: `{total_deemed_amount_eur:.4f} EUR`",
                "",
                "## Working Ledger",
                *ledger_lines,
                "",
                "## Sale Simulation",
                sale_line,
                "",
                "## Notes",
                "- Ordinary distributions and withholding-tax handling remain outside this workflow.",
                "- Taxable basis/proceeds exclude buy and sell fees; fee fields are informational only.",
            ]
        )
        + "\n"
    )


def run_workflow(
    statement_path: str | Path,
    price_input_path: str | Path,
    output_dir: str | Path,
    tax_year: int = 2025,
    sale_plan_path: str | Path | None = None,
    raw_exchange_rates_path: str | Path = "data/input/currencies/raw_exchange_rates.csv",
) -> dict[str, Path]:
    price_rows = load_price_rows(price_input_path, tax_year, target_tickers=TARGET_TICKERS)
    sale_rows = load_sale_rows(sale_plan_path)

    all_relevant_dates = [trade.trade_date for trade in load_target_trades(statement_path)]
    all_relevant_dates.append(date(tax_year, 12, 31))
    all_relevant_dates.extend(
        datetime.strptime(row["sale_date"], "%Y-%m-%d").date() for row in sale_rows if row.get("sale_date")
    )
    fx_table = build_fx_table(min(all_relevant_dates), max(all_relevant_dates), raw_exchange_rates_path)

    year_end_lots, year_end_date = build_lots(statement_path, fx_table, tax_year)
    calc_rows, adjustment_rows = apply_year_end_stepup(year_end_lots, price_rows, tax_year, year_end_date, fx_table)
    full_lots = continue_lot_history(year_end_lots, statement_path, fx_table, tax_year)
    full_lots = carry_stepups_forward(year_end_lots, full_lots)

    working_ledger_df = pl.DataFrame(
        [lot.to_record() for lot in sorted(full_lots, key=lambda lot: (lot.ticker, lot.buy_date, lot.lot_id))]
    )
    calc_df = pl.DataFrame(calc_rows)
    basis_adjustments_df = pl.DataFrame(adjustment_rows)
    sales_df = simulate_sales(full_lots, sale_rows, fx_table)

    output_dir = Path(output_dir)
    ledger_path = output_dir / "non_reporting_funds_working_ledger.csv"
    calc_path = output_dir / "non_reporting_funds_2025_calc.csv"
    basis_path = output_dir / "non_reporting_funds_2025_basis_adjustments.csv"
    sales_path = output_dir / "non_reporting_funds_exit_sales.csv"
    summary_path = output_dir / "non_reporting_funds_exit_summary.md"

    write_csv(working_ledger_df, ledger_path)
    write_csv(calc_df, calc_path)
    write_csv(basis_adjustments_df, basis_path)
    write_csv(sales_df, sales_path)
    write_summary(summary_path, working_ledger_df, calc_df, sales_df, tax_year)

    return {
        "working_ledger": ledger_path,
        "calc": calc_path,
        "basis_adjustments": basis_path,
        "sales": sales_path,
        "summary": summary_path,
    }


def run_ibkr_reit_workflow(
    opening_state_path: str | Path,
    ibkr_trade_history_path: str | Path,
    price_input_path: str | Path,
    output_dir: str | Path,
    tax_year: int = 2025,
    sale_plan_path: str | Path | None = None,
    raw_exchange_rates_path: str | Path = "data/input/currencies/raw_exchange_rates.csv",
    target_tickers: tuple[str, ...] | None = None,
    include_post_year_end_trades: bool = False,
) -> dict[str, Path]:
    """Non-reporting funds (Nicht-Meldefonds) workflow for IBKR REITs.

    1. Load initial REIT lots from the Austrian opening state CSV (FMV reset on move-in date).
    2. Load post-opening REIT trades from IBKR trade history XML (buys and sells after snapshot).
    3. Build FX table covering all relevant dates from ECB rates.
    4. Apply trades to opening lots up to year-end to get year-end position.
    5. Calculate AgE (deemed income) per ticker using max(0.9*(last-first), 0.1*last) formula.
    6. Allocate step-up to open lots and adjust their EUR cost basis.
    7. By default, freeze the year-end stepped-up ledger and simulate later exits only
       from the manual sale-plan CSV, mirroring the non-reporting ETF workflow.
    8. Optionally continue lot history with post-year-end trades if explicitly enabled.
    9. Write output artifacts: working ledger, AgE calc, basis adjustments, sales, summary.
    """
    from scripts.non_reporting_funds_exit.ibkr_lots import IBKR_REIT_TICKERS, load_ibkr_reit_trades, load_opening_lots

    if target_tickers is None:
        target_tickers = IBKR_REIT_TICKERS

    opening_lots = load_opening_lots(opening_state_path, target_tickers=target_tickers)
    if not opening_lots:
        raise ValueError(f"No REIT lots found in opening state: {opening_state_path}")

    snapshot_date = opening_lots[0].buy_date
    reit_trades = load_ibkr_reit_trades(
        ibkr_trade_history_path, target_tickers=target_tickers, after_date=snapshot_date,
    )

    price_rows = load_price_rows(price_input_path, tax_year, target_tickers=target_tickers)
    sale_rows = load_sale_rows(sale_plan_path)

    all_relevant_dates: list[date] = [snapshot_date, date(tax_year, 12, 31)]
    all_relevant_dates.extend(trade.trade_date for trade in reit_trades)
    all_relevant_dates.extend(
        datetime.strptime(row["sale_date"], "%Y-%m-%d").date() for row in sale_rows if row.get("sale_date")
    )
    fx_table = build_fx_table(min(all_relevant_dates), max(all_relevant_dates), raw_exchange_rates_path)

    year_end_lots = process_events_into_lots(
        opening_lots, reit_trades, [], fx_table, tax_year, source_label="ibkr",
    )
    year_end_date = date(tax_year, 12, 31)
    calc_rows, adjustment_rows = apply_year_end_stepup(year_end_lots, price_rows, tax_year, year_end_date, fx_table)

    if include_post_year_end_trades:
        full_lots = process_events_into_lots(
            deepcopy(year_end_lots), reit_trades, [], fx_table, tax_year,
            source_label="ibkr", up_to_year_end=False,
        )
        full_lots = carry_stepups_forward(year_end_lots, full_lots)
    else:
        full_lots = deepcopy(year_end_lots)

    working_ledger_df = pl.DataFrame(
        [lot.to_record() for lot in sorted(full_lots, key=lambda lot: (lot.ticker, lot.buy_date, lot.lot_id))]
    )
    calc_df = pl.DataFrame(calc_rows)
    basis_adjustments_df = pl.DataFrame(adjustment_rows)
    sales_df = simulate_sales(full_lots, sale_rows, fx_table)

    output_dir = Path(output_dir)
    ledger_path = output_dir / "ibkr_reit_working_ledger.csv"
    calc_path = output_dir / f"ibkr_reit_{tax_year}_calc.csv"
    basis_path = output_dir / f"ibkr_reit_{tax_year}_basis_adjustments.csv"
    sales_path = output_dir / "ibkr_reit_exit_sales.csv"
    summary_path = output_dir / "ibkr_reit_exit_summary.md"

    write_csv(working_ledger_df, ledger_path)
    write_csv(calc_df, calc_path)
    write_csv(basis_adjustments_df, basis_path)
    write_csv(sales_df, sales_path)
    write_summary(summary_path, working_ledger_df, calc_df, sales_df, tax_year, source_label="IBKR REIT Non-Reporting Funds")

    return {
        "working_ledger": ledger_path,
        "calc": calc_path,
        "basis_adjustments": basis_path,
        "sales": sales_path,
        "summary": summary_path,
    }
