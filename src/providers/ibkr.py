import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Sequence

import polars as pl

from src.broker_history import build_fx_table_from_rates_df, get_fx_rate, load_ibkr_stock_like_trades, round_money, round_qty
from src.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET, FLOAT_PRECISION, CurrencyCode, TransactionTypeIBKR
from src.const import Column as Col
from src.finanzonline import (
    BUCKET_AMOUNT_EUR_COL,
    BUCKET_CATEGORY_COL,
    BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL,
    BUCKET_LABEL_COL,
    BUCKET_SCHEMA,
    BUCKET_SOURCE_COL,
    BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL,
    ETF_DISTRIBUTION_BUCKET_CATEGORY,
    ORDINARY_INCOME_BUCKET_CATEGORY,
    empty_finanzonline_bucket_df,
)
from src.moving_average import (
    PositionEvent,
    build_basis_reset_event,
    build_buy_event,
    build_sell_event,
    load_position_states,
    position_events_to_df,
    position_states_to_df,
    replay_events,
)
from src.utils import (
    build_separate_trade_profit_loss_rows,
    calculate_kest,
    convert_to_euro,
    extract_elements,
    has_rows,
    join_exchange_rates,
    read_xml_to_df,
)

IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES = {"COMMON", "ADR", "REIT"}


SummarySectionName = Literal["dividends", "bonds", "etf_dividends", "trades"]


@dataclass(frozen=True)
class IbkrSummarySection:
    name: SummarySectionName
    df: pl.DataFrame


def handle_dividend_adjustments(df: pl.DataFrame) -> pl.DataFrame:
    df = df.group_by(
        ["action_id", "settle_date", "issuer_country_code", "sub_category", "symbol", "currency", "type"]
    ).agg(pl.sum("amount").alias("amount"))

    return df


def apply_pivot(df: pl.DataFrame) -> pl.DataFrame:
    # Grouping keys
    index_keys = [
        "settle_date",
        "issuer_country_code",
        "sub_category",
        "symbol",
        "currency",
    ]

    duplicates_df = df.group_by(index_keys + ["type"]).agg(pl.len().alias("row_count")).filter(pl.col("row_count") > 1)

    if not duplicates_df.is_empty():
        logging.warning(f"\nWarning: Duplicate rows detected:\n{duplicates_df}")

    pivoted_df = df.pivot(
        values=["amount", "amount_euro"],  # Columns to aggregate
        index=index_keys,  # Grouping columns
        on="type",  # Values in this column become new column names
        aggregate_function="sum",  # Take unique values (default for pivot)
    )

    # Rename columns for clarity
    pivoted_df = pivoted_df.rename(
        {
            "amount_Dividends": "dividends",
            "amount_euro_Dividends": "dividends_euro",
        }
    ).with_columns(
        (pl.col("amount_Withholding Tax") if "amount_Withholding Tax" in pivoted_df.columns else pl.lit(0.0))
        .fill_null(0.0)
        .alias("withholding_tax"),
        (pl.col("amount_euro_Withholding Tax") if "amount_euro_Withholding Tax" in pivoted_df.columns else pl.lit(0.0))
        .fill_null(0.0)  # Also fill nulls
        .alias("withholding_tax_euro"),
    )

    return pivoted_df.select(
        [
            "settle_date",
            "issuer_country_code",
            "sub_category",
            "symbol",
            "currency",
            "dividends",
            "withholding_tax",
            "dividends_euro",
            "withholding_tax_euro",
        ]
    )


def agg_final_transactions(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("issuer_country_code", Col.currency)
        .agg(
            pl.sum("dividends").round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.sum("dividends_euro").round(FLOAT_PRECISION).alias(Col.dividends_euro_total),
            pl.sum("dividends_euro_net").round(FLOAT_PRECISION).alias(Col.dividends_euro_net_total),
            pl.sum("withholding_tax_euro").round(FLOAT_PRECISION).alias("withholding_tax_euro_total"),
            pl.sum("kest_gross").round(FLOAT_PRECISION).alias("kest_gross_total"),
            pl.sum("kest_net").round(FLOAT_PRECISION).alias("kest_net_total"),
        )
        .sort("dividends_euro_total", descending=True)
    )


def _build_cash_transactions_tax_df(
    xml_file_path: str | Sequence[str],
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> pl.DataFrame | None:
    cash_transactions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CashTransactions"), "CashTransaction"),
        dedupe=True,
    )
    if cash_transactions_df.is_empty():
        return None

    cash_transactions_df = (
        cash_transactions_df.rename({"subCategory": "sub_category", "actionID": "action_id"})
        .with_columns(
            pl.col("amount").cast(pl.Float64).alias("amount"),
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            pl.col("settleDate").str.strptime(pl.Date, "%Y-%m-%d").alias("settle_date"),
        )
        .filter(pl.col("settle_date").is_between(start_date, end_date))
    )
    if excluded_cash_transaction_subcategories:
        cash_transactions_df = cash_transactions_df.filter(
            ~pl.col("sub_category").is_in(sorted(excluded_cash_transaction_subcategories))
        )
    if cash_transactions_df.is_empty():
        return None
    cash_transactions_df = handle_dividend_adjustments(cash_transactions_df)

    types = cash_transactions_df["type"].unique().to_list()
    logging.info(f"Transaction Types: {types}")
    sum_per_type_df = cash_transactions_df.group_by("sub_category", "type").agg(pl.col("amount").sum())
    logging.info(f"\nSum per Transaction Category-Type:\n{sum_per_type_df}")

    cash_transactions_df = (
        cash_transactions_df.select(
            [
                "symbol",
                "sub_category",
                "currency",
                pl.when(pl.col("type") == TransactionTypeIBKR.pil)
                .then(pl.lit(TransactionTypeIBKR.dividend))
                .otherwise(pl.col("type"))
                .alias("type"),
                pl.col("amount").alias("amount"),
                "settle_date",
                "issuer_country_code",
            ]
        )
        .filter(pl.col("type").is_in(["Dividends", "Withholding Tax"]))
        .with_columns(
            pl.when(pl.col("type") == TransactionTypeIBKR.tax)
            .then(pl.col("amount").abs())
            .otherwise(pl.col("amount"))
            .alias("amount")
        )
    )

    joined_df = join_exchange_rates(
        df=cash_transactions_df,
        rates_df=exchange_rates_df,
        df_date_col="settle_date",
    )
    joined_df = convert_to_euro(joined_df, col_to_convert="amount")

    pivoted_df = apply_pivot(joined_df)
    logging.debug("\nPivoted DataFrame:\n {}".format(pivoted_df))

    pivoted_df = calculate_kest(pivoted_df, amount_col="dividends_euro", tax_withheld_col="withholding_tax_euro")
    logging.debug("\nPivoted with KeST DataFrame:\n {}".format(pivoted_df))
    return pivoted_df


def build_finanzonline_dividend_buckets_ibkr(
    xml_file_path: str | Sequence[str],
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> pl.DataFrame:
    tax_df = _build_cash_transactions_tax_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        excluded_cash_transaction_subcategories=excluded_cash_transaction_subcategories,
    )
    if tax_df is None or tax_df.is_empty():
        return empty_finanzonline_bucket_df()

    return tax_df.select(
        pl.lit("ibkr").alias(BUCKET_SOURCE_COL),
        pl.concat_str(
            [
                pl.lit("cash_dividend:"),
                pl.col("settle_date").cast(pl.String),
                pl.lit(":"),
                pl.col("symbol"),
                pl.lit(":"),
                pl.col("sub_category"),
            ]
        ).alias(BUCKET_LABEL_COL),
        pl.when(pl.col("sub_category") == "ETF")
        .then(pl.lit(ETF_DISTRIBUTION_BUCKET_CATEGORY))
        .otherwise(pl.lit(ORDINARY_INCOME_BUCKET_CATEGORY))
        .alias(BUCKET_CATEGORY_COL),
        pl.col("dividends_euro").alias(BUCKET_AMOUNT_EUR_COL),
        pl.col("withholding_tax_euro").alias(BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL),
        (pl.col("kest_gross") - pl.col("kest_net"))
        .clip(lower_bound=0.0)
        .alias(BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL),
    ).cast(BUCKET_SCHEMA)


def _build_position_events_from_raw_trades(
    raw_trades,
    *,
    exchange_rates_df: pl.DataFrame,
    sequence_offset: int = 0,
) -> list[PositionEvent]:
    if not raw_trades:
        return []
    relevant_currencies = {trade.currency for trade in raw_trades if trade.currency != CurrencyCode.euro.value}
    fx_table = build_fx_table_from_rates_df(exchange_rates_df, currencies=relevant_currencies)
    events: list[PositionEvent] = []
    for index, trade in enumerate(raw_trades, start=sequence_offset):
        fx_to_eur = get_fx_rate(fx_table, trade.currency, trade.trade_date)
        if trade.operation == "buy":
            events.append(
                build_buy_event(
                    broker="ibkr",
                    ticker=trade.ticker,
                    isin=trade.isin,
                    currency=trade.currency,
                    asset_class=trade.asset_class,
                    trade_date=trade.trade_date,
                    quantity=trade.quantity,
                    price_ccy=trade.price_ccy,
                    fx_to_eur=fx_to_eur,
                    source_id=trade.trade_id,
                    source_file=trade.source_statement_file,
                    sequence_key=index,
                )
            )
            continue
        events.append(
            build_sell_event(
                broker="ibkr",
                ticker=trade.ticker,
                isin=trade.isin,
                currency=trade.currency,
                asset_class=trade.asset_class,
                trade_date=trade.trade_date,
                quantity=trade.quantity,
                price_ccy=trade.price_ccy,
                fx_to_eur=fx_to_eur,
                source_id=trade.trade_id,
                source_file=trade.source_statement_file,
                notes="Austrian moving-average sale result from raw IBKR trade history.",
                sequence_key=index,
            )
        )
    return events


def _extract_ibkr_trade_rows(root) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    for parent_tag in (".//TradeConfirms", ".//Trades"):
        for parent in root.findall(parent_tag):
            rows.extend(extract_elements(parent, "TradeConfirm"))
            rows.extend(extract_elements(parent, "Trade"))
    return rows


def _select_stock_event_columns(events_df: pl.DataFrame) -> pl.DataFrame:
    return events_df.select(
        [
            "ticker",
            "isin",
            "asset_class",
            "currency",
            "event_type",
            "event_date",
            "effective_date",
            "quantity",
            "quantity_delta",
            "price_ccy",
            "fx_to_eur",
            "proceeds_eur",
            "base_cost_delta_eur",
            "realized_basis_eur",
            "realized_base_cost_eur",
            "realized_gain_loss_eur",
            "split_ratio",
            "quantity_after",
            "base_cost_total_eur_after",
            "total_basis_eur_after",
            "average_basis_eur_after",
            "broker",
            "source_id",
            "sequence_key",
            "source_file",
            "notes",
        ]
    )


def _select_stock_sale_columns(sales_df: pl.DataFrame) -> pl.DataFrame:
    return sales_df.select(
        [
            "sale_date",
            "ticker",
            "isin",
            "quantity_sold",
            "sale_price_ccy",
            "sale_fx",
            "taxable_proceeds_eur",
            "realized_base_cost_eur",
            "taxable_original_basis_eur",
            "realized_oekb_adjustment_eur",
            "taxable_stepup_basis_eur",
            "taxable_total_basis_eur",
            "taxable_gain_loss_eur",
            "notes",
            "sale_trade_id",
        ]
    )


def _build_stock_authoritative_outputs(
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    *,
    ibkr_trade_history_path: str,
    austrian_opening_state_path: str | None,
    authoritative_start_date: date | None,
    excluded_trade_subcategories: set[str] | None,
) -> tuple[pl.DataFrame | None, pl.DataFrame, pl.DataFrame]:
    if authoritative_start_date is not None and not austrian_opening_state_path:
        raise ValueError(
            "authoritative_start_date requires austrian_opening_state_path; "
            "move-in authoritative start without opening state is not supported"
        )

    opening_states = []
    snapshot_date: date | None = None
    if austrian_opening_state_path:
        loaded_states = [
            state
            for state in load_position_states(austrian_opening_state_path)
            if state.asset_class in AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES
        ]
        opening_states = loaded_states
        snapshot_dates = {state.snapshot_date for state in loaded_states if state.snapshot_date}
        if snapshot_dates:
            snapshot_date = date.fromisoformat(sorted(snapshot_dates)[0])
    raw_trades = load_ibkr_stock_like_trades(
        ibkr_trade_history_path,
        allowed_asset_classes=AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES,
    )
    if not raw_trades:
        return None, position_states_to_df(opening_states), _select_stock_event_columns(position_events_to_df([]))

    processing_start_date = start_date
    if authoritative_start_date is not None:
        processing_start_date = max(processing_start_date, authoritative_start_date)
    if snapshot_date is not None and snapshot_date.year == processing_start_date.year:
        processing_start_date = max(processing_start_date, snapshot_date)

    opening_lower_bound = snapshot_date or authoritative_start_date
    opening_trades = [
        trade
        for trade in raw_trades
        if (opening_lower_bound is None or opening_lower_bound <= trade.trade_date) and trade.trade_date < start_date
    ]
    current_period_trades = [trade for trade in raw_trades if processing_start_date <= trade.trade_date <= end_date]
    if excluded_trade_subcategories:
        opening_trades = [trade for trade in opening_trades if trade.asset_class not in excluded_trade_subcategories]
        current_period_trades = [
            trade for trade in current_period_trades if trade.asset_class not in excluded_trade_subcategories
        ]
    opening_events = _build_position_events_from_raw_trades(opening_trades, exchange_rates_df=exchange_rates_df)
    opening_states, _, _ = replay_events(opening_states, opening_events)

    run_start_events: list[PositionEvent] = []
    if snapshot_date is not None and start_date <= snapshot_date <= end_date:
        for index, state in enumerate(opening_states):
            run_start_events.append(
                build_basis_reset_event(
                    broker=state.broker,
                    ticker=state.ticker,
                    isin=state.isin,
                    currency=state.currency,
                    asset_class=state.asset_class,
                    event_date=snapshot_date,
                    quantity=state.quantity,
                    base_cost_total_eur=state.base_cost_total_eur,
                    basis_adjustment_total_eur=state.basis_adjustment_total_eur,
                    basis_method=state.basis_method or "carryforward_opening_state",
                    source_file=state.source_file,
                    notes="Synthetic event log anchor for opening Austrian state.",
                    sequence_key=index,
                )
            )

    current_events = _build_position_events_from_raw_trades(
        current_period_trades,
        exchange_rates_df=exchange_rates_df,
        sequence_offset=len(run_start_events),
    )
    final_states, event_rows, sale_rows = replay_events(opening_states, current_events)
    events_df = position_events_to_df(event_rows)
    sales_df = _select_stock_sale_columns(pl.DataFrame(sale_rows).sort(["sale_date", "ticker"])) if sale_rows else None
    if run_start_events:
        _, opening_event_rows, _ = replay_events([], run_start_events)
        events_df = position_events_to_df(opening_event_rows + event_rows)
    return sales_df, position_states_to_df(final_states), _select_stock_event_columns(events_df)


def _build_trade_summary_df(
    trades_detail_df: pl.DataFrame,
    *,
    separate_trade_profit_loss: bool,
) -> pl.DataFrame | None:
    trades_totals_df = trades_detail_df.select(
        pl.col("taxable_gain_loss_eur").sum().fill_null(0.0).round(FLOAT_PRECISION).alias(Col.profit_euro_total),
        pl.when(pl.col("taxable_gain_loss_eur") > 0)
        .then(pl.col("taxable_gain_loss_eur"))
        .otherwise(0.0)
        .sum()
        .round(FLOAT_PRECISION)
        .alias("trade_profit_euro_total"),
        (-pl.when(pl.col("taxable_gain_loss_eur") < 0).then(pl.col("taxable_gain_loss_eur")).otherwise(0.0).sum())
        .round(FLOAT_PRECISION)
        .alias("trade_loss_euro_total"),
    ).with_columns(
        pl.col(Col.profit_euro_total).alias(Col.profit_total),
        pl.col(Col.profit_euro_total).clip(lower_bound=0.0).alias("taxable_profit_euro"),
    )

    if not separate_trade_profit_loss:
        trades_tax_df = calculate_kest(
            df=trades_totals_df,
            amount_col="taxable_profit_euro",
            tax_withheld_col=None,
            net_col_name="taxable_profit_euro_net",
        )
        return trades_tax_df.select(
            pl.lit(CurrencyCode.euro.value).alias(Col.currency),
            pl.col(Col.profit_total),
            pl.col(Col.profit_euro_total),
            (pl.col(Col.profit_euro_total) - pl.col(Col.kest_net))
            .round(FLOAT_PRECISION)
            .alias(Col.profit_euro_net_total),
            pl.lit(0.0).alias(Col.withholding_tax_euro_total),
            pl.col(Col.kest_gross).round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net).round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )

    summary_frames = build_separate_trade_profit_loss_rows(trades_totals_df)
    return pl.concat(summary_frames, how="vertical_relaxed") if summary_frames else None


def process_trades_ibkr(
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    separate_trade_profit_loss: bool = True,
    excluded_trade_subcategories: set[str] | None = None,
    austrian_opening_state_path: str | None = None,
    ibkr_trade_history_path: str | None = None,
    authoritative_start_date: date | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None]:
    """
    Returns:
    - trade tax detail dataframe
    - trade summary dataframe
    - final stock-like moving-average state dataframe
    - stock position events dataframe
    """
    logging.info("\n\n======================== Processing Trades ========================\n")
    if not ibkr_trade_history_path:
        raise ValueError("process_trades_ibkr requires ibkr_trade_history_path; broker closed-lot fallback has been removed")

    trades_detail_df, state_df, returned_events_df = _build_stock_authoritative_outputs(
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        ibkr_trade_history_path=ibkr_trade_history_path,
        austrian_opening_state_path=austrian_opening_state_path,
        authoritative_start_date=authoritative_start_date,
        excluded_trade_subcategories=excluded_trade_subcategories,
    )
    if trades_detail_df is None or trades_detail_df.is_empty():
        logging.warning("No authoritative stock-like sales matched the selected date range.")
        return None, None, state_df, (returned_events_df if returned_events_df is not None and returned_events_df.height else None)
    trades_summary_df = _build_trade_summary_df(
        trades_detail_df,
        separate_trade_profit_loss=separate_trade_profit_loss,
    )
    return trades_detail_df, trades_summary_df, state_df, (returned_events_df if returned_events_df.height else None)


def process_cash_transactions_ibkr(
    xml_file_path: str | Sequence[str],
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    extract_etf: bool = False,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load IBKR cash transactions from XML and normalize key columns/types.
    2. Filter by settle date and collapse adjustment records by action/type group.
    3. Keep only dividend/tax cash flows and normalize IBKR-specific PIL type.
    4. Convert withholding tax to absolute amount while keeping dividend sign as-is.
    5. Join FX by settle date and convert original amounts to EUR.
    6. Pivot dividend/tax rows into one row per security/date and compute KESt fields.
    7. Optionally split ETF rows into a separate aggregate bucket.
    8. Aggregate final country-level totals and return main + optional ETF outputs.
    """
    logging.info("\n\n======================== Processing Cash Transactions ========================\n")
    pivoted_df = _build_cash_transactions_tax_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        excluded_cash_transaction_subcategories=excluded_cash_transaction_subcategories,
    )
    if pivoted_df is None or pivoted_df.is_empty():
        return None, None

    etf_agg_df = None
    if extract_etf:
        etf_df = pivoted_df.filter(pl.col("sub_category") == "ETF")
        if has_rows(etf_df):
            pivoted_df = pivoted_df.filter(pl.col("sub_category") != "ETF")

            etf_agg_df = agg_final_transactions(etf_df)
            logging.info("Dividends from ETFs:\n{}".format(etf_agg_df))

    country_agg_df = agg_final_transactions(pivoted_df) if has_rows(pivoted_df) else None
    logging.info("Dividends by Country:\n{}".format(country_agg_df))

    return country_agg_df, etf_agg_df


def process_bonds_ibkr(
    xml_file_path: str | Sequence[str],
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    ibkr_trade_history_path: str | Sequence[str] | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load corporate actions from XML and keep bill maturities plus any non-bill bond actions.
    2. For Treasury bills, rebuild Austrian basis from raw BUY trade history:
       - use buy amount plus accrued interest
       - exclude buy commission from basis
       - convert buy basis to EUR at buy-date FX
       - convert maturity proceeds to EUR at maturity/report-date FX
    3. For other `assetCategory="BOND"` actions, keep the existing broker realized-PnL path.
    4. Compute KESt on EUR realized PnL for each event.
    5. Produce detailed per-event output and aggregate country-level summary totals.
    """
    logging.info("\n\n======================== Processing Corporate Actions ========================\n")

    # Convert the extracted data into Polars DataFrames
    corporate_actions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CorporateActions"), "CorporateAction"),
        dedupe=True,
    )
    if corporate_actions_df.is_empty():
        logging.warning("No Corporate Actions found in the XML file.")
        return None, None

    corporate_actions_df = corporate_actions_df.select(
        [
            pl.col("assetCategory").alias("asset_category"),
            pl.col("type").alias("action_type"),
            pl.col("reportDate").str.strptime(pl.Date, "%Y-%m-%d").alias("report_date"),
            "isin",
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            "currency",
            pl.col("proceeds").cast(pl.Float64),
            pl.col("fifoPnlRealized").cast(pl.Float64).alias("realized_pnl"),
        ]
    ).filter(pl.col("report_date").is_between(start_date, end_date))

    logging.debug("\nCorporate Actions DataFrame: %s\n", corporate_actions_df)

    bill_maturities_df = corporate_actions_df.filter(
        (pl.col("asset_category") == "BILL") & (pl.col("action_type") == "TM")
    )
    other_bond_actions_df = corporate_actions_df.filter(pl.col("asset_category") == "BOND")

    tax_frames: list[pl.DataFrame] = []

    if not bill_maturities_df.is_empty():
        if not ibkr_trade_history_path:
            raise ValueError("Bill maturity processing requires ibkr_trade_history_path.")

        bill_trade_rows_df = read_xml_to_df(
            file_path=ibkr_trade_history_path,
            xml_extract_func=_extract_ibkr_trade_rows,
            dedupe=True,
        )
        bill_buy_trades_df = bill_trade_rows_df.filter(
            (pl.col("assetCategory") == "BILL") & (pl.col("buySell") == "BUY")
        ).select(
            pl.col("tradeDate").str.strptime(pl.Date, "%Y-%m-%d").alias("trade_date"),
            "isin",
            "currency",
            pl.col("amount").cast(pl.Float64).abs().alias("basis_ccy_ex_fees"),
            pl.col("commission").cast(pl.Float64, strict=False).abs().fill_null(0.0).alias("ignored_buy_commission_ccy"),
            pl.col("accruedInt").cast(pl.Float64, strict=False).abs().fill_null(0.0).alias("accrued_interest_ccy"),
        ).with_columns(
            (pl.col("basis_ccy_ex_fees") + pl.col("accrued_interest_ccy")).round(FLOAT_PRECISION).alias("basis_ccy_ex_fees")
        )

        relevant_isins = bill_maturities_df["isin"].unique().to_list()
        bill_buy_trades_df = bill_buy_trades_df.filter(pl.col("isin").is_in(relevant_isins))
        if bill_buy_trades_df.is_empty():
            raise ValueError("No raw BILL buy trades were found for the matched bill maturity events.")

        buy_basis_df = (
            convert_to_euro(
                join_exchange_rates(df=bill_buy_trades_df, rates_df=exchange_rates_df, df_date_col="trade_date"),
                "basis_ccy_ex_fees",
            )
            .group_by("isin", "currency")
            .agg(
                pl.sum("basis_ccy_ex_fees").round(FLOAT_PRECISION).alias("basis_ccy_ex_fees"),
                pl.sum("basis_ccy_ex_fees_euro").round(FLOAT_PRECISION).alias("basis_euro_ex_fees"),
                pl.sum("ignored_buy_commission_ccy").round(FLOAT_PRECISION).alias("ignored_buy_commission_ccy"),
            )
        )

        bill_tax_df = (
            convert_to_euro(
                join_exchange_rates(df=bill_maturities_df, rates_df=exchange_rates_df, df_date_col="report_date"),
                "proceeds",
            )
            .join(buy_basis_df, on=["isin", "currency"], how="left")
        )

        missing_basis_df = bill_tax_df.filter(pl.col("basis_ccy_ex_fees").is_null())
        if not missing_basis_df.is_empty():
            raise ValueError(f"Missing raw BILL buy basis for maturity events:\n{missing_basis_df}")

        bill_tax_df = bill_tax_df.with_columns(
            (pl.col("proceeds") - pl.col("basis_ccy_ex_fees")).round(FLOAT_PRECISION).alias("realized_pnl"),
            (pl.col("proceeds_euro") - pl.col("basis_euro_ex_fees")).round(FLOAT_PRECISION).alias("realized_pnl_euro"),
        )
        bill_tax_df = calculate_kest(bill_tax_df, amount_col="realized_pnl_euro").select(
            "report_date",
            "isin",
            "issuer_country_code",
            "currency",
            "proceeds",
            "realized_pnl",
            "realized_pnl_euro",
            "realized_pnl_euro_net",
            "kest_gross",
            "kest_net",
        )
        tax_frames.append(bill_tax_df)

    if not other_bond_actions_df.is_empty():
        joined_df = join_exchange_rates(
            df=other_bond_actions_df,
            rates_df=exchange_rates_df,
            df_date_col="report_date",
        )
        joined_df = convert_to_euro(joined_df, "realized_pnl")
        other_bond_tax_df = calculate_kest(joined_df, amount_col="realized_pnl_euro").select(
            "report_date",
            "isin",
            "issuer_country_code",
            "currency",
            "proceeds",
            "realized_pnl",
            "realized_pnl_euro",
            "realized_pnl_euro_net",
            "kest_gross",
            "kest_net",
        )
        tax_frames.append(other_bond_tax_df)

    if not tax_frames:
        logging.warning("No bond-like corporate actions matched the selected date range.")
        return None, None

    tax_df = pl.concat(tax_frames, how="vertical_relaxed").sort("realized_pnl", "isin", descending=True)
    logging.info(tax_df)

    country_agg_df = (
        tax_df.group_by("issuer_country_code", Col.currency)
        .agg(
            pl.sum("realized_pnl").round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.sum("realized_pnl_euro").round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.sum("realized_pnl_euro_net").round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.sum("kest_gross").round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.sum("kest_net").round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        .sort(Col.profit_euro_total, descending=True)
    )
    logging.info(country_agg_df)

    return tax_df, country_agg_df


def calculate_summary_ibkr(
    sections: list[IbkrSummarySection],
) -> pl.DataFrame:
    """
    1. Validate section names, reject duplicates, and map each section to its aggregation config.
    2. For each section dataframe, aggregate totals by currency.
    3. Normalize section totals into a single summary schema with a type label.
    4. If no sections are provided, return an empty dataframe with summary schema.
    5. Concatenate all section summaries into one IBKR summary dataframe.
    """
    section_configs = {
        "dividends": {
            "label": "dividends",
            "profit_euro_col": "dividends_euro_total",
            "profit_euro_net_col": "dividends_euro_net_total",
            "withholding_col": "withholding_tax_euro_total",
        },
        "bonds": {
            "label": "bonds",
            "profit_euro_col": Col.profit_euro_total,
            "profit_euro_net_col": Col.profit_euro_net_total,
            "withholding_col": None,
        },
        "etf_dividends": {
            "label": "ETF div",
            "profit_euro_col": "dividends_euro_total",
            "profit_euro_net_col": "dividends_euro_net_total",
            "withholding_col": "withholding_tax_euro_total",
        },
        "trades": {
            "label": "trades",
            "profit_euro_col": Col.profit_euro_total,
            "profit_euro_net_col": Col.profit_euro_net_total,
            "withholding_col": Col.withholding_tax_euro_total,
        },
    }

    if not sections:
        return pl.DataFrame(
            schema={
                Col.type: pl.String,
                Col.currency: pl.String,
                Col.profit_total: pl.Float64,
                Col.profit_euro_total: pl.Float64,
                Col.profit_euro_net_total: pl.Float64,
                Col.withholding_tax_euro_total: pl.Float64,
                Col.kest_gross_total: pl.Float64,
                Col.kest_net_total: pl.Float64,
            }
        )

    seen_sections: set[str] = set()
    for section in sections:
        if section.name in seen_sections:
            raise ValueError(f"Duplicate IBKR summary section: {section.name}")
        seen_sections.add(section.name)

    merge_dfs = []
    for section in sections:
        config = section_configs[section.name]

        withholding_col = config["withholding_col"]
        withholding_expr = (
            pl.col(withholding_col).sum().round(FLOAT_PRECISION) if withholding_col else pl.lit(0.0)
        ).alias(Col.withholding_tax_euro_total)

        label_expr = pl.col(Col.type) if Col.type in section.df.columns else pl.lit(config["label"])

        section_summary_df = section.df.group_by(label_expr.alias(Col.type), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col(config["profit_euro_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col(config["profit_euro_net_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            withholding_expr,
            pl.col(Col.kest_gross_total).sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net_total).sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(section_summary_df)

    return pl.concat(merge_dfs, how="vertical_relaxed")
