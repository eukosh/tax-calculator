import logging
from datetime import date
from pathlib import Path

import polars as pl

from src.const import FLOAT_PRECISION, CurrencyCode
from src.const import Column as Col
from src.const import CorporateActionTypesFF
from src.utils import calculate_kest, convert_to_euro, join_exchange_rates, read_json

EMPTY_VALUE = "-"
EX_DATE_COL = "ex_date"
TRADE_OPERATION_COL = "operation"
ABS_EPSILON = 1e-9

TICKERS_WITHHOLDING_ZERO_TAX = ["TLT.US"]

SUMMARY_COLUMNS = [
    Col.type,
    Col.currency,
    Col.profit_total,
    Col.profit_euro_total,
    Col.profit_euro_net_total,
    Col.withholding_tax_euro_total,
    Col.kest_gross_total,
    Col.kest_net_total,
]

DIVIDENDS_SCHEMA = {
    Col.date: pl.Date,
    EX_DATE_COL: pl.Date,
    Col.type: pl.String,
    Col.corporate_action_id: pl.String,
    Col.ticker: pl.String,
    Col.currency: pl.String,
    Col.amount: pl.Float64,
    Col.withholding_tax: pl.Float64,
    Col.shares_count: pl.Float64,
    Col.amount_per_share: pl.Float64,
}

TRADES_SCHEMA = {
    Col.trade_date: pl.Date,
    Col.ticker: pl.String,
    Col.currency: pl.String,
    TRADE_OPERATION_COL: pl.String,
    Col.profit: pl.Float64,
}


def _assert_required_columns(df: pl.DataFrame, required_columns: set[str], section_name: str) -> None:
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{section_name} is missing required columns: {sorted(missing_columns)}")


def _load_corporate_actions_df(statement: dict, start_date: date, end_date: date) -> pl.DataFrame:
    """
    1. Read Freedom corporate actions and return an empty typed dataframe when the section is missing.
    2. Validate required columns before any tax logic is applied.
    3. Fail fast when currencies disagree inside one corporate action record.
    4. Normalize types (dates, amount, withholding tax) into a strict schema.
    5. Keep only records whose `ex_date` belongs to the reporting period.
    """
    corporate_actions = (statement.get("corporate_actions") or {}).get("detailed") or []
    if not corporate_actions:
        return pl.DataFrame(schema=DIVIDENDS_SCHEMA)

    corporate_actions_df = pl.DataFrame(corporate_actions)
    _assert_required_columns(
        corporate_actions_df,
        {
            Col.date,
            EX_DATE_COL,
            "type_id",
            Col.corporate_action_id,
            Col.ticker,
            Col.currency,
            Col.amount,
            "tax_amount",
            "q_on_ex_date",
            "amount_per_one",
            "external_tax_currency",
            "tax_currency",
        },
        section_name="Freedom corporate_actions.detailed",
    )

    tax_currency_expr = pl.col("tax_currency").cast(pl.String).fill_null("")
    unmatched_currencies = corporate_actions_df.filter(
        (pl.col(Col.currency) != pl.col("external_tax_currency"))
        | ((pl.col(Col.currency) != tax_currency_expr) & (~tax_currency_expr.is_in([EMPTY_VALUE, ""])))
    )
    if not unmatched_currencies.is_empty():
        logging.error("Some currencies do not match: %s", unmatched_currencies)
        raise ValueError("Some currencies do not match")

    return (
        corporate_actions_df.select(
            pl.col(Col.date).str.to_date("%Y-%m-%d"),
            pl.col(EX_DATE_COL).str.to_date("%Y-%m-%d").alias(EX_DATE_COL),
            pl.col("type_id").alias(Col.type),
            pl.col(Col.corporate_action_id).cast(pl.String),
            pl.col(Col.ticker).cast(pl.String),
            pl.col(Col.currency).cast(pl.String),
            pl.col(Col.amount).cast(pl.Float64),
            pl.col("tax_amount")
            .cast(pl.String)
            .str.replace(f"^{EMPTY_VALUE}$", "0")
            .fill_null("0")
            .cast(pl.Float64)
            .alias(Col.withholding_tax),
            pl.col("q_on_ex_date").cast(pl.Float64, strict=False).fill_null(0.0).alias(Col.shares_count),
            pl.col("amount_per_one").cast(pl.Float64, strict=False).fill_null(0.0).alias(Col.amount_per_share),
        )
        # ATTENTION:
        # 1. Period cut is currently based on ex_date to keep FF reversal chains together by corporate_action_id.
        # 2. FF can retroactively rewrite previous events (dividend -> dividend_reverted + corrected dividend later).
        # 3. This is a pragmatic workaround; Austrian tax timing is cash-date based, so this should be revisited.
        .filter(pl.col(EX_DATE_COL).is_between(start_date, end_date))
        .cast(DIVIDENDS_SCHEMA)
    )


def _handle_uncanceled_reverted_dividends(dividends_df: pl.DataFrame, reverted_df: pl.DataFrame) -> pl.DataFrame:
    # TODO(next filing cycle):
    # Reassess and remove this FF-specific reconciliation if statement data remains stable:
    # 1. no cross-year dividend_reverted -> corrected dividend chains,
    # 2. no duplicate correction triples per corporate_action_id,
    # 3. zero-tax instruments (e.g. TLT.US) consistently arrive with zero withholding tax.
    # If all conditions hold for the full reporting year, delete backfill/duplicate workaround logic.
    # FF quirk for some zero-tax instruments (e.g. TLT):
    # 1. Initial dividend may appear with withholding tax.
    # 2. Later FF posts dividend_reverted and a corrected dividend entry.
    # 3. Around year boundaries these legs can split across statements, so backfill is needed.
    reverted_agg_df = reverted_df.group_by(Col.ticker, Col.corporate_action_id).agg(
        pl.sum(Col.amount).alias(Col.amount),
        pl.sum(Col.withholding_tax).alias(Col.withholding_tax),
    )

    uncanceled_reverted_df = reverted_agg_df.filter(
        (pl.col(Col.amount).abs() > ABS_EPSILON) | (pl.col(Col.withholding_tax).abs() > ABS_EPSILON)
    )
    if uncanceled_reverted_df.is_empty():
        return dividends_df

    dividend_ids = dividends_df[Col.corporate_action_id].to_list()
    backfill_df = reverted_df.filter(
        ~pl.col(Col.corporate_action_id).is_in(dividend_ids)
        & pl.col(Col.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX)
        & (pl.col(Col.withholding_tax) != 0)
        & (pl.col(Col.amount) > 0)
    )

    if backfill_df[Col.corporate_action_id].n_unique() != backfill_df.height:
        raise ValueError("Reverted dividend backfill expects at most one row per corporate_action_id")

    handled_ids = set(dividend_ids) | set(backfill_df[Col.corporate_action_id].to_list())
    unresolved_df = uncanceled_reverted_df.filter(~pl.col(Col.corporate_action_id).is_in(list(handled_ids)))
    if not unresolved_df.is_empty():
        logging.error("Unresolved reverted dividends: %s", unresolved_df)
        raise ValueError("Some reverted dividends are unresolved for the selected reporting period")

    if backfill_df.is_empty():
        return dividends_df

    logging.info(
        "FF reversal reconciliation: backfilling corrected dividends for corporate_action_id values missing in dividend rows"
    )
    logging.warning("Backfilling reverted dividends for zero-tax tickers: %s", backfill_df)
    return pl.concat([dividends_df, backfill_df], how="vertical")


def _resolve_duplicate_dividends(dividends_df: pl.DataFrame, reverted_df: pl.DataFrame) -> pl.DataFrame:
    duplicate_ids = (
        dividends_df.group_by(Col.corporate_action_id)
        .agg(pl.len().alias("row_count"))
        .filter(pl.col("row_count") > 1)[Col.corporate_action_id]
        .to_list()
    )
    if not duplicate_ids:
        return dividends_df

    reverted_ids = set(reverted_df[Col.corporate_action_id].to_list())
    duplicates_df = dividends_df.filter(pl.col(Col.corporate_action_id).is_in(duplicate_ids))
    invalid_duplicates_df = duplicates_df.filter(
        ~pl.col(Col.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX)
        | ~pl.col(Col.corporate_action_id).is_in(list(reverted_ids))
    )
    if not invalid_duplicates_df.is_empty():
        logging.error("Unexpected duplicate dividend records: %s", invalid_duplicates_df)
        raise ValueError("Duplicate corporate_action_id found in dividends")

    preferred_duplicates_df = (
        duplicates_df.with_columns(pl.col(Col.withholding_tax).abs().alias("_abs_withholding"))
        .sort(
            [Col.corporate_action_id, "_abs_withholding", Col.date, Col.amount],
            descending=[False, False, True, True],
        )
        .unique(subset=[Col.corporate_action_id], keep="first")
        .drop("_abs_withholding")
    )

    non_duplicate_df = dividends_df.filter(~pl.col(Col.corporate_action_id).is_in(duplicate_ids))
    return pl.concat([non_duplicate_df, preferred_duplicates_df], how="vertical")


def _apply_dividend_exclusions(dividends_df: pl.DataFrame, exclude_file_path: str | None) -> pl.DataFrame:
    if exclude_file_path is None:
        return dividends_df

    exclusion_path = Path(exclude_file_path)
    if not exclusion_path.exists():
        raise FileNotFoundError(f"Exclusion file does not exist: {exclude_file_path}")

    exclusion_df = pl.read_csv(exclusion_path)
    if Col.corporate_action_id not in exclusion_df.columns:
        raise ValueError("Exclusion file must include a corporate_action_id column")

    excluded_ids = (
        exclusion_df[Col.corporate_action_id].cast(pl.String).drop_nulls().unique(maintain_order=True).to_list()
    )
    if not excluded_ids:
        return dividends_df

    filtered_df = dividends_df.filter(~pl.col(Col.corporate_action_id).is_in(excluded_ids))
    logging.info(
        "Excluded %s dividend rows using %s",
        dividends_df.height - filtered_df.height,
        exclude_file_path,
    )
    return filtered_df


def _prepare_dividends_df(
    corporate_actions_df: pl.DataFrame,
    exclude_corporate_action_ids_file: str | None,
) -> pl.DataFrame:
    """
    1. Split dividend and dividend_reverted rows for the selected period.
    2. Backfill zero-tax reverted rows when final dividend rows are not yet present.
    3. Resolve valid duplicate dividend ids produced by correction triples.
    4. Apply optional local exclusions by corporate action id.
    5. Fail fast if duplicate ids remain after normalization.
    """
    if corporate_actions_df.is_empty():
        return pl.DataFrame(schema=DIVIDENDS_SCHEMA)

    dividends_df = corporate_actions_df.filter(pl.col(Col.type) == CorporateActionTypesFF.dividend)
    reverted_df = corporate_actions_df.filter(pl.col(Col.type) == CorporateActionTypesFF.dividend_reverted)

    if not reverted_df.is_empty():
        dividends_df = _handle_uncanceled_reverted_dividends(dividends_df=dividends_df, reverted_df=reverted_df)
        dividends_df = _resolve_duplicate_dividends(dividends_df=dividends_df, reverted_df=reverted_df)

    dividends_df = _apply_dividend_exclusions(dividends_df, exclude_corporate_action_ids_file)
    if dividends_df[Col.corporate_action_id].n_unique() != dividends_df.height:
        raise ValueError("Duplicate corporate_action_id found in final dividends dataframe")

    return dividends_df


def _summarize_dividends(
    dividends_df: pl.DataFrame,
    exchange_rates_df: pl.DataFrame,
    incorrect_withholding_tax_output_file: str | None,
) -> pl.DataFrame | None:
    if dividends_df.is_empty():
        return None

    incorrect_withholding_df = dividends_df.filter(
        pl.col(Col.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX) & (pl.col(Col.withholding_tax) != 0)
    )
    if not incorrect_withholding_df.is_empty():
        logging.warning("Incorrect withholding tax for zero-tax tickers: %s", incorrect_withholding_df)
        if incorrect_withholding_tax_output_file:
            incorrect_withholding_df.write_csv(incorrect_withholding_tax_output_file)

    gross_recovered_df = (
        dividends_df.with_columns(pl.col(Col.withholding_tax).abs().alias(Col.withholding_tax))
        .with_columns(
            (pl.col(Col.amount) + pl.col(Col.withholding_tax)).alias(Col.amount),
            pl.when(pl.col(Col.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX) & (pl.col(Col.withholding_tax) != 0))
            .then(0.0)
            .otherwise(pl.col(Col.withholding_tax))
            .alias(Col.withholding_tax),
        )
        .sort(EX_DATE_COL)
    )

    joined_df = join_exchange_rates(
        df=gross_recovered_df,
        rates_df=exchange_rates_df,
        df_date_col=EX_DATE_COL,
    )
    joined_df = convert_to_euro(joined_df, col_to_convert=[Col.amount, Col.withholding_tax])
    tax_df = calculate_kest(joined_df, amount_col=Col.amount_euro, tax_withheld_col=Col.withholding_tax_euro)

    return (
        tax_df.group_by(Col.currency)
        .agg(
            pl.sum(Col.amount).round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.sum(Col.amount_euro).round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.sum(Col.amount_euro_net).round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.sum(Col.withholding_tax_euro).round(FLOAT_PRECISION).alias(Col.withholding_tax_euro_total),
            pl.sum(Col.kest_gross).round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.sum(Col.kest_net).round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        .with_columns(pl.lit("dividends").alias(Col.type))
        .select(SUMMARY_COLUMNS)
    )


def _load_trades_df(statement: dict, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date) -> pl.DataFrame:
    """
    1. Read Freedom trades and return an empty typed dataframe when no trades are present.
    2. Validate trade columns and normalize dates/currencies/profit values.
    3. Keep only rows in the reporting period and exclude FX conversion pairs.
    4. Join FX rates by trade date and convert realized trade profit to EUR.
    5. Return normalized per-trade rows ready for tax aggregation.
    """
    trades_section = statement.get("trades")
    trades_raw = []
    if isinstance(trades_section, dict):
        trades_raw = trades_section.get("detailed") or []

    if not trades_raw:
        return pl.DataFrame(schema={**TRADES_SCHEMA, Col.profit_euro: pl.Float64})

    trades_df = pl.DataFrame(trades_raw)
    _assert_required_columns(
        trades_df,
        {"short_date", "instr_nm", "curr_c", TRADE_OPERATION_COL},
        section_name="Freedom trades.detailed",
    )
    has_fifo_profit = "fifo_profit" in trades_df.columns
    has_profit = "profit" in trades_df.columns
    if not has_fifo_profit and not has_profit:
        raise ValueError("Freedom trades must include at least one of: fifo_profit, profit")

    if has_fifo_profit and has_profit:
        fifo_profit_expr = pl.col("fifo_profit").cast(pl.Float64, strict=False)
        profit_expr = pl.col("profit").cast(pl.Float64, strict=False)
        realized_profit_expr = (
            pl.when(fifo_profit_expr.is_not_null() & (fifo_profit_expr.abs() > ABS_EPSILON))
            .then(fifo_profit_expr)
            .otherwise(profit_expr)
            .alias(Col.profit)
        )
    elif has_fifo_profit:
        realized_profit_expr = pl.col("fifo_profit").cast(pl.Float64, strict=False).alias(Col.profit)
    else:
        realized_profit_expr = pl.col("profit").cast(pl.Float64, strict=False).alias(Col.profit)

    trades_df = (
        trades_df.select(
            pl.col("short_date").str.to_date("%Y-%m-%d").alias(Col.trade_date),
            pl.col("instr_nm").cast(pl.String).alias(Col.ticker),
            pl.col("curr_c").cast(pl.String).alias(Col.currency),
            pl.col(TRADE_OPERATION_COL).cast(pl.String).str.to_lowercase().alias(TRADE_OPERATION_COL),
            realized_profit_expr,
        )
        .filter(pl.col(Col.trade_date).is_between(start_date, end_date))
        .filter(pl.col(Col.profit).is_not_null())
        .filter(~pl.col(Col.ticker).str.to_uppercase().str.contains(r"^[A-Z]{3}/[A-Z]{3}$"))
        .filter(pl.col(Col.profit) != 0)
    )

    if trades_df.is_empty():
        return pl.DataFrame(schema={**TRADES_SCHEMA, Col.profit_euro: pl.Float64})

    joined_df = join_exchange_rates(df=trades_df, rates_df=exchange_rates_df, df_date_col=Col.trade_date)
    return convert_to_euro(joined_df, col_to_convert=Col.profit)


def _summarize_trades(trades_df: pl.DataFrame) -> pl.DataFrame | None:
    if trades_df.is_empty():
        return None

    totals_df = trades_df.select(
        pl.col(Col.profit_euro).sum().fill_null(0.0).round(FLOAT_PRECISION).alias(Col.profit_euro_total),
    ).with_columns(
        pl.col(Col.profit_euro_total).clip(lower_bound=0.0).alias("taxable_profit_euro"),
    )

    trades_tax_df = calculate_kest(
        df=totals_df,
        amount_col="taxable_profit_euro",
        tax_withheld_col=None,
        net_col_name="taxable_profit_euro_net",
    )

    return trades_tax_df.select(
        pl.lit("trades").alias(Col.type),
        pl.lit(CurrencyCode.euro.value).alias(Col.currency),
        pl.col(Col.profit_euro_total).alias(Col.profit_total),
        pl.col(Col.profit_euro_total),
        (pl.col(Col.profit_euro_total) - pl.col(Col.kest_net)).round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
        pl.lit(0.0).alias(Col.withholding_tax_euro_total),
        pl.col(Col.kest_gross).round(FLOAT_PRECISION).alias(Col.kest_gross_total),
        pl.col(Col.kest_net).round(FLOAT_PRECISION).alias(Col.kest_net_total),
    )


def process_freedom_statement(
    json_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    exclude_corporate_action_ids_file: str | None = None,
    incorrect_withholding_tax_output_file: str | None = None,
) -> pl.DataFrame:
    """
    1. Load Freedom statement sections and normalize corporate actions/trades for the reporting period.
    2. Build normalized dividend events (reversal reconciliation, duplicate correction handling, optional exclusions).
    3. Compute dividend tax summary in EUR using `ex_date` as the date anchor for period + FX matching.
    4. Build realized trades summary from trade profit fields, excluding FX conversion pairs.
    5. Merge dividend and trade summaries into one provider summary schema.
    6. Return an empty typed summary when no taxable rows are present.
    """
    print("\n\n======================== Processing Freedom Finance Statement ========================\n")

    statement = read_json(json_file_path)
    corporate_actions_df = _load_corporate_actions_df(
        statement=statement,
        start_date=start_date,
        end_date=end_date,
    )
    dividends_df = _prepare_dividends_df(
        corporate_actions_df=corporate_actions_df,
        exclude_corporate_action_ids_file=exclude_corporate_action_ids_file,
    )
    dividends_summary_df = _summarize_dividends(
        dividends_df=dividends_df,
        exchange_rates_df=exchange_rates_df,
        incorrect_withholding_tax_output_file=incorrect_withholding_tax_output_file,
    )

    trades_df = _load_trades_df(
        statement=statement,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
    )
    trades_summary_df = _summarize_trades(trades_df)

    summary_frames = [df for df in [dividends_summary_df, trades_summary_df] if df is not None and not df.is_empty()]
    if not summary_frames:
        return pl.DataFrame(
            schema={
                Col.type.value: pl.String,
                Col.currency.value: pl.String,
                Col.profit_total.value: pl.Float64,
                Col.profit_euro_total.value: pl.Float64,
                Col.profit_euro_net_total.value: pl.Float64,
                Col.withholding_tax_euro_total.value: pl.Float64,
                Col.kest_gross_total.value: pl.Float64,
                Col.kest_net_total.value: pl.Float64,
            }
        )

    summary_df = pl.concat(summary_frames, how="vertical_relaxed").sort([Col.type, Col.currency])
    logging.info("Freedom Finance Summary: %s", summary_df)
    return summary_df.select(SUMMARY_COLUMNS)
