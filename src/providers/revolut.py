import logging
from datetime import date
from typing import Final

import polars as pl

from src.const import FLOAT_PRECISION, Column, CurrencyCode, RevolutColumn, RevolutType
from src.precision import cast_decimal_columns_to_float
from src.utils import calculate_kest, convert_to_euro, join_exchange_rates

_IGNORED_PREFIXES: Final[tuple[str, ...]] = (
    "BUY",
    "SELL",
    "Interest Reinvested",
    "Interest WITHDRAWN",
)


def _parse_currency_from_value_col_name(value_col_name: str) -> CurrencyCode:
    try:
        # Splits "Value, EUR" into ["Value", " EUR"] and gets the currency part
        currency_str = value_col_name.split(",", maxsplit=1)[1].strip().upper()
        return CurrencyCode(currency_str)
    except (IndexError, ValueError) as error:
        raise ValueError(f"Could not parse currency from column name: '{value_col_name}'.") from error


def _infer_statement_currency_from_description(statement_df: pl.DataFrame) -> CurrencyCode:
    currencies = (
        statement_df.select(pl.col("Description").str.extract(r"\b(EUR|USD)\b Class", group_index=1).alias("currency"))
        .drop_nulls()
        .get_column("currency")
        .unique()
        .to_list()
    )

    if len(currencies) == 1:
        return CurrencyCode(currencies[0])
    if len(currencies) > 1:
        raise ValueError(
            "Revolut statement contains multiple currencies in descriptions. "
            "Process each currency statement separately."
        )

    raise ValueError("Could not infer statement currency from Revolut descriptions.")


def _resolve_value_column_and_currency(statement_df: pl.DataFrame) -> tuple[str, CurrencyCode]:
    named_value_columns = [column for column in statement_df.columns if column.startswith("Value,")]
    if len(named_value_columns) == 1:
        value_col_name = named_value_columns[0]
        return value_col_name, _parse_currency_from_value_col_name(value_col_name)

    if len(named_value_columns) > 1:
        statement_currency = _infer_statement_currency_from_description(statement_df)
        value_col_name = f"Value, {statement_currency.value}"
        if value_col_name not in named_value_columns:
            raise ValueError(
                f"Expected value column '{value_col_name}' for inferred statement currency '{statement_currency.value}', "
                f"but available value columns are: {named_value_columns}."
            )
        return value_col_name, statement_currency

    if "Value" in statement_df.columns:
        return "Value", _infer_statement_currency_from_description(statement_df)

    raise ValueError(
        "Critical Error: Could not find a supported value column in the Revolut statement. "
        "Expected 'Value' or a column starting with 'Value,'."
    )


def _parse_amount_expr(value_col_name: str) -> pl.Expr:
    return (
        pl.col(value_col_name)
        .cast(pl.String)
        .str.replace_all(r"[\u202f\u00a0]", "")
        .str.replace_all(",", "")
        .str.replace_all(r"[^0-9\.\-]", "")
        .cast(pl.Float64)
    )


def _date_expr() -> pl.Expr:
    return (
        pl.col("Date")
        .cast(pl.String)
        .str.replace_all(r"[\u202f\u00a0]", " ")
        .str.to_datetime(format="%b %e, %Y, %I:%M:%S %p")
        .dt.date()
    )


def _type_expr() -> pl.Expr:
    description_col = pl.col("Description")
    ignored_condition = pl.any_horizontal([description_col.str.starts_with(prefix) for prefix in _IGNORED_PREFIXES])
    return (
        pl.when(description_col.str.starts_with("Interest PAID"))
        .then(pl.lit(RevolutType.interest.value))
        .when(description_col.str.starts_with("Service Fee Charged"))
        .then(pl.lit(RevolutType.fee.value))
        .when(ignored_condition)
        .then(pl.lit("ignored"))
        .otherwise(pl.lit("unknown"))
    )


def _raise_if_unknown_descriptions(processed_statement_df: pl.DataFrame) -> None:
    unknown_descriptions = (
        processed_statement_df.filter(pl.col(RevolutColumn.type) == "unknown")
        .get_column("description")
        .unique()
        .to_list()
    )
    if unknown_descriptions:
        raise ValueError(
            "Unsupported Revolut savings statement description(s): " + ", ".join(sorted(unknown_descriptions))
        )


def process_revolut_savings_statement(
    csv_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    """
    1. Resolve the statement currency and correct value column for the Revolut CSV format.
    2. Parse and classify rows, fail fast on unknown description types, and keep only taxable rows.
    3. Aggregate daily taxable profit, convert to EUR, compute KESt, and return the summary schema.
    """
    logging.info(
        "In the current implementation it is not ready to properly process from euro and usd accounts combined."
    )
    print("\n\n======================== Processing Revolut Savings Statement ========================\n")

    # Convert the extracted data into Polars DataFrames
    statement_df = pl.read_csv(csv_file_path)
    value_col_name, currency = _resolve_value_column_and_currency(statement_df)

    processed_statement_df = statement_df.select(
        [
            _date_expr().alias(Column.date),
            pl.col("Description").alias("description"),
            _type_expr().alias(RevolutColumn.type),
            pl.lit(currency).alias(Column.currency),
            _parse_amount_expr(value_col_name).alias(RevolutColumn.amount),
        ]
    ).filter(pl.col(Column.date).is_between(start_date, end_date))

    _raise_if_unknown_descriptions(processed_statement_df)

    logging.debug("\nProcessed Statement Df: %s\n", processed_statement_df)

    fees_interest_df = processed_statement_df.filter(
        pl.col(RevolutColumn.type).is_in([RevolutType.fee.value, RevolutType.interest.value])
    )

    # here it will be net profit since negative fee is accrued on the same day in the same currency
    profit_by_date_df = fees_interest_df.group_by(Column.date, Column.currency).agg(
        pl.sum(RevolutColumn.amount).alias(Column.profit)
    )
    logging.debug("\nprofit_by_date_df:  %s\n", profit_by_date_df)

    joined_df = join_exchange_rates(
        df=profit_by_date_df,
        rates_df=exchange_rates_df,
        df_date_col=Column.date,
    )
    profit_euro_df = convert_to_euro(joined_df, Column.profit)
    logging.debug("\nprofit_euro_df: %s\n", profit_euro_df)

    tax_df = calculate_kest(profit_euro_df, amount_col=Column.profit_euro)
    logging.debug("\ntax_df:  %s\n", tax_df)

    # once i update this func to process both usd and euro accounts, i will need to add a group by currency here or deal only with euro amounts
    summary_df = tax_df.select(
        pl.first(Column.currency),
        pl.sum(Column.profit).round(FLOAT_PRECISION).alias(Column.profit_total),
        pl.sum(Column.profit_euro).round(FLOAT_PRECISION).alias(Column.profit_euro_total),
        pl.sum(Column.profit_euro_net).round(FLOAT_PRECISION).alias(Column.profit_euro_net_total),
        pl.lit(0.0).alias(Column.withholding_tax_euro_total),
        pl.sum(Column.kest_gross).round(FLOAT_PRECISION).alias(Column.kest_gross_total),
        pl.sum(Column.kest_net).round(FLOAT_PRECISION).alias(Column.kest_net_total),
    ).sort(Column.profit_euro_total, descending=True)

    logging.info(summary_df)

    return cast_decimal_columns_to_float(summary_df)
