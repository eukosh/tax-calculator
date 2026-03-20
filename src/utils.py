import glob
import json
import logging
from pathlib import Path
from typing import Callable, Sequence, TypeGuard, Union

import lxml.etree as etree
import polars as pl

from src.const import (
    EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET,
    FLOAT_PRECISION,
    KEST_RATE,
    MAX_DTT_RATE,
    Column,
    CurrencyCode,
)


def has_rows(df: pl.DataFrame | None) -> TypeGuard[pl.DataFrame]:
    return df is not None and not df.is_empty()


# Helper function to extract elements into a list of dictionaries
def extract_elements(parent, tag):
    return [{key: element.get(key) for key in element.keys()} for element in parent.findall(tag)]


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        json_data = json.load(f)
    return json_data


def resolve_input_file_paths(file_path: Union[str, Sequence[str]], *, suffix: str | None = None) -> list[str]:
    raw_paths = [file_path] if isinstance(file_path, str) else list(file_path)
    resolved_paths: list[str] = []
    seen: set[str] = set()

    for raw_path in raw_paths:
        path = Path(raw_path)
        if path.exists():
            if path.is_dir():
                pattern = f"*{suffix}" if suffix else "*"
                candidates = sorted(str(candidate) for candidate in path.glob(pattern))
            else:
                candidates = [str(path)]
        else:
            candidates = sorted(glob.glob(raw_path))

        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            resolved_paths.append(candidate)

    return resolved_paths


def read_xml_to_df(
    file_path: Union[str, Sequence[str]],
    xml_extract_func: Callable[[etree._Element], list[dict]],
    *,
    dedupe: bool = False,
) -> pl.DataFrame:
    """
    Reads XML files into a Polars DataFrame. Supports reading multiple files matching a wildcard pattern.

    Args:
        file_path: Path to an XML file, a wildcard pattern, a directory, or a list of those.

    Returns:
        pl.DataFrame: Combined DataFrame containing data from all matched XML files.
    """
    file_paths = resolve_input_file_paths(file_path, suffix=".xml")

    if not file_paths:
        raise FileNotFoundError(f"No files matched the pattern: {file_path}")

    # Read and combine all XML files into a single DataFrame
    dfs = []
    for path in file_paths:
        try:
            tree = etree.parse(path)
            root = tree.getroot()
            data = xml_extract_func(root)
            df = pl.DataFrame(data)

            dfs.append(df)
        except Exception as e:
            raise ValueError(f"Failed to read XML file at {path}: {e}")

    if not dfs:
        return pl.DataFrame()

    combined_df = pl.concat(dfs, how="vertical")
    return combined_df.unique(maintain_order=True) if dedupe else combined_df


def read_csv_to_df(file_path: Union[str, Sequence[str]]) -> pl.DataFrame:
    """
    Reads CSV files into a Polars DataFrame. Supports reading multiple files matching a wildcard pattern.

    Args:
        file_path: Path to a CSV file, a wildcard pattern, a directory, or a list of those.

    Returns:
        pl.DataFrame: Combined DataFrame containing data from all matched CSV files.
    """
    file_paths = resolve_input_file_paths(file_path, suffix=".csv")

    if not file_paths:
        raise FileNotFoundError(f"No files matched the pattern: {file_path}")

    # Read and combine all CSV files into a single DataFrame
    dfs = []
    for path in file_paths:
        try:
            df = pl.read_csv(path)  # Read the CSV file into a Polars DataFrame
            dfs.append(df)
        except Exception as e:
            raise ValueError(f"Failed to read CSV file at {path}: {e}")

    # Concatenate all DataFrames vertically (union)
    return pl.concat(dfs, how="vertical")


def join_exchange_rates(df: pl.DataFrame, rates_df: pl.DataFrame, df_date_col: str) -> pl.DataFrame:
    if Column.currency not in df.columns:
        raise ValueError("df is missing a 'currency' column.")

    rates_df_required_cols: set[str] = {Column.currency.value, Column.rate_date.value, Column.exchange_rate.value}
    rates_df_missing_cols = rates_df_required_cols - set(rates_df.columns)
    if rates_df_missing_cols != set():
        raise ValueError(f"rates_df is missing the following required columns: {rates_df_missing_cols}")

    # Ensure the all required currencies are present in rates_df
    df_currencies = set(df[Column.currency].unique().to_list())
    rates_df_currencies = set(rates_df[Column.currency].unique().to_list())
    rates_df_currencies.add(CurrencyCode.euro)  # We are converting to Euro thus we can assume its there

    rates_df_missing_currencies = df_currencies.difference(rates_df_currencies)
    if rates_df_missing_currencies:
        raise ValueError(f"rates_df is missing the following currencies: {rates_df_missing_currencies}")

    # order here is important for join_asof
    rates_df = rates_df.sort([Column.currency, Column.rate_date])
    df = df.sort([Column.currency, df_date_col])

    joined_df = df.join_asof(
        rates_df,
        left_on=df_date_col,
        right_on=Column.rate_date,
        by=Column.currency,
        strategy="backward",  # Fallback to the previous available date
    )

    # For non-EUR rows, null means we could not find any usable rate at or before transaction date.
    null_rate_df = joined_df.filter((pl.col(Column.currency) != CurrencyCode.euro) & pl.col(Column.rate_date).is_null())
    if not null_rate_df.is_empty():
        logging.error(f"\nFailed to match exchange rate for some non-EUR rows:\n{null_rate_df}")
        raise ValueError("Some dates did not match. See the logs above.")

    # Non-exact matches are allowed only within an acceptable offset.
    unmatched_dates_df = joined_df.filter(
        (pl.col(Column.currency) != CurrencyCode.euro) & (pl.col(Column.rate_date) != pl.col(df_date_col))
    )
    if unmatched_dates_df.shape[0] > 0:
        logging.warning(f"\nSome dates did not match exactly, double check:\n{unmatched_dates_df}")

        mismatches_in_acceptable_range_df = unmatched_dates_df.filter(
            pl.col(Column.rate_date).is_between(
                pl.col(df_date_col).dt.offset_by(f"-{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET}d"),
                pl.col(df_date_col).dt.offset_by(f"{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET}d"),
            )
        )

        if mismatches_in_acceptable_range_df.shape[0] != unmatched_dates_df.shape[0]:
            unacceptable_date_mismatch = unmatched_dates_df.join(
                mismatches_in_acceptable_range_df,
                on=unmatched_dates_df.columns,
                how="anti",
            )
            logging.error(
                "A few matched rates are outside acceptable range of +-{} days:\n{}".format(
                    EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET, unacceptable_date_mismatch
                ),
            )
            raise ValueError("Some dates did not match. See the logs above.")

        logging.warning(
            f"\nSome dates did not match exactly, but all are within +- {EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET} day.",
        )

    return joined_df


def convert_to_euro(df: pl.DataFrame, col_to_convert: Union[str, Sequence[str]]) -> pl.DataFrame:
    # Ensure col_to_convert is a list, even if a single string is provided
    if isinstance(col_to_convert, str):
        col_to_convert = [col_to_convert]

    cols_conversion_expr = [
        (
            pl.when(pl.col(Column.currency) != CurrencyCode.euro)
            .then((pl.col(col) / pl.col(Column.exchange_rate)).round(FLOAT_PRECISION))
            .otherwise(pl.col(col))
        ).alias(f"{col}_euro")
        for col in col_to_convert
    ]

    return df.with_columns(cols_conversion_expr)


def build_separate_trade_profit_loss_rows(
    totals_df: pl.DataFrame,
    profit_col: str = "trade_profit_euro_total",
    loss_col: str = "trade_loss_euro_total",
) -> list[pl.DataFrame]:
    """Build separate profit and loss summary rows with zero KESt for cross-broker offset."""
    frames: list[pl.DataFrame] = []

    profit_row_df = totals_df.select(
        pl.lit("trades profit").alias(Column.type),
        pl.lit(CurrencyCode.euro.value).alias(Column.currency),
        pl.col(profit_col).alias(Column.profit_total),
        pl.col(profit_col).alias(Column.profit_euro_total),
        pl.col(profit_col).alias(Column.profit_euro_net_total),
        pl.lit(0.0).alias(Column.withholding_tax_euro_total),
        pl.lit(0.0).alias(Column.kest_gross_total),
        pl.lit(0.0).alias(Column.kest_net_total),
    ).filter(pl.col(Column.profit_euro_total) != 0)
    if not profit_row_df.is_empty():
        frames.append(profit_row_df)

    loss_row_df = totals_df.select(
        pl.lit("trades loss").alias(Column.type),
        pl.lit(CurrencyCode.euro.value).alias(Column.currency),
        (-pl.col(loss_col)).round(FLOAT_PRECISION).alias(Column.profit_total),
        (-pl.col(loss_col)).round(FLOAT_PRECISION).alias(Column.profit_euro_total),
        (-pl.col(loss_col)).round(FLOAT_PRECISION).alias(Column.profit_euro_net_total),
        pl.lit(0.0).alias(Column.withholding_tax_euro_total),
        pl.lit(0.0).alias(Column.kest_gross_total),
        pl.lit(0.0).alias(Column.kest_net_total),
    ).filter(pl.col(Column.profit_euro_total) != 0)
    if not loss_row_df.is_empty():
        frames.append(loss_row_df)

    return frames


def calculate_kest(
    df: pl.DataFrame, amount_col: str, tax_withheld_col: str | None = None, net_col_name: str | None = None
) -> pl.DataFrame:
    # Net Austrian KESt=Austrian KESt on Gross amount − min(Foreign Withholding Tax,Treaty Rate × Gross Dividends)
    # keep in mind that witholding tax is negative number, it causes error in formula
    kest_gross = pl.col(amount_col) * KEST_RATE
    kest_net = (
        (kest_gross)
        - pl.min_horizontal(
            pl.col(tax_withheld_col),
            MAX_DTT_RATE * pl.col(amount_col),
        )
        if tax_withheld_col
        else kest_gross
    )
    df = df.with_columns(
        kest_gross=kest_gross,
        kest_net=kest_net,
    )
    amount_net = pl.col(amount_col) - pl.col("kest_net")
    if tax_withheld_col:
        amount_net = amount_net - pl.col(tax_withheld_col)
    return df.with_columns(
        amount_net.alias(net_col_name or f"{amount_col}_net"),
    )
