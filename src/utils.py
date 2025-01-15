import glob
import json
import logging
from typing import Callable, Sequence, Union

import polars as pl
from lxml import etree

from src.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET, KEST_RATE, MAX_DTT_RATE, Column, CurrencyCode


# Helper function to extract elements into a list of dictionaries
def extract_elements(parent, tag):
    return [{key: element.get(key) for key in element.keys()} for element in parent.findall(tag)]


def read_json(path: str) -> dict:
    with open(path, "r") as f:
        json_data = json.load(f)
    return json_data


def read_xml_to_df(file_path: str, xml_extract_func: Callable[[etree._Element], list[dict]]) -> pl.DataFrame:
    """
    Reads XML files into a Polars DataFrame. Supports reading multiple files matching a wildcard pattern.

    Args:
        file_path (str): Path to the XML file or a wildcard pattern (e.g., 'folder/File*').

    Returns:
        pl.DataFrame: Combined DataFrame containing data from all matched XML files.
    """
    # Find all matching files using the wildcard
    file_paths = glob.glob(file_path)

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

    return pl.concat(dfs, how="vertical")


def read_csv_to_df(file_path: str) -> pl.DataFrame:
    """
    Reads CSV files into a Polars DataFrame. Supports reading multiple files matching a wildcard pattern.

    Args:
        file_path (str): Path to the CSV file or a wildcard pattern (e.g., 'folder/File*').

    Returns:
        pl.DataFrame: Combined DataFrame containing data from all matched CSV files.
    """
    # Find all matching files using the wildcard
    file_paths = glob.glob(file_path)

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

    rates_df_required_cols = {Column.currency, Column.rate_date, Column.exchange_rate}
    rates_df_missing_cols = rates_df_required_cols - set(rates_df.columns)
    if rates_df_missing_cols != set():
        raise ValueError(f"rates_df is missing the following required columns: {rates_df_missing_cols}")

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

    # make sure it works when no conversion is needed
    unmatched_dates_df = joined_df.filter(
        (pl.col(Column.rate_date) != pl.col(df_date_col)) & (pl.col(Column.currency) != CurrencyCode.euro)
    )
    if unmatched_dates_df.shape[0] > 0:
        logging.warning(f"\nSome dates did not match, it might be okay, but double check:\n{unmatched_dates_df}")

        mismatches_in_acceptable_range_df = unmatched_dates_df.filter(
            pl.col(Column.rate_date).is_between(
                pl.col(df_date_col).dt.offset_by(f"-{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET}d"),
                pl.col(df_date_col).dt.offset_by(f"{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET}d"),
            )
        )

        if mismatches_in_acceptable_range_df.shape[0] == unmatched_dates_df.shape[0]:
            logging.warning(
                f"\nSome dates did not match, but ALL are within acceptable range of +-{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET} day ✅.",
            )
            return joined_df
        else:
            unacceptable_date_mismatch = unmatched_dates_df.join(
                mismatches_in_acceptable_range_df,
                on=unmatched_dates_df.columns,
                how="anti",
            )

            logging.error(
                "Unfortunatelly, a few dates are outside of the acceptable range of +-{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET} days:\n",
                unacceptable_date_mismatch,
            )

            raise ValueError("Some dates did not match. See the logs above.")

    return joined_df


def convert_to_euro(df: pl.DataFrame, col_to_convert: Union[str, Sequence[str]]) -> pl.DataFrame:
    # Ensure col_to_convert is a list, even if a single string is provided
    if isinstance(col_to_convert, str):
        col_to_convert = [col_to_convert]

    cols_conversion_expr = [
        (
            pl.when(pl.col("currency") != CurrencyCode.euro)
            .then(pl.col(col) / pl.col(Column.exchange_rate))
            .otherwise(pl.col(col))
        ).alias(f"{col}_euro")
        for col in col_to_convert
    ]

    return df.with_columns(cols_conversion_expr)


def calculate_kest(df: pl.DataFrame, amount_col: str, tax_withheld_col: str = None) -> pl.DataFrame:
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
        amount_net.alias(f"{amount_col}_net"),
    )
