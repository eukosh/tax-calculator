import logging
from datetime import date

import polars as pl

from src.const import (
    TransactionTypeIBKR,
)
from src.utils import calculate_kest, convert_to_euro, extract_elements, join_exchange_rates, read_xml_to_df

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s\n",
)


def apply_pivot(df: pl.DataFrame) -> pl.DataFrame:
    # Grouping keys
    index_keys = [
        "settle_date",
        "issuer_country_code",
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
            "amount_Withholding Tax": "withholding_tax",
            "amount_euro_Dividends": "dividends_euro",
            "amount_euro_Withholding Tax": "withholding_tax_euro",
        }
    ).with_columns(
        pl.col("withholding_tax").fill_null(0),
        pl.col("withholding_tax_euro").fill_null(0),
    )

    return pivoted_df


def process_cash_transactions_ibkr(
    xml_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    print("\n\n======================== Processing Cash Transactions ========================\n")
    cash_transactions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CashTransactions"), "CashTransaction"),
    )

    cash_transactions_df = cash_transactions_df.with_columns(
        pl.when(pl.col("type") == TransactionTypeIBKR.tax)
        .then(pl.col("amount").cast(pl.Float64).abs())
        .otherwise(pl.col("amount").cast(pl.Float64))
        .alias("amount"),
        pl.col("issuerCountryCode").alias("issuer_country_code"),
        pl.col("settleDate").str.strptime(pl.Date, "%Y-%m-%d").alias("settle_date"),
    ).filter(pl.col("settle_date").is_between(start_date, end_date))

    types = cash_transactions_df["type"].unique().to_list()
    logging.info(f"Transaction Types: {types}")
    sum_per_type_df = cash_transactions_df.group_by("type").agg(pl.col("amount").sum())
    logging.info(f"\nSum per Transaction Type:\n{sum_per_type_df}")

    # Filter for dividends/tax in Cash Transactions
    cash_transactions_df = cash_transactions_df.select(
        [
            "symbol",
            "currency",
            pl.when(pl.col("type") == TransactionTypeIBKR.pil)
            .then(pl.lit(TransactionTypeIBKR.dividend))
            .otherwise(pl.col("type"))
            .alias("type"),  # in Austria, PIL is the same as Dividend
            "amount",
            "settle_date",
            "issuer_country_code",
        ]
    ).filter(pl.col("type").is_in(["Dividends", "Withholding Tax"]))

    joined_df = join_exchange_rates(
        df=cash_transactions_df,
        rates_df=exchange_rates_df,
        df_date_col="settle_date",
    )
    # print("joined", joined_df.filter(pl.col("settle_date") == date(2023, 9, 15)))
    joined_df = convert_to_euro(joined_df, col_to_convert="amount")

    pivoted_df = apply_pivot(joined_df)
    print(pivoted_df)
    # previously it used withholding_tax instead of withholding_tax_euro, verify if it is correct now
    pivoted_df = calculate_kest(pivoted_df, amount_col="dividends_euro", tax_withheld_col="withholding_tax_euro")
    print(pivoted_df)
    # raise ValueError("Stop here")
    country_agg_df = (
        pivoted_df.group_by("issuer_country_code")
        .agg(
            pl.sum("dividends_euro").alias("dividends_euro_total"),
            pl.sum("dividends_euro_net").alias("dividends_euro_net_total"),
            pl.sum("withholding_tax_euro").alias("withholding_tax_euro_total"),
            pl.sum("kest_gross").alias("kest_gross_total"),
            pl.sum("kest_net").alias("kest_net_total"),
        )
        .sort("dividends_euro_total", descending=True)
    )
    print(country_agg_df)

    return country_agg_df


def process_bonds_ibkr(
    xml_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> [pl.DataFrame, pl.DataFrame]:
    print("\n\n======================== Processing Corporate Actions ========================\n")

    # Convert the extracted data into Polars DataFrames
    corporate_actions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CorporateActions"), "CorporateAction"),
    )

    corporate_actions_df = corporate_actions_df.select(
        [
            pl.col("reportDate").str.strptime(pl.Date, "%Y-%m-%d").alias("report_date"),
            "isin",
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            "currency",
            pl.col("proceeds").cast(pl.Float64),
            pl.col("fifoPnlRealized").cast(pl.Float64).alias("realized_pnl"),
        ]
    ).filter(pl.col("report_date").is_between(start_date, end_date))

    logging.debug("\nCorporate Actions DataFrame:\n", corporate_actions_df)

    joined_df = join_exchange_rates(
        df=corporate_actions_df,
        rates_df=exchange_rates_df,
        df_date_col="report_date",
    )

    joined_df = convert_to_euro(joined_df, "realized_pnl")
    tax_df = calculate_kest(joined_df, amount_col="realized_pnl_euro")

    tax_df = tax_df.select(
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
    ).sort("report_date", "isin")
    logging.info(tax_df)

    country_agg_df = (
        tax_df.group_by("issuer_country_code")
        .agg(
            pl.sum("realized_pnl_euro").alias("realized_pnl_euro_total"),
            pl.sum("realized_pnl_euro_net").alias("realized_pnl_euro_net_total"),
            pl.sum("kest_gross").alias("kest_gross_total"),
            pl.sum("kest_net").alias("kest_net_total"),
        )
        .sort("realized_pnl_euro_total", descending=True)
    )
    logging.info(country_agg_df)

    return tax_df, country_agg_df


def calculate_summary_ibkr(dividends_df: pl.DataFrame, bonds_df: pl.DataFrame) -> pl.DataFrame:
    dividends_summary_df = dividends_df.select(
        pl.lit("dividends").alias("type"),
        pl.col("dividends_euro_total").sum().alias("realized_pnl_euro_total"),
        pl.col("dividends_euro_net_total").sum().alias("realized_pnl_euro_net_total"),
        pl.col("withholding_tax_euro_total").sum().alias("withholding_tax_euro_total"),
        pl.col("kest_gross_total").sum().alias("kest_gross_total"),
        pl.col("kest_net_total").sum().alias("kest_net_total"),
    )

    bonds_summary_df = bonds_df.select(
        pl.lit("bonds").alias("type"),
        pl.col("realized_pnl_euro_total").sum().alias("realized_pnl_euro_total"),
        pl.col("realized_pnl_euro_net_total").sum().alias("realized_pnl_euro_net_total"),
        pl.lit(0.0).alias("withholding_tax_euro_total"),
        pl.col("kest_gross_total").sum().alias("kest_gross_total"),
        pl.col("kest_net_total").sum().alias("kest_net_total"),
    )

    return pl.concat([dividends_summary_df, bonds_summary_df], how="vertical_relaxed")
