import logging
from datetime import date

import polars as pl

from src.const import FLOAT_PRECISION, TransactionTypeIBKR
from src.const import Column as Col
from src.utils import calculate_kest, convert_to_euro, extract_elements, join_exchange_rates, read_xml_to_df

IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


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
    print(f"\nPivoted DataFrame:\n{pivoted_df}")
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

    return pivoted_df


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


def process_trades_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
):
    logging.info("\n\n======================== Processing Trades ========================\n")
    trades_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//Trades"), "Lot"),
    )

    trades_df = (
        trades_df.rename({"openDateTime": Col.buy_date, "tradeDate": Col.trade_date, "fifoPnlRealized": Col.profit})
        .select(
            Col.symbol,
            pl.col("cost").cast(pl.Float64),
            Col.currency,
            pl.col(Col.buy_date).str.to_date(format=IBKR_DATETIME_FORMAT),
            pl.col(Col.trade_date).str.to_date(),
            pl.col(Col.profit).cast(pl.Float64),
        )
        .filter(pl.col(Col.trade_date).is_between(start_date, end_date))
        .with_columns((pl.col("cost") + pl.col(Col.profit)).alias(Col.proceeds))
    )
    print(trades_df)

    joined_df = join_exchange_rates(
        df=trades_df,
        rates_df=exchange_rates_df,
        df_date_col=[Col.buy_date],
    )

    joined_df = convert_to_euro(joined_df, col_to_convert=["cost", Col.proceeds])

    joined_df = joined_df.with_columns((pl.col(Col.proceeds_euro) - pl.col("cost_euro")).alias("euro_gain"))
    print(joined_df)

    trades_summary_df = joined_df.select(pl.col("euro_gain").sum().alias("trades_gain_euro")).with_columns(
        pl.col("trades_gain_euro").clip(lower_bound=0.0).alias("trades_gain_euro_clipped")
    )

    trades_tax_df = calculate_kest(
        df=trades_summary_df,
        amount_col="trades_gain_euro_clipped",  # already clamped to >= 0
        tax_withheld_col=None,
        net_col_name="net_trades_gain_euro",
    )

    return trades_tax_df.select(
        "trades_gain_euro",
        "net_trades_gain_euro",
        "kest_net",
    )


def process_cash_transactions_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    extract_etf_and_reit: bool = False,
) -> (pl.DataFrame, pl.DataFrame | None):
    logging.info("\n\n======================== Processing Cash Transactions ========================\n")
    cash_transactions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CashTransactions"), "CashTransaction"),
    )

    cash_transactions_df = (
        cash_transactions_df.rename({"subCategory": "sub_category", "actionID": "action_id"})
        .with_columns(
            pl.col("amount").cast(pl.Float64).alias("amount"),
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            pl.col("settleDate").str.strptime(pl.Date, "%Y-%m-%d").alias("settle_date"),
        )
        .filter(pl.col("settle_date").is_between(start_date, end_date))
    )
    cash_transactions_df = handle_dividend_adjustments(cash_transactions_df)

    types = cash_transactions_df["type"].unique().to_list()
    logging.info(f"Transaction Types: {types}")
    sum_per_type_df = cash_transactions_df.group_by("sub_category", "type").agg(pl.col("amount").sum())
    logging.info(f"\nSum per Transaction Category-Type:\n{sum_per_type_df}")

    # Filter for dividends/tax in Cash Transactions
    cash_transactions_df = cash_transactions_df.select(
        [
            "symbol",
            "sub_category",
            "currency",
            pl.when(pl.col("type") == TransactionTypeIBKR.pil)
            .then(pl.lit(TransactionTypeIBKR.dividend))
            .otherwise(pl.col("type"))
            .alias("type"),  # in Austria, PIL is the same as Dividend
            pl.col("amount").abs().alias("amount"),
            "settle_date",
            "issuer_country_code",
        ]
    ).filter(pl.col("type").is_in(["Dividends", "Withholding Tax"]))

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

    etf_reit_agg_df = None
    if extract_etf_and_reit:
        etf_reit_df = pivoted_df.filter(pl.col("sub_category").is_in(["REIT", "ETF"]))
        if not etf_reit_df.is_empty():
            pivoted_df = pivoted_df.filter(~pl.col("sub_category").is_in(["REIT", "ETF"]))

            etf_reit_agg_df = agg_final_transactions(etf_reit_df)
            logging.info("Dividends from REITs:\n{}".format(etf_reit_agg_df))

    country_agg_df = agg_final_transactions(pivoted_df) if not pivoted_df.is_empty() else None
    logging.info("Dividends by Country:\n{}".format(country_agg_df))

    return country_agg_df, etf_reit_agg_df


def process_bonds_ibkr(
    xml_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> [pl.DataFrame, pl.DataFrame]:
    logging.info("\n\n======================== Processing Corporate Actions ========================\n")

    # Convert the extracted data into Polars DataFrames
    corporate_actions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CorporateActions"), "CorporateAction"),
    )
    if corporate_actions_df.is_empty():
        logging.warning("No Corporate Actions found in the XML file.")
        return None, None

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

    logging.debug("\nCorporate Actions DataFrame: %s\n", corporate_actions_df)

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
    ).sort("realized_pnl", "isin", descending=True)
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
    dividends_df: pl.DataFrame, bonds_df: pl.DataFrame = None, reits_df: pl.DataFrame = None
) -> pl.DataFrame:
    merge_dfs = []
    if not dividends_df.is_empty():
        dividends_summary_df = dividends_df.group_by(pl.lit("dividends").alias("type"), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col("dividends_euro_total").sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col("dividends_euro_net_total").sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.col("withholding_tax_euro_total").sum().round(FLOAT_PRECISION).alias(Col.withholding_tax_euro_total),
            pl.col("kest_gross_total").sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col("kest_net_total").sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(dividends_summary_df)
    if bonds_df.is_empty():
        bonds_summary_df = bonds_df.group_by(pl.lit("bonds").alias("type"), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col(Col.profit_euro_total).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col(Col.profit_euro_net_total).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.lit(0.0).alias(Col.withholding_tax_euro_total),
            pl.col(Col.kest_gross_total).sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net_total).sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(bonds_summary_df)

    if reits_df is not None:
        reits_summary_df = reits_df.group_by(pl.lit("ETF/REIT div").alias("type"), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col("dividends_euro_total").sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col("dividends_euro_net_total").sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.col("withholding_tax_euro_total").sum().round(FLOAT_PRECISION).alias(Col.withholding_tax_euro_total),
            pl.col("kest_gross_total").sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col("kest_net_total").sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(reits_summary_df)
    return pl.concat(merge_dfs, how="vertical_relaxed")
