import logging
from dataclasses import dataclass
from datetime import date
from typing import Literal

import polars as pl

from src.const import FLOAT_PRECISION, CurrencyCode, TransactionTypeIBKR
from src.const import Column as Col
from src.utils import calculate_kest, convert_to_euro, extract_elements, has_rows, join_exchange_rates, read_xml_to_df

IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


SummarySectionName = Literal["dividends", "bonds", "reit_dividends", "trades"]


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


def process_trades_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load closed trade lots from XML and keep only fields needed for tax math.
    2. Filter lots by reporting trade date and compute proceeds as cost + realized PnL.
    3. Join buy-date FX for cost conversion and sell-date FX for proceeds conversion.
    4. Compute EUR cost, EUR proceeds, and EUR realized PnL per trade.
    5. Build detailed per-trade output for audit/debug and reporting files.
    6. Aggregate total EUR PnL, clip taxable base at zero, and compute KESt.
    7. Return detail dataframe and summary dataframe used by the IBKR summary merge.
    """
    logging.info("\n\n======================== Processing Trades ========================\n")
    trades_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//Trades"), "Lot"),
    )
    if trades_df.is_empty():
        logging.warning("No Trades found in the XML file.")
        return None, None

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
    if trades_df.is_empty():
        logging.warning("No Trades matched the selected date range.")
        return None, None

    joined_df = join_exchange_rates(
        df=trades_df,
        rates_df=exchange_rates_df,
        df_date_col=Col.buy_date,
    ).select(*trades_df.columns, pl.col(Col.exchange_rate).alias("buy_exchange_rate"))

    joined_df = join_exchange_rates(
        df=joined_df,
        rates_df=exchange_rates_df,
        df_date_col=Col.trade_date,
    ).select(*joined_df.columns, pl.col(Col.exchange_rate).alias("sell_exchange_rate"))

    joined_df = joined_df.with_columns(
        (
            pl.when(pl.col(Col.currency) != CurrencyCode.euro)
            .then((pl.col("cost") / pl.col("buy_exchange_rate")).round(FLOAT_PRECISION))
            .otherwise(pl.col("cost"))
        ).alias("cost_euro"),
        (
            pl.when(pl.col(Col.currency) != CurrencyCode.euro)
            .then((pl.col(Col.proceeds) / pl.col("sell_exchange_rate")).round(FLOAT_PRECISION))
            .otherwise(pl.col(Col.proceeds))
        ).alias(Col.proceeds_euro),
    ).with_columns((pl.col(Col.proceeds_euro) - pl.col("cost_euro")).round(FLOAT_PRECISION).alias(Col.profit_euro))

    trades_detail_df = joined_df.select(
        Col.symbol,
        Col.currency,
        Col.buy_date,
        Col.trade_date,
        "buy_exchange_rate",
        "sell_exchange_rate",
        "cost",
        "cost_euro",
        Col.proceeds,
        Col.proceeds_euro,
        Col.profit,
        Col.profit_euro,
    )

    trades_totals_df = trades_detail_df.select(
        pl.col(Col.profit_euro).sum().fill_null(0.0).round(FLOAT_PRECISION).alias(Col.profit_euro_total),
    ).with_columns(
        pl.col(Col.profit_euro_total).alias(Col.profit_total),
        pl.col(Col.profit_euro_total).clip(lower_bound=0.0).alias("taxable_profit_euro"),
    )

    trades_tax_df = calculate_kest(
        df=trades_totals_df,
        amount_col="taxable_profit_euro",
        tax_withheld_col=None,
        net_col_name="taxable_profit_euro_net",
    )

    trades_summary_df = trades_tax_df.select(
        pl.lit(CurrencyCode.euro.value).alias(Col.currency),
        pl.col(Col.profit_total),
        pl.col(Col.profit_euro_total),
        (pl.col(Col.profit_euro_total) - pl.col(Col.kest_net)).round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
        pl.lit(0.0).alias(Col.withholding_tax_euro_total),
        pl.col(Col.kest_gross).round(FLOAT_PRECISION).alias(Col.kest_gross_total),
        pl.col(Col.kest_net).round(FLOAT_PRECISION).alias(Col.kest_net_total),
    )

    return trades_detail_df, trades_summary_df


def process_cash_transactions_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    extract_etf_and_reit: bool = False,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load IBKR cash transactions from XML and normalize key columns/types.
    2. Filter by settle date and collapse adjustment records by action/type group.
    3. Keep only dividend/tax cash flows and normalize IBKR-specific PIL type.
    4. Convert withholding tax to absolute amount while keeping dividend sign as-is.
    5. Join FX by settle date and convert original amounts to EUR.
    6. Pivot dividend/tax rows into one row per security/date and compute KESt fields.
    7. Optionally split ETF/REIT rows into a separate aggregate bucket.
    8. Aggregate final country-level totals and return main + optional ETF/REIT outputs.
    """
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
    cash_transactions_df = (
        cash_transactions_df.select(
            [
                "symbol",
                "sub_category",
                "currency",
                pl.when(pl.col("type") == TransactionTypeIBKR.pil)
                .then(pl.lit(TransactionTypeIBKR.dividend))
                .otherwise(pl.col("type"))
                .alias("type"),  # in Austria, PIL is the same as Dividend
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

    etf_reit_agg_df = None
    if extract_etf_and_reit:
        etf_reit_df = pivoted_df.filter(pl.col("sub_category").is_in(["REIT", "ETF"]))
        if has_rows(etf_reit_df):
            pivoted_df = pivoted_df.filter(~pl.col("sub_category").is_in(["REIT", "ETF"]))

            etf_reit_agg_df = agg_final_transactions(etf_reit_df)
            logging.info("Dividends from REITs:\n{}".format(etf_reit_agg_df))

    country_agg_df = agg_final_transactions(pivoted_df) if has_rows(pivoted_df) else None
    logging.info("Dividends by Country:\n{}".format(country_agg_df))

    return country_agg_df, etf_reit_agg_df


def process_bonds_ibkr(
    xml_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load corporate actions from XML and select bond-tax-relevant columns.
    2. Filter events by report date for the requested reporting period.
    3. Join FX by report date and convert realized PnL to EUR.
    4. Compute KESt on EUR realized PnL for each event.
    5. Produce detailed per-event output and aggregate country-level summary totals.
    6. Return detail dataframe and country aggregate dataframe.
    """
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
        "reit_dividends": {
            "label": "ETF/REIT div",
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

        section_summary_df = section.df.group_by(pl.lit(config["label"]).alias(Col.type), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col(config["profit_euro_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col(config["profit_euro_net_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            withholding_expr,
            pl.col(Col.kest_gross_total).sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net_total).sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(section_summary_df)

    return pl.concat(merge_dfs, how="vertical_relaxed")
