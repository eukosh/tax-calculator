import logging
from datetime import date

import polars as pl

from src.const import FLOAT_PRECISION, Column, CurrencyCode, RevolutColumn, RevolutType
from src.utils import calculate_kest, convert_to_euro, join_exchange_rates


def process_revolut_savings_statement(
    csv_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    logging.info(
        "In the current implementation it is not ready to properly process from euro and usd accounts combined."
    )
    print("\n\n======================== Processing Revolut Savings Statement ========================\n")

    # Convert the extracted data into Polars DataFrames
    statement_df = pl.read_csv(csv_file_path)

    processed_statement_df = statement_df.select(
        [
            pl.col("Date").str.to_datetime(format="%b %e, %Y, %I:%M:%S %p").dt.date().alias(Column.date),
            (
                pl.when(pl.col("Description").str.starts_with("BUY"))
                .then(pl.lit(RevolutType.buy))
                .when(pl.col("Description").str.starts_with("Interest PAID"))
                .then(pl.lit(RevolutType.interest))
                .when(pl.col("Description").str.starts_with("Service Fee Charged"))
                .then(pl.lit(RevolutType.fee))
                .otherwise(None)
                .alias(RevolutColumn.type)
            ),
            (
                pl.when(pl.col("Value").str.contains("$", literal=True))
                .then(pl.lit(CurrencyCode.usd))
                .when(pl.col("Value").str.contains("€", literal=True))
                .then(pl.lit(CurrencyCode.euro))
                .otherwise(None)
                .alias(Column.currency)
            ),
            pl.col("Value")
            .str.replace("$", "", literal=True)
            .str.replace("€", "", literal=True)
            .str.replace(",", "", literal=True)
            .cast(pl.Float64)
            .alias(RevolutColumn.amount),
        ]
    ).filter(pl.col(Column.date).is_between(start_date, end_date))
    logging.debug("\nProcessed Statemend Df:\n", processed_statement_df)

    fees_interest_df = processed_statement_df.filter(
        pl.col(RevolutColumn.type).is_in([RevolutType.fee, RevolutType.interest])
    )

    # here it will be net profit since negative fee is accrued on the same day in the same currency
    profit_by_date_df = fees_interest_df.group_by(Column.date, Column.currency).agg(
        pl.sum(RevolutColumn.amount).alias(Column.profit)
    )
    logging.debug("\nprofit_by_date_df:\n", profit_by_date_df)

    joined_df = join_exchange_rates(
        df=profit_by_date_df,
        rates_df=exchange_rates_df,
        df_date_col=Column.date,
    )
    profit_euro_df = convert_to_euro(joined_df, Column.profit)
    logging.debug("\nprofit_euro_df:\n", profit_euro_df)

    tax_df = calculate_kest(profit_euro_df, amount_col=Column.profit_euro)
    logging.debug("\tax_df:\n", tax_df)

    # once i update this func to process both usd and euro accounts, i will need to add a group by currency here or deal only with euro amounts
    summary_df = tax_df.select(
        pl.first(Column.currency),
        pl.sum(Column.profit).round(FLOAT_PRECISION).alias(Column.profit_total),
        pl.sum(Column.profit_euro).round(FLOAT_PRECISION).alias(Column.profit_euro_total),
        pl.sum(Column.profit_euro_net).round(FLOAT_PRECISION).alias(Column.profit_euro_net_total),
        pl.sum(Column.kest_gross).round(FLOAT_PRECISION).alias(Column.kest_gross_total),
        pl.sum(Column.kest_net).round(FLOAT_PRECISION).alias(Column.kest_net_total),
    ).sort(Column.profit_euro_total, descending=True)

    logging.info(summary_df)

    return summary_df
