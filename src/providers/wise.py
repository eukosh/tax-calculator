import logging
from datetime import date

import polars as pl

from src.const import FLOAT_PRECISION, Column
from src.utils import calculate_kest, convert_to_euro, join_exchange_rates, read_csv_to_df


def process_wise_statement(
    csv_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    print("\n\n======================== Processing Wise Statement ========================\n")

    # Convert the extracted data into Polars DataFrames
    statement_df = read_csv_to_df(csv_file_path)
    statement_df = (
        statement_df.filter(pl.col("TransferWise ID").str.starts_with("BALANCE_CASHBACK"))
        .select(
            pl.col("Date").str.to_date("%d-%m-%Y").alias(Column.date),
            pl.col("Currency").alias(Column.currency),
            pl.col("Amount").alias(Column.amount),
        )
        .filter(pl.col(Column.date).is_between(start_date, end_date))
    )
    logging.debug(statement_df)

    joined_df = join_exchange_rates(
        df=statement_df,
        rates_df=exchange_rates_df,
        df_date_col=Column.date,
    )

    converted_euro_df = convert_to_euro(joined_df, Column.amount)
    logging.debug(converted_euro_df)

    # wise tax witheld rate is Belgium's 30% thus divide by 70
    gross_amount_recovered_df = converted_euro_df.with_columns(
        [
            (pl.col(Column.amount) * 100 / 70).alias(Column.profit_gross),
            (pl.col(Column.amount_euro) * 100 / 70).alias(Column.profit_gross_euro),
        ]
    )

    gross_amount_recovered_df = gross_amount_recovered_df.with_columns(
        [
            (pl.col(Column.profit_gross) - pl.col(Column.amount)).alias(Column.withholding_tax),
            (pl.col(Column.profit_gross_euro) - pl.col(Column.amount_euro)).alias(Column.withholding_tax_euro),
        ]
    )
    logging.debug(gross_amount_recovered_df)

    tax_df = calculate_kest(
        gross_amount_recovered_df,
        amount_col=Column.profit_gross_euro,
        tax_withheld_col=Column.withholding_tax_euro,
        net_col_name=Column.profit_euro_net,
    )
    logging.debug(tax_df)

    # it is possible to add a group by currency step before calcualting combined summary, to have a summary for each currency just for curiosity

    summary_df = (
        tax_df.group_by(Column.currency)
        .agg(
            # pl.sum(Column.amount_euro).round(FLOAT_PRECISION).alias(Column.amount_euro_received_total),
            pl.sum(Column.profit_gross).round(FLOAT_PRECISION).alias(Column.profit_total),
            pl.sum(Column.profit_gross_euro).round(FLOAT_PRECISION).alias(Column.profit_euro_total),
            pl.sum(Column.withholding_tax_euro).round(FLOAT_PRECISION).alias(Column.withholding_tax_euro_total),
            pl.sum(Column.profit_euro_net).round(FLOAT_PRECISION).alias(Column.profit_euro_net_total),
            pl.sum(Column.kest_gross).round(FLOAT_PRECISION).alias(Column.kest_gross_total),
            pl.sum(Column.kest_net).round(FLOAT_PRECISION).alias(Column.kest_net_total),
        )
        .sort(Column.profit_euro_total, descending=True)
    )

    return summary_df
