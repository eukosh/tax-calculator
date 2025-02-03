import logging
from datetime import date

import polars as pl

from src.const import FLOAT_PRECISION, Column, CorporateActionTypesFF
from src.utils import calculate_kest, convert_to_euro, join_exchange_rates, read_json

EMPTY_VALUE = "-"

TICKERS_WITHHOLDING_ZERO_TAX = ["TLT.US"]


def process_freedom_statement(
    json_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> pl.DataFrame:
    print("\n\n======================== Processing Freedom Finance Statement ========================\n")

    statement = read_json(json_file_path)
    corporate_actions_df = pl.DataFrame(statement["corporate_actions"]["detailed"])

    unmatched_currencies = corporate_actions_df.filter(
        (pl.col(Column.currency) != pl.col("external_tax_currency"))
        | ((pl.col(Column.currency) != pl.col("tax_currency")) & (~pl.col("tax_currency").is_in([EMPTY_VALUE, ""])))
    )

    if unmatched_currencies.shape[0] > 0:
        logging.error("Some currencies do not match: {}".format(unmatched_currencies))
        raise ValueError("Some currencies do not match")

    corporate_actions_df = corporate_actions_df.select(
        pl.col(Column.date).str.to_date("%Y-%m-%d"),
        pl.col("type_id").alias(Column.type),
        Column.corporate_action_id,
        Column.ticker,
        Column.currency,
        pl.col(Column.amount).cast(pl.Float64).alias(Column.amount),
        pl.col("tax_amount").str.replace(f"^{EMPTY_VALUE}$", "0").cast(pl.Float64).alias(Column.withholding_tax),
        pl.col("q_on_ex_date").cast(pl.Float64).alias(Column.shares_count),
        pl.col("amount_per_one").cast(pl.Float64).alias(Column.amount_per_share),
    ).filter(pl.col(Column.date).is_between(start_date, end_date))

    dividends_df = corporate_actions_df.filter(pl.col(Column.type) == CorporateActionTypesFF.dividend)
    dividends_reverted_df = corporate_actions_df.filter(pl.col(Column.type) == CorporateActionTypesFF.dividend_reverted)

    # FF performs revert operations for securities like TLT that should have 0 withholding tax
    # First they pay dividend taxed at standard 15 %, then they revert this corporate action
    # and issue the last one per corporate_action_id with full amount and 0 withholding tax
    # here I am making sure that these revertions canceled each other out
    dividends_reverted_agg_df = dividends_reverted_df.group_by(
        Column.ticker,
        Column.corporate_action_id,
    ).agg(pl.sum(Column.amount).alias(Column.amount), pl.sum(Column.withholding_tax).alias(Column.withholding_tax))

    uncanceled_reverted_dividends_df = dividends_reverted_agg_df.filter(
        (pl.col(Column.amount) > 0) | (pl.col(Column.withholding_tax) > 0)
    )
    if uncanceled_reverted_dividends_df.shape[0] > 0:
        logging.error(
            "Some reverted dividends did not cancel out each other: {}".format(uncanceled_reverted_dividends_df)
        )
        dividends_df = dividends_df.filter(
            ~dividends_df[Column.corporate_action_id].is_in(dividends_reverted_agg_df[Column.corporate_action_id])
        )

    # FF for some payments does not handle reversions in the expected tax year, thus I am documenting entries
    # where expected withholding tax shoud be zero, but is not. For the previous tax year i will pay full Austrian
    # tax on these entries assuming these records will be zeroed out in the future. I will save them to file
    # to be able to reference in the next tax year and dont pay double tax on them.
    incorrect_withholding_tax_df = dividends_df.filter(
        pl.col(Column.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX) & (pl.col(Column.withholding_tax) != 0)
    )
    if incorrect_withholding_tax_df.shape[0] > 0:
        logging.warning("Incorrect withholding tax for some tickers: {}".format(incorrect_withholding_tax_df))
        # incorrect_withholding_tax_df.write_csv(
        #     "data/input/eugene/freedom/dividends_with_incorrect_non_0_withholding_tax.csv"
        # )

    dividends_tax_abs_df = dividends_df.with_columns(pl.col(Column.withholding_tax).abs().alias(Column.withholding_tax))
    # 1. calculate gross amount by summing withholding tax to amount
    # 2. reimburse tax withheld for tax calculation purposes to have full gross amount
    gross_amount_recovered_df = dividends_tax_abs_df.with_columns(
        (
            (pl.col(Column.amount) + pl.col(Column.withholding_tax)).alias(Column.amount),
            pl.when(pl.col(Column.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX) & (pl.col(Column.withholding_tax) != 0))
            .then(0)
            .otherwise(pl.col(Column.withholding_tax))
            .alias(Column.withholding_tax),
        )
    )
    logging.debug("gross_amount_recovered_df: {}".format(gross_amount_recovered_df))

    joined_df = join_exchange_rates(
        df=gross_amount_recovered_df,
        rates_df=exchange_rates_df,
        df_date_col=Column.date,
    )

    joined_df = convert_to_euro(joined_df, col_to_convert=[Column.amount, Column.withholding_tax])

    tax_df = calculate_kest(joined_df, amount_col=Column.amount_euro, tax_withheld_col=Column.withholding_tax_euro)
    logging.info(
        "FF dividends per ticker: {}".format(
            tax_df.group_by(Column.ticker, Column.currency).agg(
                pl.sum(Column.amount).alias(Column.profit_total),
                pl.sum(Column.amount_euro).alias(Column.profit_euro_total),
                pl.sum(Column.amount_euro_net).alias(Column.profit_euro_net_total),
                pl.sum(Column.kest_gross).alias(Column.kest_gross_total),
                pl.sum(Column.kest_net).alias(Column.kest_net_total),
            )
        )
    )
    summary_df = tax_df.group_by(Column.currency).agg(
        pl.sum(Column.amount).round(FLOAT_PRECISION).alias(Column.profit_total),
        pl.sum(Column.amount_euro).round(FLOAT_PRECISION).alias(Column.profit_euro_total),
        pl.sum(Column.amount_euro_net).alias(Column.profit_euro_net_total),
        pl.sum(Column.withholding_tax_euro).alias(Column.withholding_tax_euro_total),
        pl.sum(Column.kest_gross).round(FLOAT_PRECISION).alias(Column.kest_gross_total),
        pl.sum(Column.kest_net).round(FLOAT_PRECISION).alias(Column.kest_net_total),
    )

    logging.info("Freedom Finance Summary: {}".format(summary_df))

    return summary_df
