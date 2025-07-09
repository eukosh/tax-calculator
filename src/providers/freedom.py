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
    
    # I need some reliable mechanism in addition to output file dividend_entries_to_be_excluded_from_future_tax.csv to ensure I do not process
    # the same tax events twice. Though maybe this file and corporate action ids in it are enough
    # to filter out already processed records.

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
        pl.col("ex_date").str.to_date("%Y-%m-%d").alias("ex_date"),
        pl.col("type_id").alias(Column.type),
        Column.corporate_action_id,
        Column.ticker,
        Column.currency,
        pl.col(Column.amount).cast(pl.Float64).alias(Column.amount),
        pl.col("tax_amount").str.replace(f"^{EMPTY_VALUE}$", "0").cast(pl.Float64).alias(Column.withholding_tax),
        pl.col("q_on_ex_date").cast(pl.Float64).alias(Column.shares_count),
        pl.col("amount_per_one").cast(pl.Float64).alias(Column.amount_per_share),
    ).filter(pl.col("ex_date").is_between(start_date, end_date))
    # ATTENTION!!!: thats a huge workaround, i dont think using ex date is right the right thing,
    # but i'm using it to distinguish between tax periods because of FFs mess with reverted dividends.
    # I saw that approx. since march 2025 TLT was taxed correctly at 0 without reversal, so next year
    # check it, maybe they finally fixed it properly and i dont need all these tricks.

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
            "Some reverted dividends did not cancel out each other in the within tax year: {}".format(
                uncanceled_reverted_dividends_df
            )
        )
        logging.info("Will verify if they should be taxed at 0 withholding tax and backfill them...")
        # Using dividends_reverted_df here because I need non aggregated cols too
        reverted_divs_to_backfill_df = dividends_reverted_df.filter(
            ~pl.col(Column.corporate_action_id).is_in(dividends_df[Column.corporate_action_id])
            & pl.col(Column.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX)
            & (pl.col(Column.withholding_tax) != 0)
        )
        # Read the log msg to understand the logic. Basically i am trying to handle 2 cases:
        # 1. When i run the report in 1st months of new year and in that case I will just have
        # entries for last several months of prev tax year with status dividend and this if
        # statement should not be entered at all
        # 2. When i run the report a bit later in new year when FF has already changed status
        # of old entriest to reverted dividend and added new entries with type dividend, but
        # already with date in new year. In this case I will have to backfill the reverted dividends
        # to dividends df and manually fix withholding tax to 0
        logging.info(
            """FF retroactively changes type of coprorate action in the future when it fixes the withholding tax,
            So when the FF statement is generated in the first months of new year some late last year payments will have
            status of dividend instead of the reverted dividend and will have the withholding tax non 0, BUT when
            FF fixes these entries in the coming months of next year, it will retroactively change thet type of initial last
            year's corporate action to dividend reverted and then cancel it out and add final record with type dividend
            BUT already in the new year, thus this dividend for the last year will only have an entry of type reverted 
            dividend and I need to backfill it for further calculations. \nReverted dividends to backfill: {}""".format(
                reverted_divs_to_backfill_df
            )
        )

        if uncanceled_reverted_dividends_df.shape[0] != reverted_divs_to_backfill_df.shape[0]:
            raise ValueError("Dividends for backfill should match the number of uncanceled reverted dividends")

        dividends_df = pl.concat([dividends_df, reverted_divs_to_backfill_df])

    # Verify that i didnt generate duplicate entries
    if dividends_df[Column.corporate_action_id].n_unique() != dividends_df.shape[0]:
        raise ValueError(
            "Number of entiries in dividends df looks messed up, amount of unique corporate action ids is not equal to the number of entries"
        )

    # FF for some payments does not handle reversions in the expected tax year, thus I am documenting entries
    # where expected withholding tax shoud be zero, but is not. For the previous tax year i will pay full Austrian
    # tax on these entries assuming these records will be zeroed out in the future. I will save them to file
    # to be able to reference in the next tax year and DO NOT pay tax for these corporate action ids again.
    incorrect_withholding_tax_df = dividends_df.filter(
        pl.col(Column.ticker).is_in(TICKERS_WITHHOLDING_ZERO_TAX) & (pl.col(Column.withholding_tax) != 0)
    )
    # EXCLUDE THESE CORPORATE ACTION IDS IN THE NEXT YEAR FROM TAX CALCULATION, I HAVE ALREADY PAID TAX FOR THEM
    if incorrect_withholding_tax_df.shape[0] > 0:
        logging.warning("Incorrect withholding tax for some tickers: {}".format(incorrect_withholding_tax_df))
        incorrect_withholding_tax_df.write_csv(
            "data/input/eugene/freedom/dividend_entries_to_be_excluded_from_future_tax.csv"
        )

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
