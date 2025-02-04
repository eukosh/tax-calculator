from datetime import date

import polars as pl
from polars.testing.asserts import assert_frame_equal

from src.const import Column, CurrencyCode
from src.providers.revolut import process_revolut_savings_statement

REPORTING_PERIOD_START_DATE = date(2024, 12, 1)
REPORTING_PERIOD_START_END = date(2024, 12, 31)


def test_process_revolut_savings_statement_usd(rates_df):
    res_df = process_revolut_savings_statement(
        "tests/test_data/revolut/revolut_savings_usd.csv",
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_START_END,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: [CurrencyCode.usd],
            Column.profit_total: [0.66],
            Column.profit_euro_total: [0.6314],
            Column.profit_euro_net_total: [0.4578],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.1736],
            Column.kest_net_total: [0.1736],
        }
    ).cast({Column.currency: pl.String})

    assert_frame_equal(res_df, expected_df)


def test_process_revolut_savings_statement_euro(rates_df):
    res_df = process_revolut_savings_statement(
        "tests/test_data/revolut/revolut_savings_euro.csv",
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_START_END,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: [CurrencyCode.euro],
            Column.profit_total: [0.54],
            Column.profit_euro_total: [0.54],
            Column.profit_euro_net_total: [0.3915],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.1485],
            Column.kest_net_total: [0.1485],
        }
    ).cast({Column.currency: pl.String})

    assert_frame_equal(res_df, expected_df)
