from datetime import date

import polars as pl
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.providers.freedom import process_freedom_statement

REPORTING_PERIOD_START_DATE = date(2024, 1, 1)
REPORTING_PERIOD_START_END = date(2024, 12, 31)


def test_process_freedom_statement(rates_df):
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(100)
    res_df = process_freedom_statement(
        "tests/test_data/freedom/freedom_2024-04-30 23_59_59_2024-12-31 23_59_59_all.json",
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_START_END,
    )

    expected_df = pl.DataFrame(
        {
            Column.profit_total: [3400.0],
            Column.profit_euro_total: [3111.3477],
            Column.profit_euro_net_total: [2255.7271],
            Column.kest_gross_total: [855.6206],
            Column.kest_net_total: [444.8344],
        }
    )

    assert_frame_equal(res_df, expected_df)
