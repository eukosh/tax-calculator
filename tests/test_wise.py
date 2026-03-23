from datetime import date

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.currencies import ExchangeRates
from src.providers.wise import process_wise_statement

REPORTING_PERIOD_START_DATE = date(2024, 1, 1)
REPORTING_PERIOD_START_END = date(2024, 12, 31)
REPORTING_PERIOD_START_DATE_2025 = date(2025, 1, 1)
REPORTING_PERIOD_START_END_2025 = date(2025, 12, 31)


@pytest.fixture(scope="module")
def rates_df_2025():
    exchange_rates = ExchangeRates(
        start_date="2025-01-01",
        end_date="2025-12-31",
        raw_file_path="data/input/currencies/raw_exchange_rates.csv",
    )
    return exchange_rates.get_rates()


def test_process_wise_usd(rates_df):
    res_df = process_wise_statement(
        "tests/test_data/wise/wise_usd.csv",
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_START_END,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: ["USD"],
            Column.profit_total: [15.3143],
            # Column.amount_euro_received_total: [9.9989],
            Column.profit_euro_total: [14.2841],
            Column.withholding_tax_euro_total: [4.2852],
            Column.profit_euro_net_total: [8.2134],
            Column.kest_gross_total: [3.9281],
            Column.kest_net_total: [1.7855],
        }
    )

    assert_frame_equal(res_df, expected_df)


def test_process_wise_euro(rates_df):
    res_df = process_wise_statement(
        "tests/test_data/wise/wise_euro.csv",
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_START_END,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: ["EUR"],
            Column.profit_total: [19.3143],
            # Column.amount_euro_received_total: [13.52],
            Column.profit_euro_total: [19.3143],
            Column.withholding_tax_euro_total: [5.7943],
            Column.profit_euro_net_total: [11.1057],
            Column.kest_gross_total: [5.3114],
            Column.kest_net_total: [2.4143],
        }
    )

    assert_frame_equal(res_df, expected_df)


def test_process_wise_euro_2025_real_file(rates_df_2025):
    res_df = process_wise_statement(
        "data/input/oryna/2025/wise_EUR_2025-01-01_2025-12-31.csv",
        rates_df_2025,
        start_date=REPORTING_PERIOD_START_DATE_2025,
        end_date=REPORTING_PERIOD_START_END_2025,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: ["EUR"],
            Column.profit_total: [42.7],
            Column.profit_euro_total: [42.7],
            Column.withholding_tax_euro_total: [12.81],
            Column.profit_euro_net_total: [24.5525],
            Column.kest_gross_total: [11.7425],
            Column.kest_net_total: [5.3375],
        }
    )

    assert_frame_equal(res_df, expected_df)


def test_process_wise_usd_2025_real_file(rates_df_2025):
    res_df = process_wise_statement(
        "data/input/oryna/2025/wise_USD_2025-01-01_2025-12-31.csv",
        rates_df_2025,
        start_date=REPORTING_PERIOD_START_DATE_2025,
        end_date=REPORTING_PERIOD_START_END_2025,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: ["USD"],
            Column.profit_total: [48.0],
            Column.profit_euro_total: [42.7186],
            Column.withholding_tax_euro_total: [12.8156],
            Column.profit_euro_net_total: [24.5632],
            Column.kest_gross_total: [11.7476],
            Column.kest_net_total: [5.3398],
        }
    )

    assert_frame_equal(res_df, expected_df)
