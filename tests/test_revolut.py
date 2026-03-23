from datetime import date

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column, CurrencyCode
from src.currencies import ExchangeRates
from src.providers.revolut import process_revolut_savings_statement

REPORTING_PERIOD_START_DATE = date(2024, 12, 1)
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
            Column.profit_euro_net_total: [0.3916],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.1484],
            Column.kest_net_total: [0.1484],
        }
    ).cast({Column.currency: pl.String})

    assert_frame_equal(res_df, expected_df)


def test_process_revolut_savings_statement_euro_2025_real_file(rates_df_2025):
    res_df = process_revolut_savings_statement(
        "data/input/eugene/2025/revolut_2025-01-01_2025-12-31_en_eur.csv",
        rates_df_2025,
        start_date=REPORTING_PERIOD_START_DATE_2025,
        end_date=REPORTING_PERIOD_START_END_2025,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: [CurrencyCode.euro],
            Column.profit_total: [91.3],
            Column.profit_euro_total: [91.3],
            Column.profit_euro_net_total: [66.1925],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [25.1075],
            Column.kest_net_total: [25.1075],
        }
    ).cast({Column.currency: pl.String})

    assert_frame_equal(res_df, expected_df)


def test_process_revolut_savings_statement_usd_2025_real_file(rates_df_2025):
    res_df = process_revolut_savings_statement(
        "data/input/eugene/2025/revolut_2025-01-01_2025-12-31_en_usd.csv",
        rates_df_2025,
        start_date=REPORTING_PERIOD_START_DATE_2025,
        end_date=REPORTING_PERIOD_START_END_2025,
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: [CurrencyCode.usd],
            Column.profit_total: [74.52],
            Column.profit_euro_total: [66.1375],
            Column.profit_euro_net_total: [47.9497],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [18.1878],
            Column.kest_net_total: [18.1878],
        }
    ).cast({Column.currency: pl.String})

    assert_frame_equal(res_df, expected_df)


def test_process_revolut_savings_statement_unknown_description_fails_fast(tmp_path, rates_df_2025):
    statement = tmp_path / "revolut_unknown_description.csv"
    statement.write_text(
        "\n".join(
            [
                'Date,Description,"Value, EUR",Price per share,Quantity of shares',
                '"Jan 1, 2025, 1:00:00 AM",Service Fee Charged EUR Class IE000AZVL3K0,-0.10,,',
                '"Jan 1, 2025, 1:00:00 AM",Interest BONUS EUR Class R IE000AZVL3K0,0.20,,',
            ]
        )
    )

    with pytest.raises(ValueError, match="Unsupported Revolut savings statement description"):
        process_revolut_savings_statement(
            str(statement),
            rates_df_2025,
            start_date=REPORTING_PERIOD_START_DATE_2025,
            end_date=REPORTING_PERIOD_START_END_2025,
        )
