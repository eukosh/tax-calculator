from datetime import date

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.currencies import ExchangeRates
from src.providers.ibkr import apply_pivot, calculate_summary_ibkr, process_bonds_ibkr, process_cash_transactions_ibkr

REPORTING_START_DATE = date(2024, 1, 1)
REPORTING_END_DATE = date(2024, 12, 31)


@pytest.fixture
def sample_df_no_duplicates():
    """
    DataFrame with no duplicates.
    Coverst cases:
    - both 'Dividends' and 'Withholding Tax' are present per symbol per date
    - 'Withholding Tax' is missing, i.e. tax is not withheld at issuer's country
    """
    return pl.DataFrame(
        {
            "settle_date": ["2025-01-01", "2025-01-01", "2025-01-02"],
            "issuer_country_code": ["USA", "USA", "UK"],
            "symbol": ["AAPL", "AAPL", "UL"],
            "currency": ["USD", "USD", "USD"],
            "type": ["Dividends", "Withholding Tax", "Dividends"],
            "amount": [100.0, 20.0, 100.0],
            "amount_euro": [90.0, 18.0, 90.0],
        }
    )


@pytest.fixture
def sample_df_with_duplicates():
    """
    DataFrame with duplicates.
    """
    return pl.DataFrame(
        {
            "settle_date": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01"],
            "issuer_country_code": ["USA", "USA", "USA", "USA"],
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "currency": ["USD", "USD", "USD", "USD"],
            "type": ["Dividends", "Dividends", "Withholding Tax", "Withholding Tax"],
            "amount": [100.0, 10.0, 20.0, 2.0],
            "amount_euro": [90.0, 9.0, 18.0, 1.8],
        }
    )


@pytest.fixture
def rates_df():
    exchange_rates = ExchangeRates(
        start_date="2024-01-01",
        end_date="2024-12-31",
        raw_file_path="tests/test_data/currencies/exchange_rates.csv",
    )
    return exchange_rates.get_rates()


@pytest.fixture
def bonds_tax_df():
    return pl.DataFrame(
        {
            "report_date": [
                date(2024, 12, 2),
                date(2024, 8, 7),
                date(2024, 12, 10),
                date(2024, 2, 15),
                date(2024, 2, 29),
            ],
            "isin": [
                "US912828YV68",
                "US912797GK78",
                "US912797MN44",
                "US912797GN18",
                "US912797GP65",
            ],
            "issuer_country_code": ["US", "US", "US", "US", "US"],
            "currency": ["USD", "USD", "USD", "USD", "USD"],
            "proceeds": [4000.0, 4000.0, 5000.0, 3000.0, 3000.0],
            "realized_pnl": [100.92, 90.33, 76.64, 70.85, 42.34],
            "realized_pnl_euro": [
                96.0503,
                82.7046,
                72.8033,
                65.9499,
                39.1096,
            ],
            "realized_pnl_euro_net": [69.6365, 59.9608, 52.7824, 47.8137, 28.3545],
            "kest_gross": [26.4138, 22.7438, 20.0209, 18.1362, 10.7551],
            "kest_net": [26.4138, 22.7438, 20.0209, 18.1362, 10.7551],
        }
    )


@pytest.fixture
def bonds_country_summary_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["US"],
            Column.profit_euro_total: [356.6176],
            Column.profit_euro_net_total: [258.5478],
            Column.kest_gross_total: [98.0698],
            Column.kest_net_total: [98.0698],
        }
    )


@pytest.fixture
def dividends_country_summary_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["US", "GB"],
            Column.dividends_euro_total: [14.3017, 2.2493],
            Column.dividends_euro_net_total: [10.3642, 1.6307],
            Column.withholding_tax_euro_total: [2.1456, 0],
            Column.kest_gross_total: [3.933, 0.6186],
            Column.kest_net_total: [1.7919, 0.6186],
        }
    )


def test_apply_pivot_no_duplicates(sample_df_no_duplicates, caplog):
    """
    Test apply_pivot() with a DataFrame that has no duplicates.
    Verifies that no warning is logged and the pivoted output is correct.
    """
    res_df = apply_pivot(sample_df_no_duplicates)
    expected_df = pl.DataFrame(
        {
            "settle_date": ["2025-01-01", "2025-01-02"],
            "issuer_country_code": ["USA", "UK"],
            "symbol": ["AAPL", "UL"],
            "currency": ["USD", "USD"],
            Column.dividends: [100.0, 100.0],
            Column.withholding_tax: [20.0, 0],
            Column.dividends_euro: [90.0, 90.0],
            Column.withholding_tax_euro: [18.0, 0],
        }
    )

    assert "Duplicate rows detected" not in caplog.text
    assert_frame_equal(res_df, expected_df)


def test_apply_pivot_with_duplicates(sample_df_with_duplicates, caplog):
    """
    Test apply_pivot() with a DataFrame that has duplicates.
    Verifies that warning is logged and the pivoted output is correct.
    """
    res_df = apply_pivot(sample_df_with_duplicates)
    expected_df = pl.DataFrame(
        {
            "settle_date": ["2025-01-01"],
            "issuer_country_code": ["USA"],
            "symbol": ["AAPL"],
            "currency": ["USD"],
            Column.dividends: [110.0],
            Column.withholding_tax: [22.0],
            Column.dividends_euro: [99.0],
            Column.withholding_tax_euro: [19.8],
        }
    )

    assert "Duplicate rows detected" in caplog.text
    assert_frame_equal(res_df, expected_df)


def test_process_cash_transactions_ibkr(rates_df, dividends_country_summary_df):
    res_df = process_cash_transactions_ibkr(
        "tests/test_data/ibkr/For_tax_automation*",
        rates_df,
        start_date=REPORTING_START_DATE,
        end_date=REPORTING_END_DATE,
    )

    assert_frame_equal(res_df, dividends_country_summary_df)


def test_process_bonds_ibkr(rates_df, bonds_tax_df, bonds_country_summary_df):
    tax_res_df, summary_res_df = process_bonds_ibkr(
        "tests/test_data/ibkr/For_tax_automation*",
        rates_df,
        start_date=REPORTING_START_DATE,
        end_date=REPORTING_END_DATE,
    )

    assert_frame_equal(tax_res_df, bonds_tax_df)
    assert_frame_equal(summary_res_df, bonds_country_summary_df)


def test_calculate_summary_ibkr(dividends_country_summary_df, bonds_country_summary_df):
    ibkr_summary_df = calculate_summary_ibkr(
        dividends_df=dividends_country_summary_df, bonds_df=bonds_country_summary_df
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends", "bonds"],
            Column.profit_euro_total: [16.551, 356.6176],
            Column.profit_euro_net_total: [11.9949, 258.5478],
            Column.withholding_tax_euro_total: [2.1456, 0],
            Column.kest_gross_total: [4.5516, 98.0698],
            Column.kest_net_total: [2.4105, 98.0698],
        }
    )

    assert_frame_equal(ibkr_summary_df, expected_df)
