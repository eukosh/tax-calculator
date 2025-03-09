from datetime import date

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.providers.ibkr import (
    apply_pivot,
    calculate_summary_ibkr,
    handle_dividend_adjustments,
    process_bonds_ibkr,
    process_cash_transactions_ibkr,
)

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
            "sub_category": ["common", "common", "common"],
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
            "sub_category": ["common", "common", "common", "common"],
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "currency": ["USD", "USD", "USD", "USD"],
            "type": ["Dividends", "Dividends", "Withholding Tax", "Withholding Tax"],
            "amount": [100.0, 10.0, 20.0, 2.0],
            "amount_euro": [90.0, 9.0, 18.0, 1.8],
        }
    )


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
            Column.currency: ["USD"],
            Column.profit_total: [381.08],
            Column.profit_euro_total: [356.6176],
            Column.profit_euro_net_total: [258.5478],
            Column.kest_gross_total: [98.0698],
            Column.kest_net_total: [98.0698],
        }
    )


@pytest.fixture
def dividends_etf_reit_summary_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["IE", "US"],
            Column.currency: ["USD", "USD"],
            Column.profit_total: [41.53, 9.88],
            Column.dividends_euro_total: [39.7987, 9.0617],
            Column.dividends_euro_net_total: [28.8541, 6.5692],
            Column.withholding_tax_euro_total: [0.0, 1.2015],
            Column.kest_gross_total: [10.9446, 2.492],
            Column.kest_net_total: [10.9446, 1.291],
        }
    )


@pytest.fixture
def dividends_country_summary_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["IE", "US", "NL", "GB"],
            Column.currency: ["USD", "USD", "EUR", "USD"],
            Column.profit_total: [41.53, 25.61, 7.6, 2.38],
            Column.dividends_euro_total: [39.7988, 23.3634, 7.6, 2.2493],
            Column.dividends_euro_net_total: [28.8541, 16.9335, 5.51, 1.6307],
            Column.withholding_tax_euro_total: [0.0, 3.3471, 1.14, 0],
            Column.kest_gross_total: [10.9446, 6.4249, 2.09, 0.6186],
            Column.kest_net_total: [10.9446, 3.0828, 0.95, 0.6186],
        }
    )


@pytest.fixture
def dividends_country_summary_no_etf_reit_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["US", "NL", "GB"],
            Column.currency: ["USD", "EUR", "USD"],
            Column.profit_total: [15.73, 7.6, 2.38],
            Column.dividends_euro_total: [14.3017, 7.6, 2.2493],
            Column.dividends_euro_net_total: [10.3642, 5.51, 1.6307],
            Column.withholding_tax_euro_total: [2.1456, 1.14, 0],
            Column.kest_gross_total: [3.933, 2.09, 0.6186],
            Column.kest_net_total: [1.7919, 0.95, 0.6186],
        }
    )


def test_handle_dividend_adjustments():
    # Sample input data simulating dividend and withholding tax adjustments
    data = {
        "action_id": [1, 1, 1, 1],
        "settle_date": ["2024-10-15", "2024-10-15", "2024-10-15", "2024-10-15"],
        "issuer_country_code": ["US", "US", "US", "US"],
        "sub_category": ["REIT", "REIT", "REIT", "REIT"],
        "symbol": ["CTRE", "CTRE", "CTRE", "CTRE"],
        "currency": ["USD", "USD", "USD", "USD"],
        "type": ["Dividends", "Withholding Tax", "Withholding Tax", "Withholding Tax"],
        "amount": [4.35, -0.65, 0.65, -0.48],
    }

    df = pl.DataFrame(data)

    expected_data = {
        "action_id": [1, 1],
        "settle_date": ["2024-10-15", "2024-10-15"],
        "issuer_country_code": ["US", "US"],
        "sub_category": ["REIT", "REIT"],
        "symbol": ["CTRE", "CTRE"],
        "currency": ["USD", "USD"],
        "type": ["Withholding Tax", "Dividends"],
        "amount": [-0.48, 4.35],
    }

    expected_df = pl.DataFrame(expected_data)

    result_df = handle_dividend_adjustments(df).sort("amount")

    assert_frame_equal(result_df, expected_df)


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
            "sub_category": ["common", "common"],
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
            "sub_category": ["common"],
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


@pytest.mark.parametrize(
    "extract_etf_and_reit",
    [True, False],
)
def test_process_cash_transactions_ibkr(
    rates_df,
    dividends_country_summary_df,
    extract_etf_and_reit,
    dividends_country_summary_no_etf_reit_df,
    dividends_etf_reit_summary_df,
):
    res_df, etf_reit_df = process_cash_transactions_ibkr(
        "tests/test_data/ibkr/For_tax_automation*",
        rates_df,
        start_date=REPORTING_START_DATE,
        end_date=REPORTING_END_DATE,
        extract_etf_and_reit=extract_etf_and_reit,
    )

    if extract_etf_and_reit:
        assert_frame_equal(etf_reit_df, dividends_etf_reit_summary_df)
    else:
        assert etf_reit_df is None
    expected = dividends_country_summary_no_etf_reit_df if extract_etf_and_reit else dividends_country_summary_df

    assert_frame_equal(res_df, expected)


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
    ).sort("profit_total")

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends", "dividends", "bonds"],
            Column.currency: ["USD", "EUR", "USD"],
            Column.profit_total: [69.52, 7.6, 381.08],
            Column.profit_euro_total: [65.4115, 7.6, 356.6176],
            Column.profit_euro_net_total: [47.4183, 5.51, 258.5478],
            Column.withholding_tax_euro_total: [3.3471, 1.14, 0],
            Column.kest_gross_total: [17.9881, 2.09, 98.0698],
            Column.kest_net_total: [14.646, 0.95, 98.0698],
        }
    ).sort("profit_total")

    assert_frame_equal(ibkr_summary_df, expected_df)


def test_calculate_summary_separate_reits_ibkr(
    dividends_country_summary_no_etf_reit_df, bonds_country_summary_df, dividends_etf_reit_summary_df
):
    ibkr_summary_df = calculate_summary_ibkr(
        dividends_df=dividends_country_summary_no_etf_reit_df,
        bonds_df=bonds_country_summary_df,
        reits_df=dividends_etf_reit_summary_df,
    ).sort(Column.currency)

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends", "dividends", "bonds", "ETF/REIT div"],
            Column.currency: ["EUR", "USD", "USD", "USD"],
            Column.profit_total: [7.6, 18.11, 381.08, 51.41],
            Column.profit_euro_total: [7.6, 16.551, 356.6176, 48.8604],
            Column.profit_euro_net_total: [5.51, 11.9949, 258.5478, 35.4233],
            Column.withholding_tax_euro_total: [1.14, 2.1456, 0, 1.2015],
            Column.kest_gross_total: [2.09, 4.5516, 98.0698, 13.4366],
            Column.kest_net_total: [0.95, 2.4105, 98.0698, 12.2355],
        }
    ).sort(Column.currency)

    assert_frame_equal(ibkr_summary_df, expected_df)
