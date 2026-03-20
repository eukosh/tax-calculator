from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.providers.ibkr import (
    IbkrSummarySection,
    apply_pivot,
    build_finanzonline_dividend_buckets_ibkr,
    calculate_summary_ibkr,
    handle_dividend_adjustments,
    process_bonds_ibkr,
    process_cash_transactions_ibkr,
    process_trades_ibkr,
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
def dividends_etf_summary_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["IE"],
            Column.currency: ["USD"],
            Column.profit_total: [41.53],
            Column.dividends_euro_total: [39.7987],
            Column.dividends_euro_net_total: [28.8541],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [10.9446],
            Column.kest_net_total: [10.9446],
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
def dividends_country_summary_no_etf_df():
    return pl.DataFrame(
        {
            "issuer_country_code": ["US", "NL", "GB"],
            Column.currency: ["USD", "EUR", "USD"],
            Column.profit_total: [25.61, 7.6, 2.38],
            Column.dividends_euro_total: [23.3634, 7.6, 2.2493],
            Column.dividends_euro_net_total: [16.9335, 5.51, 1.6307],
            Column.withholding_tax_euro_total: [3.3471, 1.14, 0],
            Column.kest_gross_total: [6.4249, 2.09, 0.6186],
            Column.kest_net_total: [3.0828, 0.95, 0.6186],
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


@pytest.mark.parametrize("exclude_etf", [True, False])
def test_process_cash_transactions_ibkr(
    rates_df,
    dividends_country_summary_df,
    exclude_etf,
    dividends_country_summary_no_etf_df,
):
    res_df, etf_df = process_cash_transactions_ibkr(
        "tests/test_data/ibkr/For_tax_automation*",
        rates_df,
        start_date=REPORTING_START_DATE,
        end_date=REPORTING_END_DATE,
        excluded_cash_transaction_subcategories={"ETF"} if exclude_etf else None,
    )
    assert etf_df is None
    expected = dividends_country_summary_no_etf_df if exclude_etf else dividends_country_summary_df

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
        sections=[
            IbkrSummarySection("dividends", dividends_country_summary_df),
            IbkrSummarySection("bonds", bonds_country_summary_df),
        ]
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


def test_calculate_summary_separate_etfs_ibkr(
    dividends_country_summary_no_etf_df, bonds_country_summary_df, dividends_etf_summary_df
):
    ibkr_summary_df = calculate_summary_ibkr(
        sections=[
            IbkrSummarySection("dividends", dividends_country_summary_no_etf_df),
            IbkrSummarySection("bonds", bonds_country_summary_df),
            IbkrSummarySection("etf_dividends", dividends_etf_summary_df),
        ]
    ).sort(Column.currency)

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends", "dividends", "bonds", "ETF div"],
            Column.currency: ["EUR", "USD", "USD", "USD"],
            Column.profit_total: [7.6, 27.99, 381.08, 41.53],
            Column.profit_euro_total: [7.6, 25.6127, 356.6176, 39.7987],
            Column.profit_euro_net_total: [5.51, 18.5642, 258.5478, 28.8541],
            Column.withholding_tax_euro_total: [1.14, 3.3471, 0, 0.0],
            Column.kest_gross_total: [2.09, 7.0435, 98.0698, 10.9446],
            Column.kest_net_total: [0.95, 3.7014, 98.0698, 10.9446],
        }
    ).sort(Column.currency)

    assert_frame_equal(ibkr_summary_df, expected_df)


def test_process_trades_ibkr_uses_buy_and_sell_rates(tmp_path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="120",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2024-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="20",
                sale_trade_id="broker-sell-1",
            )
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 6, 3)],
            Column.currency: ["USD", "USD"],
            Column.exchange_rate: [1.0, 1.2],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        ibkr_trade_history_path=str(trade_history_path),
    )

    expected_detail_df = pl.DataFrame(
        {
            "sale_date": ["2024-06-03"],
            "sale_datetime": ["2024-06-03 10:00:00"],
            "sale_trade_id": ["sell-1"],
            "ticker": ["AAPL"],
            "isin": ["US0378331005"],
            "quantity_sold": [1.0],
            "sale_price_ccy": [120.0],
            "sale_fx": [1.2],
            "lot_id": ["AAPL:2024-01-02:buy-1"],
            "lot_buy_date": ["2024-01-02"],
            "lot_buy_datetime": ["2024-01-02 10:00:00"],
            "lot_source_trade_id": ["buy-1"],
            "quantity_from_lot": [1.0],
            "taxable_proceeds_eur": [100.0],
            "taxable_original_basis_eur": [100.0],
            "taxable_total_basis_eur": [100.0],
            "taxable_gain_loss_eur": [0.0],
            "allocated_buy_fee_eur": [0.0],
            "allocated_sale_fee_eur": [0.0],
            "basis_origin": ["post_move_buy"],
            "notes": [
                "Austrian-authoritative FIFO sale result uses raw post-move buy lots and matches broker closed-lot output after fee adjustment."
            ],
        }
    )
    assert_frame_equal(detail_df, expected_detail_df)
    assert summary_df is None
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None


def test_process_trades_ibkr_clips_taxable_profit_on_loss(tmp_path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="80",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2024-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="-20",
                sale_trade_id="broker-sell-1",
            )
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 6, 3)],
            Column.currency: ["USD", "USD"],
            Column.exchange_rate: [1.0, 1.0],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        ibkr_trade_history_path=str(trade_history_path),
    )

    expected_detail_df = pl.DataFrame(
        {
            "sale_date": ["2024-06-03"],
            "sale_datetime": ["2024-06-03 10:00:00"],
            "sale_trade_id": ["sell-1"],
            "ticker": ["AAPL"],
            "isin": ["US0378331005"],
            "quantity_sold": [1.0],
            "sale_price_ccy": [80.0],
            "sale_fx": [1.0],
            "lot_id": ["AAPL:2024-01-02:buy-1"],
            "lot_buy_date": ["2024-01-02"],
            "lot_buy_datetime": ["2024-01-02 10:00:00"],
            "lot_source_trade_id": ["buy-1"],
            "quantity_from_lot": [1.0],
            "taxable_proceeds_eur": [80.0],
            "taxable_original_basis_eur": [100.0],
            "taxable_total_basis_eur": [100.0],
            "taxable_gain_loss_eur": [-20.0],
            "allocated_buy_fee_eur": [0.0],
            "allocated_sale_fee_eur": [0.0],
            "basis_origin": ["post_move_buy"],
            "notes": [
                "Austrian-authoritative FIFO sale result uses raw post-move buy lots and matches broker closed-lot output after fee adjustment."
            ],
        }
    )
    expected_summary_df = pl.DataFrame(
        {
            Column.type: ["trades loss"],
            Column.currency: ["EUR"],
            Column.profit_total: [-20.0],
            Column.profit_euro_total: [-20.0],
            Column.profit_euro_net_total: [-20.0],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.0],
            Column.kest_net_total: [0.0],
        }
    )

    assert_frame_equal(detail_df, expected_detail_df)
    assert_frame_equal(summary_df, expected_summary_df)
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None


def test_process_trades_ibkr_raises_without_buy_side_rate(tmp_path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2023-01-02",
                date_time="2023-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="120",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2023-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="20",
                sale_trade_id="broker-sell-1",
            )
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 6, 3)],
            Column.currency: ["USD"],
            Column.exchange_rate: [1.2],
        }
    )

    with pytest.raises(ValueError, match="No FX rate available for USD on or before 2023-01-02"):
        process_trades_ibkr(
            xml_file_path=str(closed_lot_path),
            exchange_rates_df=rates_df,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            ibkr_trade_history_path=str(trade_history_path),
        )


def test_process_trades_ibkr_separates_realized_profit_and_loss_by_default(tmp_path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="buy-aapl",
            ),
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-02-02",
                date_time="2024-02-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="200",
                trade_id="buy-msft",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="210",
                trade_id="sell-aapl",
            ),
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 11:00:00",
                operation="SELL",
                quantity="-1",
                price="145",
                trade_id="sell-msft",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2024-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="110",
                sale_trade_id="broker-sell-aapl",
            ),
            _closed_lot_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 11:00:00",
                buy_datetime="2024-02-02 10:00:00",
                quantity="1",
                cost="200",
                pnl="-55",
                sale_trade_id="broker-sell-msft",
            ),
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 2, 2), date(2024, 6, 3)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.1],
        }
    )

    _, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        ibkr_trade_history_path=str(trade_history_path),
    )

    expected_summary_df = pl.DataFrame(
        {
            Column.type: ["trades loss", "trades profit"],
            Column.currency: ["EUR", "EUR"],
            Column.profit_total: [-68.1818, 90.9091],
            Column.profit_euro_total: [-68.1818, 90.9091],
            Column.profit_euro_net_total: [-68.1818, 90.9091],
            Column.withholding_tax_euro_total: [0.0, 0.0],
            Column.kest_gross_total: [0.0, 0.0],
            Column.kest_net_total: [0.0, 0.0],
        }
    ).sort(Column.type)

    assert_frame_equal(summary_df.sort(Column.type), expected_summary_df)
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None


def test_process_trades_ibkr_can_disable_separate_profit_loss_reporting(tmp_path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="buy-aapl",
            ),
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-02-02",
                date_time="2024-02-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="200",
                trade_id="buy-msft",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="210",
                trade_id="sell-aapl",
            ),
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 11:00:00",
                operation="SELL",
                quantity="-1",
                price="145",
                trade_id="sell-msft",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2024-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="110",
                sale_trade_id="broker-sell-aapl",
            ),
            _closed_lot_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 11:00:00",
                buy_datetime="2024-02-02 10:00:00",
                quantity="1",
                cost="200",
                pnl="-55",
                sale_trade_id="broker-sell-msft",
            ),
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 2, 2), date(2024, 6, 3)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.1],
        }
    )

    _, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        separate_trade_profit_loss=False,
        ibkr_trade_history_path=str(trade_history_path),
    )

    expected_summary_df = pl.DataFrame(
        {
            Column.currency: ["EUR"],
            Column.profit_total: [22.7273],
            Column.profit_euro_total: [22.7273],
            Column.profit_euro_net_total: [16.4773],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [6.25],
            Column.kest_net_total: [6.25],
        }
    )

    assert_frame_equal(summary_df, expected_summary_df)
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None


def test_process_trades_ibkr_requires_raw_trade_history_path(tmp_path: Path):
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2024-06-03",
                sale_datetime="2024-06-03 10:00:00",
                buy_datetime="2024-01-02 10:00:00",
                quantity="1",
                cost="100",
                pnl="20",
                sale_trade_id="broker-sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 6, 3)],
            Column.currency: ["USD", "USD"],
            Column.exchange_rate: [1.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="requires ibkr_trade_history_path"):
        process_trades_ibkr(
            xml_file_path=str(closed_lot_path),
            exchange_rates_df=rates_df,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )


def test_core_ibkr_excludes_etfs_from_trades_cash_and_finanzonline_buckets(tmp_path):
    broker_xml_content = """\
<FlexQueryResponse>
  <FlexStatements count="1">
    <FlexStatement>
      <Trades>
        <Lot symbol="SPY5" currency="USD" subCategory="ETF" assetCategory="STK" quantity="1" dateTime="2024-06-03 10:00:00" tradeDate="2024-06-03" cost="100" fifoPnlRealized="20" transactionID="etf-sell" buySell="SELL" openDateTime="2024-01-02 10:00:00" levelOfDetail="CLOSED_LOT" isin="IE00B6YX5C33" />
        <Lot symbol="AAPL" currency="USD" subCategory="COMMON" assetCategory="STK" quantity="1" dateTime="2024-06-03 11:00:00" tradeDate="2024-06-03" cost="200" fifoPnlRealized="30" transactionID="aapl-sell" buySell="SELL" openDateTime="2024-01-02 10:00:00" levelOfDetail="CLOSED_LOT" isin="US0378331005" />
      </Trades>
      <CashTransactions>
        <CashTransaction
          accountId="-"
          currency="USD"
          assetCategory="STK"
          subCategory="ETF"
          symbol="SPY5"
          issuerCountryCode="IE"
          dateTime="2024-10-01 20:20:00"
          settleDate="2024-10-01"
          amount="4.5"
          type="Dividends"
          actionID="1"
        />
      </CashTransactions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""
    xml_path = tmp_path / "ibkr_mixed.xml"
    xml_path.write_text(broker_xml_content)
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                sub_category="ETF",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="100",
                trade_id="etf-buy",
            ),
            _trade_confirm_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                sub_category="ETF",
                trade_date="2024-06-03",
                date_time="2024-06-03 10:00:00",
                operation="SELL",
                quantity="-1",
                price="120",
                trade_id="etf-sell",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-01-02",
                date_time="2024-01-02 10:00:00",
                operation="BUY",
                quantity="1",
                price="200",
                trade_id="aapl-buy",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2024-06-03",
                date_time="2024-06-03 11:00:00",
                operation="SELL",
                quantity="-1",
                price="230",
                trade_id="aapl-sell",
            ),
        ],
    )

    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 1, 2), date(2024, 6, 3), date(2024, 10, 1)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(xml_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        excluded_trade_subcategories={"ETF"},
        ibkr_trade_history_path=str(trade_history_path),
    )
    dividends_df, etf_dividends_df = process_cash_transactions_ibkr(
        xml_file_path=str(xml_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        excluded_cash_transaction_subcategories={"ETF"},
    )
    dividend_buckets_df = build_finanzonline_dividend_buckets_ibkr(
        xml_file_path=str(xml_path),
        exchange_rates_df=rates_df,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        excluded_cash_transaction_subcategories={"ETF"},
    )

    assert detail_df is not None
    assert detail_df["ticker"].to_list() == ["AAPL"]
    assert detail_df["basis_origin"].to_list() == ["post_move_buy"]
    assert summary_df is not None
    assert summary_df["profit_euro_total"].to_list() == [30.0]
    assert dividends_df is None
    assert etf_dividends_df is None
    assert dividend_buckets_df.is_empty()
    assert stock_lot_state_df is not None
    assert stock_lot_state_df["ticker"].to_list() == ["AAPL"]
    assert trades_reconciliation_df is not None


def _write_opening_lots_csv(path: Path, rows: list[dict[str, object]]) -> None:
    header = (
        "snapshot_date,asset_class,ticker,isin,lot_id,buy_date,original_quantity,remaining_quantity,"
        "currency,buy_price_ccy,buy_fx_to_eur,original_cost_eur,cumulative_oekb_stepup_eur,adjusted_basis_eur,"
        "status,broker,account_id,notes,last_adjustment_year,last_adjustment_reference,last_sale_date,"
        "sold_quantity_ytd,source_trade_id,source_statement_file,broker_buy_date,broker_buy_price_ccy,"
        "broker_buy_fx_to_eur,broker_original_cost_eur,austrian_basis_method,austrian_basis_price_ccy,"
        "austrian_basis_fx_to_eur\n"
    )
    body = "".join(
        ",".join(
            [
                str(row["snapshot_date"]),
                str(row["asset_class"]),
                str(row["ticker"]),
                str(row["isin"]),
                str(row["lot_id"]),
                str(row["buy_date"]),
                str(row["original_quantity"]),
                str(row["remaining_quantity"]),
                str(row["currency"]),
                str(row["buy_price_ccy"]),
                str(row["buy_fx_to_eur"]),
                str(row["original_cost_eur"]),
                str(row.get("cumulative_oekb_stepup_eur", 0.0)),
                str(row["adjusted_basis_eur"]),
                str(row.get("status", "open")),
                str(row.get("broker", "ibkr")),
                str(row.get("account_id", "U1")),
                str(row.get("notes", "")),
                str(row.get("last_adjustment_year", "")),
                str(row.get("last_adjustment_reference", "")),
                str(row.get("last_sale_date", "")),
                str(row.get("sold_quantity_ytd", 0.0)),
                str(row.get("source_trade_id", "")),
                str(row.get("source_statement_file", "seed.csv")),
                str(row.get("broker_buy_date", "")),
                str(row.get("broker_buy_price_ccy", "")),
                str(row.get("broker_buy_fx_to_eur", "")),
                str(row.get("broker_original_cost_eur", "")),
                str(row.get("austrian_basis_method", "move_in_fmv_reset")),
                str(row.get("austrian_basis_price_ccy", "")),
                str(row.get("austrian_basis_fx_to_eur", "")),
            ]
        )
        + "\n"
        for row in rows
    )
    path.write_text(header + body, encoding="utf-8")


def _write_trade_history_xml(path: Path, trade_rows: list[str]) -> None:
    path.write_text(
        "<FlexQueryResponse><FlexStatements count=\"1\"><FlexStatement><TradeConfirms>\n"
        + "\n".join(trade_rows)
        + "\n</TradeConfirms></FlexStatement></FlexStatements></FlexQueryResponse>\n",
        encoding="utf-8",
    )


def _write_closed_lot_xml(path: Path, lot_rows: list[str]) -> None:
    path.write_text(
        "<FlexQueryResponse><FlexStatements count=\"1\"><FlexStatement><Trades>\n"
        + "\n".join(lot_rows)
        + "\n</Trades></FlexStatement></FlexStatements></FlexQueryResponse>\n",
        encoding="utf-8",
    )


def _trade_confirm_row(
    *,
    ticker: str,
    isin: str,
    sub_category: str,
    trade_date: str,
    date_time: str,
    operation: str,
    quantity: str,
    price: str,
    trade_id: str,
    currency: str = "USD",
    extra_attrs: dict[str, str] | None = None,
) -> str:
    extra = ""
    if extra_attrs:
        extra = " " + " ".join(f'{key}="{value}"' for key, value in extra_attrs.items())
    return (
        f"<TradeConfirm accountId=\"U1\" symbol=\"{ticker}\" isin=\"{isin}\" subCategory=\"{sub_category}\" "
        f"assetCategory=\"STK\" currency=\"{currency}\" tradeDate=\"{trade_date}\" dateTime=\"{date_time}\" "
        f"buySell=\"{operation}\" quantity=\"{quantity}\" tradePrice=\"{price}\" transactionID=\"{trade_id}\"{extra} />"
    )


def _closed_lot_row(
    *,
    ticker: str,
    isin: str,
    sub_category: str,
    sale_date: str,
    sale_datetime: str,
    buy_datetime: str,
    quantity: str,
    cost: str,
    pnl: str,
    sale_trade_id: str,
    currency: str = "USD",
) -> str:
    return (
        f"<Lot accountId=\"U1\" currency=\"{currency}\" assetCategory=\"STK\" subCategory=\"{sub_category}\" "
        f"symbol=\"{ticker}\" isin=\"{isin}\" tradeDate=\"{sale_date}\" dateTime=\"{sale_datetime}\" "
        f"quantity=\"{quantity}\" cost=\"{cost}\" fifoPnlRealized=\"{pnl}\" transactionID=\"{sale_trade_id}\" "
        f"buySell=\"SELL\" openDateTime=\"{buy_datetime}\" levelOfDetail=\"CLOSED_LOT\" />"
    )


@pytest.mark.parametrize(
    ("sub_category", "ticker", "isin"),
    [
        ("COMMON", "AAPL", "US0378331005"),
        ("ADR", "RELX", "US7595301083"),
        ("REIT", "O", "US7561091049"),
    ],
)
def test_process_trades_ibkr_authoritative_uses_snapshot_basis_for_snapshot_covered_stock_like_lot(
    tmp_path: Path,
    sub_category: str,
    ticker: str,
    isin: str,
):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": sub_category,
                "ticker": ticker,
                "isin": isin,
                "lot_id": f"{ticker}:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 2.0,
                "remaining_quantity": 2.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 200.0,
                "adjusted_basis_eur": 200.0,
                "broker_buy_date": "2023-04-25",
                "broker_buy_price_ccy": 80.0,
                "broker_buy_fx_to_eur": 1.0,
                "broker_original_cost_eur": 160.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker=ticker,
                isin=isin,
                sub_category=sub_category,
                trade_date="2025-06-03",
                date_time="2025-06-03 10:00:00",
                operation="SELL",
                quantity="-2",
                price="120",
                trade_id="sell-1",
            )
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker=ticker,
                isin=isin,
                sub_category=sub_category,
                sale_date="2025-06-03",
                sale_datetime="2025-06-03 10:00:00",
                buy_datetime="2023-04-25 09:31:38",
                quantity="2",
                cost="160",
                pnl="80",
                sale_trade_id="sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 6, 3)],
            Column.currency: ["USD", "USD"],
            Column.exchange_rate: [1.0, 1.0],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        austrian_opening_lots_path=str(opening_path),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is not None
    assert summary_df is not None
    assert detail_df["basis_origin"].to_list() == ["snapshot"]
    assert detail_df["taxable_original_basis_eur"].to_list() == [200.0]
    assert detail_df["taxable_gain_loss_eur"].to_list() == [40.0]
    assert "taxable_stepup_basis_eur" not in detail_df.columns
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None
    assert trades_reconciliation_df["reconciliation_status"].to_list() == ["informational"]
    assert stock_lot_state_df["lot_id"].to_list() == [f"{ticker}:snapshot"]
    assert stock_lot_state_df["status"].to_list() == ["closed"]
    assert stock_lot_state_df["remaining_quantity"].to_list() == [0.0]


def test_process_trades_ibkr_authoritative_returns_partial_and_open_stock_lot_state(tmp_path: Path):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "COMMON",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "lot_id": "AAPL:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 3.0,
                "remaining_quantity": 3.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 300.0,
                "adjusted_basis_eur": 300.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2025-02-01",
                date_time="2025-02-01 12:00:00",
                operation="BUY",
                quantity="1",
                price="110",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2025-06-03",
                date_time="2025-06-03 10:00:00",
                operation="SELL",
                quantity="-2",
                price="130",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2025-06-03",
                sale_datetime="2025-06-03 10:00:00",
                buy_datetime="2023-04-25 09:31:38",
                quantity="2",
                cost="160",
                pnl="100",
                sale_trade_id="sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 2, 1), date(2025, 6, 3)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    _, _, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        austrian_opening_lots_path=str(opening_path),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None
    snapshot_row = stock_lot_state_df.filter(pl.col("lot_id") == "AAPL:snapshot")
    assert snapshot_row["status"].to_list() == ["partially_sold"]
    assert snapshot_row["remaining_quantity"].to_list() == [1.0]
    assert snapshot_row["original_cost_eur"].to_list() == [100.0]
    assert snapshot_row["initial_original_cost_eur"].to_list() == [300.0]

    post_move_row = stock_lot_state_df.filter(pl.col("basis_origin") == "post_move_buy")
    assert post_move_row["status"].to_list() == ["open"]
    assert post_move_row["remaining_quantity"].to_list() == [1.0]
    assert post_move_row["initial_original_cost_eur"].to_list() == [110.0]


def test_process_trades_ibkr_authoritative_matches_post_move_non_snapshot_stock_like_closed_lot(tmp_path: Path):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "COMMON",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "lot_id": "AAPL:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 1.0,
                "remaining_quantity": 1.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 100.0,
                "adjusted_basis_eur": 100.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-01-02",
                date_time="2025-01-02 10:00:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-03-01",
                date_time="2025-03-01 11:00:00",
                operation="SELL",
                quantity="-2",
                price="130",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                sale_date="2025-03-01",
                sale_datetime="2025-03-01 11:00:00",
                buy_datetime="2025-01-02 10:00:00",
                quantity="2",
                cost="200",
                pnl="60",
                sale_trade_id="broker-sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 1, 2), date(2025, 3, 1)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    detail_df, _, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        austrian_opening_lots_path=str(opening_path),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is not None
    assert detail_df["basis_origin"].to_list() == ["post_move_buy"]
    assert trades_reconciliation_df is not None
    assert trades_reconciliation_df["reconciliation_status"].to_list() == ["matched"]
    assert trades_reconciliation_df["sale_aggregate_status"].to_list() == ["matched"]
    assert stock_lot_state_df is not None


def test_process_trades_ibkr_authoritative_mixed_sale_uses_segmented_reconciliation(tmp_path: Path):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "COMMON",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "lot_id": "AAPL:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 2.0,
                "remaining_quantity": 2.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 200.0,
                "adjusted_basis_eur": 200.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2025-02-01",
                date_time="2025-02-01 12:00:00",
                operation="BUY",
                quantity="1",
                price="110",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                trade_date="2025-06-03",
                date_time="2025-06-03 10:00:00",
                operation="SELL",
                quantity="-3",
                price="130",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2025-06-03",
                sale_datetime="2025-06-03 10:00:00",
                buy_datetime="2023-04-25 09:31:38",
                quantity="2",
                cost="160",
                pnl="100",
                sale_trade_id="sell-1",
            ),
            _closed_lot_row(
                ticker="AAPL",
                isin="US0378331005",
                sub_category="COMMON",
                sale_date="2025-06-03",
                sale_datetime="2025-06-03 10:00:00",
                buy_datetime="2025-02-01 12:00:00",
                quantity="1",
                cost="110",
                pnl="20",
                sale_trade_id="sell-1",
            ),
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 2, 1), date(2025, 6, 3)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    detail_df, _, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        austrian_opening_lots_path=str(opening_path),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is not None
    assert set(detail_df["basis_origin"].to_list()) == {"snapshot", "post_move_buy"}
    assert trades_reconciliation_df is not None
    assert set(trades_reconciliation_df["reconciliation_status"].to_list()) == {"informational", "matched"}
    assert trades_reconciliation_df["sale_aggregate_status"].unique().to_list() == ["matched"]
    assert stock_lot_state_df is not None


def test_process_trades_ibkr_authoritative_fails_when_post_move_exact_segment_mismatches(tmp_path: Path):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "COMMON",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "lot_id": "AAPL:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 1.0,
                "remaining_quantity": 1.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 100.0,
                "adjusted_basis_eur": 100.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-01-02",
                date_time="2025-01-02 10:00:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-1",
            ),
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-03-01",
                date_time="2025-03-01 11:00:00",
                operation="SELL",
                quantity="-2",
                price="130",
                trade_id="sell-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                sale_date="2025-03-01",
                sale_datetime="2025-03-01 11:00:00",
                buy_datetime="2025-01-02 10:00:00",
                quantity="2",
                cost="210",
                pnl="50",
                sale_trade_id="sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 1, 2), date(2025, 3, 1)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="Post-move basis mismatch"):
        process_trades_ibkr(
            xml_file_path=str(closed_lot_path),
            exchange_rates_df=rates_df,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            austrian_opening_lots_path=str(opening_path),
            ibkr_trade_history_path=str(trade_history_path),
        )


def test_process_trades_ibkr_authoritative_uses_gross_tax_math_and_fee_adjusted_reconciliation(tmp_path: Path):
    opening_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "COMMON",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "lot_id": "AAPL:snapshot",
                "buy_date": "2024-05-01",
                "original_quantity": 1.0,
                "remaining_quantity": 1.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 100.0,
                "adjusted_basis_eur": 100.0,
            }
        ],
    )
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-01-02",
                date_time="2025-01-02 10:00:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-1",
                extra_attrs={"netCash": "-200.5"},
            ),
            _trade_confirm_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                trade_date="2025-03-01",
                date_time="2025-03-01 11:00:00",
                operation="SELL",
                quantity="-2",
                price="130",
                trade_id="sell-1",
                extra_attrs={"netCash": "259.5"},
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="CRM",
                isin="US79466L3024",
                sub_category="COMMON",
                sale_date="2025-03-01",
                sale_datetime="2025-03-01 11:00:00",
                buy_datetime="2025-01-02 10:00:00",
                quantity="2",
                cost="200.5",
                pnl="59.0",
                sale_trade_id="broker-sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 5, 1), date(2025, 1, 2), date(2025, 3, 1)],
            Column.currency: ["USD", "USD", "USD"],
            Column.exchange_rate: [1.0, 1.0, 1.0],
        }
    )

    detail_df, _, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        austrian_opening_lots_path=str(opening_path),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is not None
    assert detail_df["taxable_original_basis_eur"].to_list() == [200.0]
    assert detail_df["taxable_proceeds_eur"].to_list() == [260.0]
    assert detail_df["taxable_gain_loss_eur"].to_list() == [60.0]
    assert detail_df["allocated_buy_fee_eur"].to_list() == [0.5]
    assert detail_df["allocated_sale_fee_eur"].to_list() == [0.5]
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None
    assert trades_reconciliation_df["reconciliation_status"].to_list() == ["matched"]


def test_process_trades_ibkr_without_snapshot_replays_prior_raw_buys_and_ignores_fees_in_tax_math(
    tmp_path: Path,
):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-10-10",
                date_time="2024-10-10 09:30:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-2024-1",
                extra_attrs={"netCash": "-200.5"},
            ),
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2025-03-01",
                date_time="2025-03-01 11:00:00",
                operation="SELL",
                quantity="-2",
                price="130",
                trade_id="sell-2025-1",
                extra_attrs={"netCash": "259.5"},
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(
        closed_lot_path,
        [
            _closed_lot_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                sale_date="2025-03-01",
                sale_datetime="2025-03-01 11:00:00",
                buy_datetime="2024-10-10 09:30:00",
                quantity="2",
                cost="200.5",
                pnl="59.0",
                sale_trade_id="broker-sell-1",
            )
        ],
    )
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 10, 10), date(2025, 3, 1)],
            Column.currency: ["USD", "USD"],
            Column.exchange_rate: [1.0, 1.0],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is not None
    assert summary_df is not None
    assert stock_lot_state_df is not None
    assert trades_reconciliation_df is not None
    assert detail_df["basis_origin"].to_list() == ["post_move_buy"]
    assert detail_df["taxable_original_basis_eur"].to_list() == [200.0]
    assert detail_df["taxable_proceeds_eur"].to_list() == [260.0]
    assert detail_df["taxable_gain_loss_eur"].to_list() == [60.0]
    assert detail_df["allocated_buy_fee_eur"].to_list() == [0.5]
    assert detail_df["allocated_sale_fee_eur"].to_list() == [0.5]
    assert trades_reconciliation_df["reconciliation_status"].to_list() == ["matched"]
    assert trades_reconciliation_df["sale_aggregate_status"].to_list() == ["matched"]
    lot_row = stock_lot_state_df.filter(pl.col("lot_id") == "MSFT:2024-10-10:buy-2024-1")
    assert lot_row["status"].to_list() == ["closed"]
    assert lot_row["remaining_quantity"].to_list() == [0.0]
    assert lot_row["initial_original_cost_eur"].to_list() == [200.0]


def test_process_trades_ibkr_requires_snapshot_when_authoritative_start_date_is_set(tmp_path: Path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-10-10",
                date_time="2024-10-10 09:30:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-2024-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(closed_lot_path, [])
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 10, 10)],
            Column.currency: ["USD"],
            Column.exchange_rate: [1.0],
        }
    )

    with pytest.raises(ValueError, match="authoritative_start_date requires austrian_opening_lots_path"):
        process_trades_ibkr(
            xml_file_path=str(closed_lot_path),
            exchange_rates_df=rates_df,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            ibkr_trade_history_path=str(trade_history_path),
            authoritative_start_date=date(2024, 5, 1),
        )


def test_process_trades_ibkr_quiet_year_keeps_open_lot_state(tmp_path: Path):
    trade_history_path = tmp_path / "history.xml"
    _write_trade_history_xml(
        trade_history_path,
        [
            _trade_confirm_row(
                ticker="MSFT",
                isin="US5949181045",
                sub_category="COMMON",
                trade_date="2024-10-10",
                date_time="2024-10-10 09:30:00",
                operation="BUY",
                quantity="2",
                price="100",
                trade_id="buy-2024-1",
            ),
        ],
    )
    closed_lot_path = tmp_path / "closed.xml"
    _write_closed_lot_xml(closed_lot_path, [])
    rates_df = pl.DataFrame(
        {
            Column.rate_date: [date(2024, 10, 10)],
            Column.currency: ["USD"],
            Column.exchange_rate: [1.0],
        }
    )

    detail_df, summary_df, stock_lot_state_df, trades_reconciliation_df = process_trades_ibkr(
        xml_file_path=str(closed_lot_path),
        exchange_rates_df=rates_df,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        ibkr_trade_history_path=str(trade_history_path),
    )

    assert detail_df is None
    assert summary_df is None
    assert trades_reconciliation_df is None
    assert stock_lot_state_df is not None
    assert stock_lot_state_df["ticker"].to_list() == ["MSFT"]
    assert stock_lot_state_df["status"].to_list() == ["open"]
    assert stock_lot_state_df["remaining_quantity"].to_list() == [2.0]
    assert stock_lot_state_df["original_cost_eur"].to_list() == [200.0]
    assert stock_lot_state_df["initial_original_cost_eur"].to_list() == [200.0]


def test_calculate_summary_ibkr_rejects_duplicate_sections(dividends_country_summary_df):
    with pytest.raises(ValueError, match="Duplicate IBKR summary section: dividends"):
        calculate_summary_ibkr(
            sections=[
                IbkrSummarySection("dividends", dividends_country_summary_df),
                IbkrSummarySection("dividends", dividends_country_summary_df),
            ]
        )


def test_calculate_summary_ibkr_empty_sections_returns_empty_summary():
    result = calculate_summary_ibkr(sections=[])

    expected_columns = {
        Column.type,
        Column.currency,
        Column.profit_total,
        Column.profit_euro_total,
        Column.profit_euro_net_total,
        Column.withholding_tax_euro_total,
        Column.kest_gross_total,
        Column.kest_net_total,
    }

    assert result.is_empty()
    assert set(result.columns) == expected_columns
