from datetime import date
from pathlib import Path

import polars as pl

from scripts.non_reporting_funds_exit.ibkr_lots import load_ibkr_reit_trades, load_opening_lots
from scripts.non_reporting_funds_exit.workflow import run_ibkr_reit_workflow

RAW_RATES_PATH = Path("data/input/currencies/raw_exchange_rates.csv")


def _write_opening_state_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "snapshot_date,broker,ticker,isin,currency,asset_class,quantity,base_cost_total_eur,basis_adjustment_total_eur,total_basis_eur,average_basis_eur,status,last_event_date,basis_method,notes,source_file",
                '2024-05-01,ibkr,AAPL,US0378331005,USD,COMMON,15.0,2369.38,0.0,2369.38,157.96,open,2024-05-01,move_in_fmv_reset,"Opening position",some.xml',
                '2024-05-01,ibkr,CTRE,US14174T1079,USD,REIT,15.0,348.90,0.0,348.90,23.26,open,2024-05-01,move_in_fmv_reset,"Opening REIT position",some.xml',
                '2024-05-01,ibkr,O,US7561091049,USD,REIT,21.0,1056.47,0.0,1056.47,50.31,open,2024-05-01,move_in_fmv_reset,"Opening REIT position",some.xml',
            ]
        )
        + "\n"
    )


def _write_trade_history_xml(path: Path, trade_rows: list[str]) -> None:
    path.write_text(
        '<FlexQueryResponse><FlexStatements count="1"><FlexStatement><TradeConfirms>\n'
        + "\n".join(trade_rows)
        + "\n</TradeConfirms></FlexStatement></FlexStatements></FlexQueryResponse>\n",
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
) -> str:
    return (
        f'<TradeConfirm accountId="U1" symbol="{ticker}" isin="{isin}" subCategory="{sub_category}" '
        f'assetCategory="STK" currency="{currency}" tradeDate="{trade_date}" dateTime="{date_time}" '
        f'buySell="{operation}" quantity="{quantity}" tradePrice="{price}" transactionID="{trade_id}" />'
    )


def _write_price_input(path: Path, rows: list[str] | None = None) -> None:
    if rows is None:
        rows = [
            "2025,CTRE,US14174T1079,USD,25,30,test",
            "2025,O,US7561091049,USD,50,55,test",
        ]
    path.write_text(
        "tax_year,ticker,isin,trade_currency,first_price_ccy,last_price_ccy,notes\n"
        + "\n".join(rows)
        + "\n"
    )


def test_load_opening_lots_filters_reits_and_sets_eur_basis(tmp_path):
    csv_path = tmp_path / "opening.csv"
    _write_opening_state_csv(csv_path)

    lots = load_opening_lots(csv_path)

    assert len(lots) == 2
    tickers = [lot.ticker for lot in lots]
    assert "CTRE" in tickers
    assert "O" in tickers
    assert "AAPL" not in tickers

    ctre_lot = next(lot for lot in lots if lot.ticker == "CTRE")
    assert ctre_lot.original_cost_eur == 348.90
    assert ctre_lot.remaining_quantity == 15.0
    assert ctre_lot.buy_date == date(2024, 5, 1)
    assert ctre_lot.lot_id == "CTRE:opening:2024-05-01"
    assert ctre_lot.total_cost_ccy == 0.0
    assert ctre_lot.buy_fx == 0.0


def test_load_ibkr_reit_trades_converts_and_filters(tmp_path):
    xml_path = tmp_path / "trades.xml"
    _write_trade_history_xml(
        xml_path,
        [
            _trade_confirm_row(
                ticker="O", isin="US7561091049", sub_category="REIT",
                trade_date="2024-06-15", date_time="2024-06-15 10:00:00",
                operation="BUY", quantity="5", price="55.50", trade_id="reit-buy-1",
            ),
            _trade_confirm_row(
                ticker="AAPL", isin="US0378331005", sub_category="COMMON",
                trade_date="2024-06-15", date_time="2024-06-15 11:00:00",
                operation="BUY", quantity="1", price="200", trade_id="common-buy",
            ),
            _trade_confirm_row(
                ticker="O", isin="US7561091049", sub_category="REIT",
                trade_date="2024-04-01", date_time="2024-04-01 10:00:00",
                operation="BUY", quantity="3", price="50.00", trade_id="reit-buy-old",
            ),
        ],
    )

    trades = load_ibkr_reit_trades(xml_path, after_date=date(2024, 5, 1))

    assert len(trades) == 1
    assert trades[0].ticker == "O"
    assert trades[0].trade_date == date(2024, 6, 15)
    assert trades[0].operation == "buy"
    assert float(trades[0].quantity) == 5.0
    assert trades[0].trade_currency == "USD"

    all_trades = load_ibkr_reit_trades(xml_path, after_date=None)
    assert len(all_trades) == 2
    assert all(t.ticker == "O" for t in all_trades)


def test_ibkr_reit_workflow_calculates_age_and_adjusts_basis(tmp_path):
    opening_csv = tmp_path / "opening.csv"
    _write_opening_state_csv(opening_csv)

    xml_path = tmp_path / "trades.xml"
    _write_trade_history_xml(
        xml_path,
        [
            # Post-opening buy of more O shares
            _trade_confirm_row(
                ticker="O", isin="US7561091049", sub_category="REIT",
                trade_date="2024-08-15", date_time="2024-08-15 10:00:00",
                operation="BUY", quantity="4", price="55.00", trade_id="o-buy-post",
            ),
            # Sell some CTRE in 2025
            _trade_confirm_row(
                ticker="CTRE", isin="US14174T1079", sub_category="REIT",
                trade_date="2025-03-10", date_time="2025-03-10 10:00:00",
                operation="SELL", quantity="-5", price="28.00", trade_id="ctre-sell",
            ),
        ],
    )

    price_csv = tmp_path / "prices.csv"
    _write_price_input(price_csv)

    output_dir = tmp_path / "output"

    output_paths = run_ibkr_reit_workflow(
        opening_state_path=str(opening_csv),
        ibkr_trade_history_path=str(xml_path),
        price_input_path=str(price_csv),
        output_dir=str(output_dir),
        raw_exchange_rates_path=str(RAW_RATES_PATH),
        target_tickers=("CTRE", "O"),
    )

    calc_df = pl.read_csv(output_paths["calc"]).sort("ticker")
    ledger_df = pl.read_csv(output_paths["working_ledger"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])

    # CTRE: started with 15, sold 5 in 2025 → 10 held at year-end
    ctre_calc = calc_df.filter(pl.col("ticker") == "CTRE")
    assert ctre_calc["shares_held_year_end"].item() == 10.0

    # O: started with 21, bought 4 more → 25 held at year-end
    o_calc = calc_df.filter(pl.col("ticker") == "O")
    assert o_calc["shares_held_year_end"].item() == 25.0

    # Verify step-up totals match per-lot allocations
    stepup_by_ticker = basis_df.group_by("ticker").agg(pl.sum("stepup_eur").alias("stepup_eur")).sort("ticker")
    deemed_by_ticker = calc_df.select("ticker", "deemed_amount_eur").sort("ticker")
    assert stepup_by_ticker["stepup_eur"].round(6).to_list() == deemed_by_ticker["deemed_amount_eur"].round(6).to_list()

    # O has 2 lots (opening + post-opening buy)
    o_lots = ledger_df.filter(pl.col("ticker") == "O")
    assert o_lots.height == 2
    assert sorted(o_lots["remaining_quantity"].to_list()) == [4.0, 21.0]

    # CTRE has 1 lot, partially sold
    ctre_lots = ledger_df.filter(pl.col("ticker") == "CTRE")
    assert ctre_lots.height == 1
    assert ctre_lots["remaining_quantity"].item() == 10.0
    assert ctre_lots["status"].item() == "partially_sold"

    # All lots should have step-up applied
    assert all(row > 0 for row in ledger_df.filter(pl.col("remaining_quantity") > 0)["cumulative_stepup_eur"].to_list())

    # Summary file exists
    assert output_paths["summary"].exists()
    summary_text = output_paths["summary"].read_text()
    assert "IBKR REIT" in summary_text


def test_ibkr_reit_workflow_with_sale_simulation(tmp_path):
    opening_csv = tmp_path / "opening.csv"
    _write_opening_state_csv(opening_csv)

    xml_path = tmp_path / "trades.xml"
    _write_trade_history_xml(xml_path, [])

    price_csv = tmp_path / "prices.csv"
    _write_price_input(price_csv)

    sale_plan = tmp_path / "sales.csv"
    sale_plan.write_text(
        "ticker,sale_date,quantity,sale_price_ccy\n"
        "O,2026-03-15,21,60.00\n"
    )

    output_dir = tmp_path / "output"

    output_paths = run_ibkr_reit_workflow(
        opening_state_path=str(opening_csv),
        ibkr_trade_history_path=str(xml_path),
        price_input_path=str(price_csv),
        sale_plan_path=str(sale_plan),
        output_dir=str(output_dir),
        raw_exchange_rates_path=str(RAW_RATES_PATH),
        target_tickers=("CTRE", "O"),
    )

    sales_df = pl.read_csv(output_paths["sales"])

    assert sales_df.height == 1
    assert sales_df["ticker"].item() == "O"
    assert sales_df["quantity_from_lot"].item() == 21.0

    # Basis should include step-up
    assert sales_df["taxable_stepup_basis_eur"].item() > 0
    recomputed = (sales_df["taxable_original_basis_eur"] + sales_df["taxable_stepup_basis_eur"]).round(6)
    assert recomputed.to_list() == sales_df["taxable_total_basis_eur"].round(6).to_list()

    # Informational CCY fields are 0 for opening lots (FMV reset, no actual trade)
    assert sales_df["informational_buy_cost_ccy_excl_fees"].item() == 0.0
