from pathlib import Path

import polars as pl

from scripts.non_reporting_funds_exit.workflow import load_price_rows, run_workflow

STATEMENT_PATH = Path(
    "data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json"
)
RAW_RATES_PATH = Path("data/input/currencies/raw_exchange_rates.csv")


def _write_price_input(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "tax_year,ticker,isin,trade_currency,first_price_ccy,last_price_ccy,notes",
                "2025,SCHD.US,US8085247976,USD,29,30,test input",
                "2025,TLT.US,US4642874329,USD,85,80,test input",
            ]
        )
        + "\n"
    )


def _write_sale_plan(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "ticker,sale_date,quantity,sale_price_ccy",
                "SCHD.US,2026-03-17,375,30.87",
                "TLT.US,2026-03-17,43,87.21",
            ]
        )
        + "\n"
    )


def test_load_price_rows_keeps_only_supported_rows_present_for_year(tmp_path):
    price_input_path = tmp_path / "prices.csv"
    price_input_path.write_text(
        "\n".join(
            [
                "tax_year,ticker,isin,trade_currency,first_price_ccy,last_price_ccy,notes",
                "2025,TLT.US,US4642874329,USD,85,80,test input",
                "2025,FOO.US,US0000000000,USD,1,2,ignore me",
                "2024,SCHD.US,US8085247976,USD,29,30,wrong year",
            ]
        )
        + "\n"
    )

    price_rows = load_price_rows(price_input_path, tax_year=2025)

    assert list(price_rows) == ["TLT.US"]


def test_run_workflow_rebuilds_lots_and_allocates_2025_stepup(tmp_path):
    price_input_path = tmp_path / "prices.csv"
    _write_price_input(price_input_path)
    sale_plan_path = tmp_path / "sale_plan.csv"
    sale_plan_path.write_text("ticker,sale_date,quantity,sale_price_ccy\n")
    output_dir = tmp_path / "output"

    output_paths = run_workflow(
        statement_path=STATEMENT_PATH,
        price_input_path=price_input_path,
        sale_plan_path=sale_plan_path,
        output_dir=output_dir,
        raw_exchange_rates_path=RAW_RATES_PATH,
    )

    ledger_df = pl.read_csv(output_paths["working_ledger"])
    calc_df = pl.read_csv(output_paths["calc"]).sort("ticker")
    basis_df = pl.read_csv(output_paths["basis_adjustments"]).sort(["ticker", "lot_id"])

    assert calc_df.select("ticker", "shares_held_year_end").to_dict(as_series=False) == {
        "ticker": ["SCHD.US", "TLT.US"],
        "shares_held_year_end": [330.0, 43.0],
    }

    schd_df = ledger_df.filter(pl.col("ticker") == "SCHD.US").sort("buy_date")
    tlt_df = ledger_df.filter(pl.col("ticker") == "TLT.US").sort("buy_date")
    assert schd_df["remaining_quantity"].sum() == 375.0
    assert tlt_df["remaining_quantity"].sum() == 43.0
    assert schd_df["original_quantity"].head(2).to_list() == [21.0, 21.0]

    schd_2026_lot_df = schd_df.filter(pl.col("buy_date") == "2026-02-04")
    assert schd_2026_lot_df.height == 1
    assert schd_2026_lot_df["remaining_quantity"].item() == 45.0
    assert schd_2026_lot_df["cumulative_stepup_eur"].item() == 0.0

    stepup_by_ticker = basis_df.group_by("ticker").agg(pl.sum("stepup_eur").alias("stepup_eur")).sort("ticker")
    deemed_by_ticker = calc_df.select("ticker", "deemed_amount_eur").sort("ticker")
    assert stepup_by_ticker["stepup_eur"].round(6).to_list() == deemed_by_ticker["deemed_amount_eur"].round(6).to_list()


def test_run_workflow_sale_plan_uses_fifo_and_keeps_fees_informational_only(tmp_path):
    price_input_path = tmp_path / "prices.csv"
    _write_price_input(price_input_path)
    sale_plan_path = tmp_path / "sale_plan.csv"
    _write_sale_plan(sale_plan_path)
    output_dir = tmp_path / "output"

    output_paths = run_workflow(
        statement_path=STATEMENT_PATH,
        price_input_path=price_input_path,
        sale_plan_path=sale_plan_path,
        output_dir=output_dir,
        raw_exchange_rates_path=RAW_RATES_PATH,
    )

    sales_df = pl.read_csv(output_paths["sales"]).sort(["ticker", "lot_buy_date"])

    assert sales_df.filter(pl.col("ticker") == "SCHD.US")["quantity_from_lot"].sum() == 375.0
    assert sales_df.filter(pl.col("ticker") == "TLT.US")["quantity_from_lot"].sum() == 43.0
    assert sales_df.filter(pl.col("ticker") == "SCHD.US")["lot_buy_date"].head(3).to_list() == [
        "2024-04-19",
        "2024-05-30",
        "2024-12-19",
    ]

    recomputed_basis = (
        sales_df["taxable_original_basis_eur"] + sales_df["taxable_stepup_basis_eur"]
    ).round(6)
    assert recomputed_basis.to_list() == sales_df["taxable_total_basis_eur"].round(6).to_list()

    fee_gap_df = sales_df.filter(
        pl.col("informational_buy_cost_ccy_incl_fees") > pl.col("informational_buy_cost_ccy_excl_fees")
    )
    assert fee_gap_df.height > 0
    assert all(
        note == "Taxable result excludes buy and sell fees under Austrian private-investor rules."
        for note in sales_df["notes"].to_list()
    )
