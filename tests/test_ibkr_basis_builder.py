from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from scripts.ibkr_basis_builder.workflow import build_opening_lot_snapshot
from scripts.reporting_funds.workflow import run_workflow


def _write_rates_csv(path: Path, rows: list[tuple[str, str, float]]) -> None:
    path.write_text(
        "TIME_PERIOD,CURRENCY,CURRENCY_DENOM,OBS_VALUE\n"
        + "".join(f"{rate_date},{currency},EUR,{rate}\n" for rate_date, currency, rate in rows)
    )


def _write_price_csv(path: Path, rows: list[tuple[str, str, str, float, str]]) -> None:
    path.write_text(
        "ticker,isin,cutoff_date,price_ccy,currency\n"
        + "".join(f"{ticker},{isin},{cutoff_date},{price_ccy},{currency}\n" for ticker, isin, cutoff_date, price_ccy, currency in rows),
        encoding="utf-8",
    )


def _write_trade_xml(path: Path, trade_rows: list[str]) -> None:
    path.write_text(
        "<FlexQueryResponse><FlexStatements count=\"1\"><FlexStatement><Trades>\n"
        + "\n".join(trade_rows)
        + "\n</Trades></FlexStatement></FlexStatements></FlexQueryResponse>\n",
        encoding="utf-8",
    )


def _write_tax_xml(path: Path, *, cash_rows: list[str], accrual_rows: list[str]) -> None:
    path.write_text(
        "<FlexQueryResponse><FlexStatements count=\"1\"><FlexStatement>"
        "<CashTransactions>\n"
        + "\n".join(cash_rows)
        + "\n</CashTransactions>"
        "<ChangeInDividendAccruals>\n"
        + "\n".join(accrual_rows)
        + "\n</ChangeInDividendAccruals>"
        "</FlexStatement></FlexStatements></FlexQueryResponse>\n",
        encoding="utf-8",
    )


def _trade_row(
    *,
    ticker: str,
    isin: str,
    trade_date: str,
    date_time: str,
    operation: str,
    quantity: str,
    price: str,
    transaction_id: str,
    sub_category: str,
) -> str:
    return (
        f"<Trade accountId=\"U1\" symbol=\"{ticker}\" isin=\"{isin}\" subCategory=\"{sub_category}\" assetCategory=\"STK\" "
        f"currency=\"USD\" tradeDate=\"{trade_date}\" dateTime=\"{date_time}\" buySell=\"{operation}\" "
        f"quantity=\"{quantity}\" tradePrice=\"{price}\" transactionID=\"{transaction_id}\" />"
    )


def _write_oekb_file(
    path: Path,
    *,
    isin: str,
    meldedatum: str,
    jahresmeldung: str,
    ausschuettungsmeldung: str,
    value_10286: str = "0,0000",
    value_10287: str = "0,0000",
    value_10595: str = "0,0000",
    value_10288: str = "0,0000",
    value_10289: str = "0,0000",
    currency: str = "USD",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "BASISINFORMATION Anteilsgattung - Stammdaten",
                "======================================",
                f"ISIN;{isin}",
                f"Währung;{currency}",
                "",
                "BASISINFORMATION Steuermeldung - Weitere Informationen (enthält auch Informationen zur Steuermeldung, auf die sich alle weiteren Daten beziehen)",
                "======================================",
                f"Meldedatum;{meldedatum}",
                f"Jahresmeldung;{jahresmeldung}",
                f"Ausschüttungsmeldung;{ausschuettungsmeldung}",
                "",
                "Kennzahlen ESt-Erklärung Privatanleger (je Anteil)",
                "======================================",
                "BEZEICHNUNG;PA_MIT_OPTION;PA_OHNE_OPTION;STEUERNAME;STEUERCODE",
                f"Ausschüttungen 27,5%;{value_10286};{value_10286};x;10286",
                f"Ausschüttungsgleiche Erträge 27,5%;{value_10287};{value_10287};x;10287",
                f"Nicht gemeldete Ausschüttungen;{value_10595};{value_10595};x;10595",
                f"Anzurechnende ausländische Quellensteuer;{value_10288};{value_10288};x;10288",
                f"Die Anschaffungskosten des Fondsanteils sind zu korrigieren um;{value_10289};{value_10289};x;10289",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_build_opening_lot_snapshot_resets_pre_cutoff_open_stock_and_etf_lots(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-04-01", "USD", 1.0),
            ("2024-04-15", "USD", 1.0),
            ("2024-05-01", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-04-01",
                date_time="2024-04-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="80",
                transaction_id="vusd-buy",
                sub_category="ETF",
            ),
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-04-20",
                date_time="2024-04-20 10:00:00",
                operation="SELL",
                quantity="3",
                price="82",
                transaction_id="vusd-sell",
                sub_category="ETF",
            ),
            _trade_row(
                ticker="AAPL",
                isin="US0378331005",
                trade_date="2024-04-15",
                date_time="2024-04-15 10:00:00",
                operation="BUY",
                quantity="2",
                price="170",
                transaction_id="aapl-buy",
                sub_category="COMMON",
            ),
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-05-01",
                date_time="2024-05-01 12:00:00",
                operation="BUY",
                quantity="1",
                price="81",
                transaction_id="vusd-cutoff-day-buy",
                sub_category="ETF",
            ),
        ],
    )
    prices_path = tmp_path / "prices.csv"
    _write_price_csv(
        prices_path,
        [
            ("VUSD", "IE00B3XXRP09", "2024-05-01", 120.0, "USD"),
            ("AAPL", "US0378331005", "2024-05-01", 200.0, "USD"),
        ],
    )
    output_path = tmp_path / "opening_lots.csv"

    build_opening_lot_snapshot(
        person="eugene",
        cutoff_date=date(2024, 5, 1),
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        move_in_price_csv_path=prices_path,
        output_path=output_path,
    )

    snapshot_df = pl.read_csv(output_path).sort(["asset_class", "ticker"])
    assert snapshot_df["ticker"].to_list() == ["AAPL", "VUSD"]
    assert snapshot_df["asset_class"].to_list() == ["COMMON", "ETF"]
    assert snapshot_df["buy_date"].to_list() == ["2024-05-01", "2024-05-01"]
    assert snapshot_df["remaining_quantity"].to_list() == [2.0, 7.0]
    assert snapshot_df["original_cost_eur"].to_list() == [400.0, 840.0]
    assert snapshot_df["austrian_basis_method"].to_list() == ["move_in_fmv_reset", "move_in_fmv_reset"]


def test_build_opening_lot_snapshot_writes_price_template_when_prices_are_missing(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-04-01", "USD", 1.0),
            ("2024-04-15", "USD", 1.0),
            ("2024-05-01", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-04-01",
                date_time="2024-04-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="80",
                transaction_id="vusd-buy",
                sub_category="ETF",
            ),
            _trade_row(
                ticker="AAPL",
                isin="US0378331005",
                trade_date="2024-04-15",
                date_time="2024-04-15 10:00:00",
                operation="BUY",
                quantity="2",
                price="170",
                transaction_id="aapl-buy",
                sub_category="COMMON",
            ),
        ],
    )
    template_path = tmp_path / "move_in_prices.template.csv"

    with pytest.raises(ValueError, match="Wrote template with required holdings"):
        build_opening_lot_snapshot(
            person="eugene",
            cutoff_date=date(2024, 5, 1),
            ibkr_trade_history_path=trade_history_path,
            raw_exchange_rates_path=rates_path,
            move_in_price_csv_path=tmp_path / "missing_prices.csv",
            output_path=tmp_path / "opening_lots.csv",
            move_in_price_template_path=template_path,
        )

    template_df = pl.read_csv(template_path).sort(["asset_class", "ticker"])
    assert template_df["ticker"].to_list() == ["AAPL", "VUSD"]
    assert template_df["remaining_quantity"].to_list() == [2.0, 10.0]
    assert template_df["cutoff_date"].to_list() == ["2024-05-01", "2024-05-01"]


def test_reporting_funds_workflow_can_seed_from_opening_lot_snapshot(tmp_path: Path) -> None:
    cutoff_date = date(2024, 5, 1)
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-04-01", "USD", 1.0),
            ("2024-05-01", "USD", 1.0),
            ("2024-07-01", "USD", 1.0),
            ("2025-10-27", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-04-01",
                date_time="2024-04-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="80",
                transaction_id="pre-cutoff-buy",
                sub_category="ETF",
            ),
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-07-01",
                date_time="2024-07-01 10:00:00",
                operation="BUY",
                quantity="4",
                price="90",
                transaction_id="post-cutoff-buy",
                sub_category="ETF",
            ),
        ],
    )
    prices_path = tmp_path / "prices.csv"
    _write_price_csv(
        prices_path,
        [
            ("VUSD", "IE00B3XXRP09", cutoff_date.isoformat(), 100.0, "USD"),
        ],
    )
    opening_lots_path = tmp_path / "opening_lots.csv"
    build_opening_lot_snapshot(
        person="eugene",
        cutoff_date=cutoff_date,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        move_in_price_csv_path=prices_path,
        output_path=opening_lots_path,
    )

    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(tax_xml_path, cash_rows=[], accrual_rows=[])
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B3XXRP09",
        meldedatum="27.10.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        value_10289="1,0000",
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        strict_unresolved_payouts=False,
        opening_lots_path=opening_lots_path,
    )

    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    ledger_df = pl.read_csv(output_paths["ledger"]).sort(["buy_date", "lot_id"])

    assert basis_df["shares_held_on_eligibility_date"].to_list() == [14.0]
    assert ledger_df["remaining_quantity"].to_list() == [10.0, 4.0]
    assert ledger_df["original_cost_eur"].to_list() == [1000.0, 360.0]
