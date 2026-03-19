from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from scripts.reporting_funds.ibkr_source import (
    build_broker_dividend_events,
    load_ibkr_etf_cash_dividend_rows,
    load_ibkr_etf_dividend_accrual_rows,
    load_ibkr_etf_trades,
)
from scripts.reporting_funds.oekb_csv import load_oekb_report, load_required_oekb_reports
from scripts.reporting_funds.workflow import basis_adjustments_to_df, run_workflow


def _write_rates_csv(path: Path, rows: list[tuple[str, str, float]]) -> None:
    path.write_text(
        "TIME_PERIOD,CURRENCY,CURRENCY_DENOM,OBS_VALUE\n"
        + "".join(f"{rate_date},{currency},EUR,{rate}\n" for rate_date, currency, rate in rows)
    )


def _write_negative_override_csv(path: Path, rows: list[tuple[str, str, str, str]]) -> None:
    path.write_text(
        "report_key,decision,eligible_quantity,notes\n"
        + "".join(f"{report_key},{decision},{eligible_quantity},{notes}\n" for report_key, decision, eligible_quantity, notes in rows),
        encoding="utf-8",
    )


def _write_oekb_file(
    path: Path,
    *,
    isin: str,
    meldedatum: str,
    jahresmeldung: str,
    ausschuettungsmeldung: str,
    ausschuettungstag: str = "",
    ex_tag: str = "",
    meldezeitraum_beginn: str = "",
    meldezeitraum_ende: str = "",
    geschaeftsjahres_beginn: str = "",
    geschaeftsjahres_ende: str = "",
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
                f"Geschäftsjahres-Beginn;{geschaeftsjahres_beginn}",
                f"Geschäftsjahres-Ende;{geschaeftsjahres_ende}",
                f"Meldezeitraum Beginn;{meldezeitraum_beginn}",
                f"Meldezeitraum Ende;{meldezeitraum_ende}",
                f"Ausschüttungstag;{ausschuettungstag}",
                f"Ex-Tag;{ex_tag}",
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
) -> str:
    return (
        f"<Trade accountId=\"U1\" symbol=\"{ticker}\" isin=\"{isin}\" subCategory=\"ETF\" currency=\"USD\" "
        f"tradeDate=\"{trade_date}\" dateTime=\"{date_time}\" buySell=\"{operation}\" quantity=\"{quantity}\" "
        f"tradePrice=\"{price}\" transactionID=\"{transaction_id}\" />"
    )


def _cash_dividend_row(
    *,
    ticker: str,
    isin: str,
    settle_date: str,
    ex_date: str,
    amount: str,
    action_id: str,
    report_date: str,
) -> str:
    return (
        f"<CashTransaction accountId=\"U1\" currency=\"USD\" assetCategory=\"STK\" subCategory=\"ETF\" "
        f"symbol=\"{ticker}\" isin=\"{isin}\" dateTime=\"{settle_date} 20:20:00\" settleDate=\"{settle_date}\" "
        f"amount=\"{amount}\" type=\"Dividends\" reportDate=\"{report_date}\" exDate=\"{ex_date}\" actionID=\"{action_id}\" />"
    )


def _accrual_row(
    *,
    ticker: str,
    isin: str,
    report_date: str,
    effective_date: str,
    ex_date: str,
    pay_date: str,
    quantity: str,
    code: str,
    action_id: str,
    gross_rate: str,
    gross_amount: str,
    net_amount: str = "",
    tax: str = "0",
) -> str:
    return (
        f"<ChangeInDividendAccrual accountId=\"U1\" currency=\"USD\" assetCategory=\"STK\" subCategory=\"ETF\" "
        f"symbol=\"{ticker}\" isin=\"{isin}\" reportDate=\"{report_date}\" date=\"{effective_date}\" "
        f"exDate=\"{ex_date}\" payDate=\"{pay_date}\" quantity=\"{quantity}\" tax=\"{tax}\" "
        f"grossRate=\"{gross_rate}\" grossAmount=\"{gross_amount}\" netAmount=\"{net_amount or gross_amount}\" "
        f"code=\"{code}\" actionID=\"{action_id}\" />"
    )


def test_load_oekb_report_parses_distribution_and_annual_fields() -> None:
    idtl = load_oekb_report(
        next(Path("data/input/oekb/2025").glob("IDTL_Ausschu*22.12.2025.csv")),
        tax_year=2025,
    )
    spy5 = load_oekb_report(
        next(Path("data/input/oekb/2025").glob("SPY5_Jahresdatenmeldung*.csv")),
        tax_year=2025,
    )

    assert idtl.is_ausschuettungsmeldung is True
    assert idtl.ex_tag == date(2025, 12, 11)
    assert idtl.ausschuettungstag == date(2025, 12, 24)
    assert idtl.acquisition_cost_correction_per_share_ccy == -0.0735

    assert spy5.is_jahresmeldung is True
    assert spy5.non_reported_distribution_per_share_ccy == 6.373
    assert spy5.creditable_foreign_tax_per_share_ccy == 0.9271


def test_load_required_oekb_reports_supports_multiple_files_per_isin_and_isin_matching(tmp_path: Path) -> None:
    oekb_dir = tmp_path / "oekb"
    _write_oekb_file(
        oekb_dir / "annual_any_name.csv",
        isin="IE00B6YX5C33",
        meldedatum="27.10.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        value_10287="1,0000",
        value_10289="0,5000",
    )
    _write_oekb_file(
        oekb_dir / "distribution_weird_name.csv",
        isin="IE00B6YX5C33",
        meldedatum="15.12.2025",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="20.12.2025",
        ex_tag="10.12.2025",
        value_10289="-0,2000",
    )

    reports = load_required_oekb_reports(
        oekb_dir,
        tax_year=2025,
        required_isins={"IE00B6YX5C33"},
        ticker_by_isin={"IE00B6YX5C33": "SPY5"},
    )

    assert [report.ticker for report in reports] == ["SPY5", "SPY5"]
    assert [report.is_jahresmeldung for report in reports] == [True, False]


def test_load_oekb_report_prefers_private_investor_section_in_full_export(tmp_path: Path) -> None:
    path = tmp_path / "full_export.csv"
    path.write_text(
        "\n".join(
            [
                "BASISINFORMATION Anteilsgattung - Stammdaten",
                "======================================",
                "ISIN;IE00B6YX5C33",
                "Währung;USD",
                "",
                "BASISINFORMATION Steuermeldung - Weitere Informationen (enthält auch Informationen zur Steuermeldung, auf die sich alle weiteren Daten beziehen)",
                "======================================",
                "Meldedatum;27.10.2025",
                "Jahresmeldung;JA",
                "Ausschüttungsmeldung;NEIN",
                "",
                "Kennzahlen ESt-Erklärung Privatanleger (je Anteil)",
                "======================================",
                "BEZEICHNUNG;PA_MIT_OPTION;PA_OHNE_OPTION;STEUERNAME;STEUERCODE",
                "Ausschüttungen 27,5%;0,0000;0,0000;x;10286",
                "Ausschüttungsgleiche Erträge 27,5%;11,2278;11,2278;x;10287",
                "Nicht gemeldete Ausschüttungen;6,3730;6,3730;x;10595",
                "Anzurechnende ausländische Quellensteuer;0,9271;0,9271;x;10288",
                "Die Anschaffungskosten des Fondsanteils sind zu korrigieren um;10,1944;10,1944;x;10289",
                "",
                "Ertragsteuerliche Behandlung (je Anteil)",
                "======================================",
                "POSITION;BEZEICHNUNG;PA_MIT_OPTION;PA_OHNE_OPTION;BV_MIT_OPTION;BV_OHNE_OPTION;BV_JUR_PERSON;STIFTUNG;STEUERNAME;STEUERCODE",
                "16.1;Ausschüttungen 27,5%;999,0000;999,0000;;;;;x;10286",
                "16.2;Ausschüttungsgleiche Erträge 27,5%;999,0000;999,0000;;;;;x;10287",
                "16.2.1;Nicht gemeldete Ausschüttungen;999,0000;999,0000;;;;;x;10595",
                "16.3;Anzurechnende ausländische Quellensteuer;999,0000;999,0000;;;;;x;10288",
                "16.4;Anschaffungskostenkorrektur;999,0000;999,0000;;;;;x;10289",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = load_oekb_report(path, tax_year=2025, ticker_by_isin={"IE00B6YX5C33": "SPY5"})

    assert report.ticker == "SPY5"
    assert report.reported_distribution_per_share_ccy == 0.0
    assert report.age_per_share_ccy == 11.2278
    assert report.non_reported_distribution_per_share_ccy == 6.373
    assert report.creditable_foreign_tax_per_share_ccy == 0.9271
    assert report.acquisition_cost_correction_per_share_ccy == 10.1944


def test_load_required_oekb_reports_missing_file_message_includes_ticker_and_isin(tmp_path: Path) -> None:
    oekb_dir = tmp_path / "oekb"
    oekb_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="VUSD \\(IE00B3XXRP09\\)"):
        load_required_oekb_reports(
            oekb_dir,
            tax_year=2025,
            required_isins={"IE00B3XXRP09"},
            ticker_by_isin={"IE00B3XXRP09": "VUSD"},
        )


def test_basis_adjustments_csv_groups_rows_by_ticker_then_date() -> None:
    df = basis_adjustments_to_df(
        [
            {
                "tax_year": 2025,
                "ticker": "VUSD",
                "isin": "IE00B3XXRP09",
                "report_type": "jahresmeldung",
                "eligibility_date": "2025-10-27",
                "effective_date": "2025-10-27",
                "currency": "USD",
                "acquisition_cost_correction_per_share_ccy": 1.0,
                "shares_held_on_eligibility_date": 10.0,
                "basis_stepup_total_ccy": 10.0,
                "basis_stepup_total_eur": 9.0,
                "fx_to_eur": 1.1,
                "source_file": "vusd-oct.csv",
                "notes": "",
            },
            {
                "tax_year": 2025,
                "ticker": "IDTL",
                "isin": "IE00BSKRJZ44",
                "report_type": "ausschuettungsmeldung",
                "eligibility_date": "2025-06-11",
                "effective_date": "2025-06-25",
                "currency": "USD",
                "acquisition_cost_correction_per_share_ccy": -0.1,
                "shares_held_on_eligibility_date": 100.0,
                "basis_stepup_total_ccy": -10.0,
                "basis_stepup_total_eur": -9.0,
                "fx_to_eur": 1.1,
                "source_file": "idtl-jun.csv",
                "notes": "",
            },
            {
                "tax_year": 2025,
                "ticker": "IDTL",
                "isin": "IE00BSKRJZ44",
                "report_type": "ausschuettungsmeldung",
                "eligibility_date": "2025-12-11",
                "effective_date": "2025-12-24",
                "currency": "USD",
                "acquisition_cost_correction_per_share_ccy": -0.2,
                "shares_held_on_eligibility_date": 100.0,
                "basis_stepup_total_ccy": -20.0,
                "basis_stepup_total_eur": -18.0,
                "fx_to_eur": 1.1,
                "source_file": "idtl-dec.csv",
                "notes": "",
            },
        ]
    )

    assert df["ticker"].to_list() == ["IDTL", "IDTL", "VUSD"]
    assert df["effective_date"].to_list() == ["2025-06-25", "2025-12-24", "2025-10-27"]


def test_build_broker_dividend_events_matches_cash_by_action_id_and_ignores_symbol_drift(tmp_path: Path) -> None:
    tax_xml = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml,
        cash_rows=[
            _cash_dividend_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                settle_date="2025-12-24",
                ex_date="2025-12-11",
                amount="40.43",
                action_id="same-action",
                report_date="2025-12-30",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="IDTLz",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="550",
                code="Po",
                action_id="same-action",
                gross_rate="0.0735",
                gross_amount="40.42",
            ),
            _accrual_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-30",
                effective_date="2025-12-24",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="550",
                code="Re",
                action_id="same-action",
                gross_rate="0.0735",
                gross_amount="-40.43",
            ),
        ],
    )

    events = build_broker_dividend_events(
        load_ibkr_etf_dividend_accrual_rows(str(tax_xml)),
        load_ibkr_etf_cash_dividend_rows(str(tax_xml)),
        tax_year=2025,
    )

    assert len(events) == 1
    assert events[0].ticker == "IDTL"
    assert events[0].gross_amount == 40.43
    assert "symbol drift ignored" in events[0].matching_notes


def test_build_broker_dividend_events_fails_on_cash_accrual_mismatch(tmp_path: Path) -> None:
    tax_xml = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2025-04-02",
                ex_date="2025-03-20",
                amount="9.49",
                action_id="bad-action",
                report_date="2025-04-03",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-03-20",
                effective_date="2025-03-19",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="14",
                code="Po",
                action_id="bad-action",
                gross_rate="0.32063",
                gross_amount="4.49",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-04-03",
                effective_date="2025-04-02",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="14",
                code="Re",
                action_id="bad-action",
                gross_rate="0.32063",
                gross_amount="-4.49",
            ),
        ],
    )

    with pytest.raises(ValueError, match="Cash/accrual gross amount mismatch"):
        build_broker_dividend_events(
            load_ibkr_etf_dividend_accrual_rows(str(tax_xml)),
            load_ibkr_etf_cash_dividend_rows(str(tax_xml)),
            tax_year=2025,
        )


def test_load_ibkr_etf_cash_dividend_rows_merges_semantic_duplicates_from_overlapping_files(tmp_path: Path) -> None:
    tax_dir = tmp_path / "tax"
    tax_dir.mkdir()
    _write_tax_xml(
        tax_dir / "sample.xml",
        cash_rows=[
            (
                "<CashTransaction accountId=\"-\" currency=\"USD\" assetCategory=\"STK\" subCategory=\"ETF\" "
                "symbol=\"IDTL\" isin=\"IE00BSKRJZ44\" dateTime=\"2025-12-24 20:20:00\" settleDate=\"2025-12-24\" "
                "amount=\"40.43\" type=\"Dividends\" reportDate=\"2025-12-30\" exDate=\"\" actionID=\"same-action\" />"
            )
        ],
        accrual_rows=[],
    )
    _write_tax_xml(
        tax_dir / "full.xml",
        cash_rows=[
            _cash_dividend_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                settle_date="2025-12-24",
                ex_date="2025-12-11",
                amount="40.43",
                action_id="same-action",
                report_date="2025-12-30",
            )
        ],
        accrual_rows=[],
    )

    rows = load_ibkr_etf_cash_dividend_rows(str(tax_dir))

    assert len(rows) == 1
    assert rows[0].action_id == "same-action"
    assert rows[0].ex_date == date(2025, 12, 11)
    assert "|" in rows[0].source_statement_file


def test_load_ibkr_etf_trades_reads_folder_deduplicates_overlap_and_filters_cash(tmp_path: Path) -> None:
    trades_dir = tmp_path / "ibkr-trades"
    trades_dir.mkdir()
    duplicate_trade = _trade_row(
        ticker="SPY5",
        isin="IE00B6YX5C33",
        trade_date="2025-04-01",
        date_time="2025-04-01 10:00:00",
        operation="BUY",
        quantity="3",
        price="100",
        transaction_id="same-trade",
    )
    _write_trade_xml(trades_dir / "part1.xml", [duplicate_trade])
    _write_trade_xml(
        trades_dir / "part2.xml",
        [
            duplicate_trade,
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2025-05-01",
                date_time="2025-05-01 10:00:00",
                operation="BUY",
                quantity="2",
                price="90",
                transaction_id="vusd-trade",
            ),
        ],
    )

    trades = load_ibkr_etf_trades(str(trades_dir), require_raw_trades=True)

    assert [(trade.ticker, trade.trade_id) for trade in trades] == [
        ("SPY5", "same-trade"),
        ("VUSD", "vusd-trade"),
    ]


def test_reporting_funds_workflow_historical_lock_bootstrap_ignores_pre_2025_reference(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-06-01", "USD", 1.0),
            ("2024-12-27", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
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
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="14",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_dir = tmp_path / "tax"
    tax_dir.mkdir()
    _write_tax_xml(
        tax_dir / "2024.xml",
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-12-27",
                ex_date="2024-12-12",
                amount="4.29",
                action_id="vusd-2024",
                report_date="2024-12-30",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-12-12",
                effective_date="2024-12-11",
                ex_date="2024-12-12",
                pay_date="2024-12-27",
                quantity="14",
                code="Po",
                action_id="vusd-2024",
                gross_rate="0.30615",
                gross_amount="4.29",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-12-30",
                effective_date="2024-12-27",
                ex_date="2024-12-12",
                pay_date="2024-12-27",
                quantity="14",
                code="Re",
                action_id="vusd-2024",
                gross_rate="0.30615",
                gross_amount="-4.29",
            ),
        ],
    )
    _write_tax_xml(tax_dir / "2025.xml", cash_rows=[], accrual_rows=[])

    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B3XXRP09",
        meldedatum="15.01.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        meldezeitraum_beginn="01.01.2024",
        meldezeitraum_ende="31.12.2024",
        value_10595="0,5468",
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=tax_dir,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        strict_unresolved_payouts=False,
    )

    payout_state_df = pl.read_csv(output_paths["payout_state"])
    historical_row = payout_state_df.filter(pl.col("payout_key") == "vusd-2024")
    assert historical_row["status"].to_list() == ["ignored_pre_2025_reference"]
    assert "ignored for tax and basis" in historical_row["notes"].to_list()[0]


def test_reporting_funds_workflow_resolves_same_year_distribution_and_writes_payout_state(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-06-01", "USD", 1.0),
            ("2025-12-11", "USD", 1.0),
            ("2025-12-24", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                settle_date="2025-12-24",
                ex_date="2025-12-11",
                amount="0.735",
                action_id="idtl-1",
                report_date="2025-12-30",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="IDTLz",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Po",
                action_id="idtl-1",
                gross_rate="0.0735",
                gross_amount="0.734",
                tax="0.11",
            ),
            _accrual_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-30",
                effective_date="2025-12-24",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Re",
                action_id="idtl-1",
                gross_rate="0.0735",
                gross_amount="-0.735",
                tax="0.11",
            ),
        ],
    )
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "idtl_distribution.csv",
        isin="IE00BSKRJZ44",
        meldedatum="22.12.2025",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="24.12.2025",
        ex_tag="11.12.2025",
        value_10287="0,0120",
        value_10288="0,0030",
        value_10289="-0,0735",
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
    )

    payout_state_df = pl.read_csv(output_paths["payout_state"])
    resolution_df = pl.read_csv(output_paths["payout_resolution_events"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    income_df = pl.read_csv(output_paths["income_events"])
    broker_row = income_df.filter(pl.col("event_type") == "broker_dividend_event").to_dicts()[0]
    deemed_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]

    assert payout_state_df["status"].to_list() == ["resolved_same_year_distribution"]
    assert resolution_df["resolution_mode"].to_list() == ["matched_same_year_distribution"]
    assert basis_df["basis_stepup_total_eur"].to_list() == [-0.735]
    assert broker_row["broker_tax_amount_ccy"] == 0.11
    assert broker_row["creditable_foreign_tax_total_ccy"] == 0.0
    assert deemed_row["event_date"] == "2025-12-24"
    assert deemed_row["eligibility_date"] == "2025-12-11"
    assert deemed_row["amount_total_ccy"] == 0.12
    assert credit_row["event_date"] == "2025-12-24"
    assert credit_row["eligibility_date"] == "2025-12-11"
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.03

    second_output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output_rerun",
        strict_unresolved_payouts=False,
    )
    rerun_payout_state_df = pl.read_csv(second_output_paths["payout_state"])
    rerun_notes = rerun_payout_state_df["notes"].to_list()[0]

    assert rerun_notes.count("resolved by same-year Ausschüttungsmeldung") == 1
    assert rerun_notes.count("matched by actionID-aware broker payout event") == 1


def test_reporting_funds_workflow_fails_when_same_year_payout_remains_unresolved(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-06-01", "USD", 1.0),
            ("2025-04-02", "USD", 1.0),
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
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="14",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2025-04-02",
                ex_date="2025-03-20",
                amount="4.49",
                action_id="vusd-2025",
                report_date="2025-04-03",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-03-20",
                effective_date="2025-03-19",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="14",
                code="Po",
                action_id="vusd-2025",
                gross_rate="0.32063",
                gross_amount="4.49",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-04-03",
                effective_date="2025-04-02",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="14",
                code="Re",
                action_id="vusd-2025",
                gross_rate="0.32063",
                gross_amount="-4.49",
            ),
        ],
    )
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B3XXRP09",
        meldedatum="27.10.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        meldezeitraum_beginn="01.01.2025",
        meldezeitraum_ende="31.12.2025",
    )

    with pytest.raises(ValueError, match="Unresolved ETF broker payouts remain"):
        run_workflow(
            person="eugene",
            tax_year=2025,
            ibkr_tax_xml_path=tax_xml_path,
            ibkr_trade_history_path=trade_history_path,
            raw_exchange_rates_path=rates_path,
            oekb_root_dir=oekb_root,
            state_dir=tmp_path / "state",
            output_dir=tmp_path / "output",
        )


def test_reporting_funds_workflow_applies_historical_negative_deemed_distributed_income_as_annual_cleanup(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-06-01", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="14",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(tax_xml_path, cash_rows=[], accrual_rows=[])
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B3XXRP09",
        meldedatum="15.01.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        geschaeftsjahres_beginn="01.07.2023",
        geschaeftsjahres_ende="30.06.2024",
        value_10287="-0,0704",
        value_10288="0,0100",
        value_10289="-0,0500",
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
    )

    review_df = pl.read_csv(output_paths["negative_deemed_distribution_review"])
    assert review_df.height == 1
    assert review_df["status"].to_list() == ["applied_historical_annual_cleanup"]

    income_df = pl.read_csv(output_paths["income_events"])
    negative_age_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]
    basis_df = pl.read_csv(output_paths["basis_adjustments"])

    assert negative_age_row["amount_total_ccy"] == -0.9856
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.14
    assert basis_df["basis_stepup_total_ccy"].to_list() == [-0.7]


def test_reporting_funds_workflow_writes_negative_deemed_distribution_review_and_blocks_without_override(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="100",
                transaction_id="buy-1",
            )
        ],
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
        geschaeftsjahres_beginn="01.01.2025",
        geschaeftsjahres_ende="30.06.2025",
        value_10287="-0,1000",
        value_10289="-0,2000",
        value_10288="0,0100",
    )

    with pytest.raises(ValueError, match="Negative deemed distributed income requires manual review"):
        run_workflow(
            person="eugene",
            tax_year=2025,
            ibkr_tax_xml_path=tax_xml_path,
            ibkr_trade_history_path=trade_history_path,
            raw_exchange_rates_path=rates_path,
            oekb_root_dir=oekb_root,
            state_dir=tmp_path / "state",
            output_dir=tmp_path / "output",
        )

    review_df = pl.read_csv(tmp_path / "output" / "fund_tax_negative_deemed_distribution_review_2025.csv")
    assert review_df.height == 1
    assert review_df["status"].to_list() == ["unresolved_block"]


def test_reporting_funds_workflow_applies_negative_deemed_distribution_override(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="100",
                transaction_id="buy-1",
            )
        ],
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
        geschaeftsjahres_beginn="01.01.2025",
        geschaeftsjahres_ende="30.06.2025",
        value_10287="-0,1000",
        value_10289="-0,2000",
        value_10288="0,0100",
    )
    overrides_path = tmp_path / "state" / "fund_tax_negative_deemed_distribution_overrides.csv"
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    _write_negative_override_csv(
        overrides_path,
        [("IE00B3XXRP09:2025-10-27", "apply_full", "10", "use full reviewed quantity")],
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
    )

    income_df = pl.read_csv(output_paths["income_events"])
    negative_age_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    assert negative_age_row["amount_total_ccy"] == -1.0
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.1

    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    assert basis_df["basis_stepup_total_ccy"].to_list() == [-2.0]

    review_df = pl.read_csv(output_paths["negative_deemed_distribution_review"])
    assert review_df["status"].to_list() == ["applied_full"]


def test_reporting_funds_workflow_resolves_2025_payout_from_2026_annual_10595(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-06-01", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
            ("2025-12-31", "USD", 1.0),
            ("2026-01-16", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="14",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2025-12-31",
                ex_date="2025-12-18",
                amount="4.19",
                action_id="vusd-cross-year",
                report_date="2026-01-02",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-12-18",
                effective_date="2025-12-17",
                ex_date="2025-12-18",
                pay_date="2025-12-31",
                quantity="14",
                code="Po",
                action_id="vusd-cross-year",
                gross_rate="0.299286",
                gross_amount="4.19",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2026-01-02",
                effective_date="2025-12-31",
                ex_date="2025-12-18",
                pay_date="2025-12-31",
                quantity="14",
                code="Re",
                action_id="vusd-cross-year",
                gross_rate="0.299286",
                gross_amount="-4.19",
            ),
        ],
    )
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual_2025.csv",
        isin="IE00B3XXRP09",
        meldedatum="15.01.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        meldezeitraum_beginn="01.01.2024",
        meldezeitraum_ende="31.12.2024",
    )
    _write_oekb_file(
        oekb_root / "2026" / "annual_2026.csv",
        isin="IE00B3XXRP09",
        meldedatum="16.01.2026",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        meldezeitraum_beginn="01.01.2025",
        meldezeitraum_ende="31.12.2025",
        value_10595="0,299286",
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
        resolution_cutoff_date="2026-04-30",
    )

    income_df = pl.read_csv(output_paths["income_events"]).filter(pl.col("event_type") == "oekb_non_reported_distribution_10595")
    payout_state_df = pl.read_csv(output_paths["payout_state"])

    assert income_df["event_date"].to_list() == ["2025-12-31"]
    assert income_df["eligibility_date"].to_list() == ["2025-12-18"]
    assert payout_state_df["status"].to_list() == ["resolved_later_year_annual_report"]


def test_reporting_funds_workflow_keeps_broker_cash_payout_when_annual_period_does_not_cover_pay_date(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
            ("2025-04-01", "USD", 1.0),
            ("2025-10-27", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="2.5",
                price="100",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                settle_date="2025-04-01",
                ex_date="2025-03-24",
                amount="4.10",
                action_id="spy5-action",
                report_date="2025-04-02",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                report_date="2025-03-24",
                effective_date="2025-03-21",
                ex_date="2025-03-24",
                pay_date="2025-04-01",
                quantity="2.5",
                code="Po",
                action_id="spy5-action",
                gross_rate="1.6414",
                gross_amount="4.10",
            ),
            _accrual_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                report_date="2025-04-02",
                effective_date="2025-04-01",
                ex_date="2025-03-24",
                pay_date="2025-04-01",
                quantity="2.5",
                code="Re",
                action_id="spy5-action",
                gross_rate="1.6414",
                gross_amount="-4.10",
            ),
        ],
    )
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B6YX5C33",
        meldedatum="27.10.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        geschaeftsjahres_beginn="01.04.2024",
        geschaeftsjahres_ende="31.03.2025",
        value_10287="11,2278",
        value_10288="0,9271",
        value_10595="6,3730",
        value_10289="10,1944",
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
    )

    income_df = pl.read_csv(output_paths["income_events"])
    payout_state_df = pl.read_csv(output_paths["payout_state"])
    resolution_df = pl.read_csv(output_paths["payout_resolution_events"])
    summary_text = Path(output_paths["summary"]).read_text(encoding="utf-8")

    assert income_df.filter(pl.col("event_type") == "oekb_non_reported_distribution_10595").height == 0
    assert income_df.filter(pl.col("event_type") == "broker_dividend_event")["amount_total_ccy"].to_list() == [4.1]
    assert payout_state_df["status"].to_list() == ["resolved_broker_cash_outside_oekb_period"]
    assert resolution_df["resolution_mode"].to_list() == ["broker_cash_outside_oekb_period"]
    assert "`ETF distributions 27.5%`: `32.169500 EUR`" in summary_text
    assert "`Creditable foreign tax`: `2.317750 EUR`" in summary_text
    assert "`diagnostic_total_income_eur`: `32.169500 EUR`" in summary_text
    assert "open lots `1`, open quantity `2.5`, original basis `250.000000 EUR`" in summary_text
    assert "OeKB basis adjustment `25.486000 EUR`, adjusted basis `275.486000 EUR`" in summary_text
    assert "## Next Reporting Period Inputs" in summary_text
    assert "`" + str((tmp_path / "state" / "fund_tax_ledger_2025_final.csv").as_posix()) + "`" in summary_text
    assert "`" + str((tmp_path / "state" / "fund_tax_payout_state.csv").as_posix()) + "`" in summary_text
