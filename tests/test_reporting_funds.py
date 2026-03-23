from __future__ import annotations

from decimal import Decimal
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
from scripts.reporting_funds.workflow import basis_adjustments_to_df, load_opening_state_snapshot, run_workflow


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
    value_10759: str = "0,0000",
    value_10760: str = "0,0000",
    value_10047: str = "",
    value_10055: str = "",
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
                f"Inländische Dividenden, die in den Verlustausgleich gemäß § 27 Abs. 8 EStG einbezogen werden können (Kennzahl 189);{value_10759};{value_10759};x;10759",
                f"KESt auf inländische Dividenden, die im Rahmen des Verlustausgleichs gemäß § 27 Abs. 8 EStG berücksichtigt werden kann (Kennzahl 899);{value_10760};{value_10760};x;10760",
                *(
                    [f"Gesamtausschüttungen;{value_10047};{value_10047};x;10047"]
                    if value_10047
                    else []
                ),
                *(
                    [f"Basis Ausschüttungskomponente;{value_10055};{value_10055};x;10055"]
                    if value_10055
                    else []
                ),
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


def _write_opening_lots_csv(path: Path, rows: list[dict[str, object]]) -> None:
    header = (
        "snapshot_date,broker,ticker,isin,currency,asset_class,quantity,base_cost_total_eur,"
        "basis_adjustment_total_eur,total_basis_eur,average_basis_eur,status,last_event_date,basis_method,"
        "notes,source_file\n"
    )
    body = "".join(
        ",".join(
            [
                str(row["snapshot_date"]),
                str(row.get("broker", "ibkr")),
                str(row["ticker"]),
                str(row["isin"]),
                str(row["currency"]),
                str(row.get("asset_class", "ETF")),
                str(row.get("quantity", row.get("remaining_quantity", row.get("original_quantity", 0.0)))),
                str(row.get("base_cost_total_eur", row.get("original_cost_eur", 0.0))),
                str(row.get("basis_adjustment_total_eur", row.get("cumulative_oekb_stepup_eur", 0.0))),
                str(
                    row.get(
                        "total_basis_eur",
                        row.get("adjusted_basis_eur")
                        if "adjusted_basis_eur" in row
                        else row.get("base_cost_total_eur", row.get("original_cost_eur", 0.0))
                        + row.get("basis_adjustment_total_eur", row.get("cumulative_oekb_stepup_eur", 0.0)),
                    )
                ),
                str(row.get("average_basis_eur", 0.0)),
                str(row.get("status", "open")),
                str(row.get("last_event_date", row["snapshot_date"])),
                str(row.get("basis_method", row.get("austrian_basis_method", "move_in_fmv_reset"))),
                str(row.get("notes", "")),
                str(row.get("source_file", row.get("source_statement_file", ""))),
            ]
        )
        + "\n"
        for row in rows
    )
    path.write_text(header + body, encoding="utf-8")


def test_load_opening_state_snapshot_filters_to_requested_asset_class(tmp_path: Path) -> None:
    opening_path = tmp_path / "opening_state.csv"
    _write_opening_lots_csv(
        opening_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "currency": "USD",
                "asset_class": "COMMON",
                "quantity": 10.0,
                "base_cost_total_eur": 1000.0,
            },
            {
                "snapshot_date": "2024-05-01",
                "ticker": "VWRL",
                "isin": "IE00B3RBWM25",
                "currency": "USD",
                "asset_class": "ETF",
                "quantity": 5.0,
                "base_cost_total_eur": 500.0,
            },
        ],
    )

    states, snapshot_date = load_opening_state_snapshot(opening_path, allowed_asset_classes={"ETF"})

    assert snapshot_date == date(2024, 5, 1)
    assert [(state.ticker, state.asset_class, state.quantity) for state in states] == [("VWRL", "ETF", 5.0)]


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
    currency: str = "USD",
) -> str:
    return (
        f"<Trade accountId=\"U1\" symbol=\"{ticker}\" isin=\"{isin}\" subCategory=\"ETF\" currency=\"{currency}\" "
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


def test_load_oekb_report_parses_distribution_and_annual_fields(tmp_path: Path) -> None:
    idtl_path = tmp_path / "IDTL_Ausschuettungsmeldung_22.12.2025.csv"
    spy5_path = tmp_path / "SPY5_Jahresdatenmeldung_27.10.2025.csv"
    _write_oekb_file(
        idtl_path,
        isin="IE00BSKRJZ44",
        meldedatum="22.12.2025",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="24.12.2025",
        ex_tag="11.12.2025",
        value_10289="-0,0735",
        value_10759="0,0016",
        value_10760="0,0004",
    )
    _write_oekb_file(
        spy5_path,
        isin="IE00B6YX5C33",
        meldedatum="27.10.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        value_10595="6,3730",
        value_10288="0,9271",
    )
    idtl = load_oekb_report(idtl_path, tax_year=2025)
    spy5 = load_oekb_report(spy5_path, tax_year=2025)

    assert idtl.is_ausschuettungsmeldung is True
    assert idtl.ex_tag == date(2025, 12, 11)
    assert idtl.ausschuettungstag == date(2025, 12, 24)
    assert idtl.acquisition_cost_correction_per_share_ccy == Decimal("-0.073500")
    assert idtl.domestic_dividends_loss_offset_per_share_ccy == Decimal("0.001600")
    assert idtl.domestic_dividend_kest_per_share_ccy == Decimal("0.000400")

    assert spy5.is_jahresmeldung is True
    assert spy5.non_reported_distribution_per_share_ccy == Decimal("6.373000")
    assert spy5.creditable_foreign_tax_per_share_ccy == Decimal("0.927100")
    assert spy5.domestic_dividends_loss_offset_per_share_ccy == Decimal("0")
    assert spy5.domestic_dividend_kest_per_share_ccy == Decimal("0")


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
                "Inländische Dividenden, die in den Verlustausgleich gemäß § 27 Abs. 8 EStG einbezogen werden können (Kennzahl 189);0,1234;0,1234;x;10759",
                "KESt auf inländische Dividenden, die im Rahmen des Verlustausgleichs gemäß § 27 Abs. 8 EStG berücksichtigt werden kann (Kennzahl 899);0,0123;0,0123;x;10760",
                "",
                "Ertragsteuerliche Behandlung (je Anteil)",
                "======================================",
                "POSITION;BEZEICHNUNG;PA_MIT_OPTION;PA_OHNE_OPTION;BV_MIT_OPTION;BV_OHNE_OPTION;BV_JUR_PERSON;STIFTUNG;STEUERNAME;STEUERCODE",
                "16.1;Ausschüttungen 27,5%;999,0000;999,0000;;;;;x;10286",
                "16.2;Ausschüttungsgleiche Erträge 27,5%;999,0000;999,0000;;;;;x;10287",
                "16.2.1;Nicht gemeldete Ausschüttungen;999,0000;999,0000;;;;;x;10595",
                "16.3;Anzurechnende ausländische Quellensteuer;999,0000;999,0000;;;;;x;10288",
                "16.4;Anschaffungskostenkorrektur;999,0000;999,0000;;;;;x;10289",
                "16.5;Inländische Dividenden;999,0000;999,0000;;;;;x;10759",
                "16.6;KESt auf inländische Dividenden;999,0000;999,0000;;;;;x;10760",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = load_oekb_report(path, tax_year=2025, ticker_by_isin={"IE00B6YX5C33": "SPY5"})

    assert report.ticker == "SPY5"
    assert report.reported_distribution_per_share_ccy == 0.0
    assert report.age_per_share_ccy == Decimal("11.227800")
    assert report.non_reported_distribution_per_share_ccy == Decimal("6.373000")
    assert report.creditable_foreign_tax_per_share_ccy == Decimal("0.927100")
    assert report.acquisition_cost_correction_per_share_ccy == Decimal("10.194400")
    assert report.domestic_dividends_loss_offset_per_share_ccy == Decimal("0.123400")
    assert report.domestic_dividend_kest_per_share_ccy == Decimal("0.012300")


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
    assert events[0].gross_amount == Decimal("40.430000")
    assert events[0].evidence_state == "confirmed_cash"
    assert "symbol drift ignored" in events[0].matching_notes


def test_build_broker_dividend_events_marks_po_only_and_re_without_cash_as_deferred_evidence(tmp_path: Path) -> None:
    tax_xml = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml,
        cash_rows=[],
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
                action_id="po-only",
                gross_rate="0.299086",
                gross_amount="4.19",
            ),
            _accrual_row(
                ticker="SPY5",
                isin="IE00B6YX5C33",
                report_date="2025-12-31",
                effective_date="2025-12-31",
                ex_date="2025-12-22",
                pay_date="2025-12-31",
                quantity="8",
                code="Re",
                action_id="re-only",
                gross_rate="1.6626",
                gross_amount="-13.30",
            ),
        ],
    )

    events = build_broker_dividend_events(
        load_ibkr_etf_dividend_accrual_rows(str(tax_xml)),
        load_ibkr_etf_cash_dividend_rows(str(tax_xml)),
        tax_year=2025,
    )

    assert [event.evidence_state for event in events] == [
        "accrual_realized_cash_missing",
        "accrual_pre_payout_only",
    ]


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
    assert historical_row["status"].to_list() == ["ignored_prior_year_reference"]
    assert "ignored for current-year tax and basis" in historical_row["notes"].to_list()[0]


def test_reporting_funds_allows_eur_trade_currency_with_usd_oekb_report(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-12-20", "USD", 1.0),
            ("2025-03-20", "USD", 1.0),
            ("2025-04-02", "USD", 1.0),
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VWRL",
                isin="IE00B3RBWM25",
                trade_date="2024-12-20",
                date_time="2024-12-20 08:09:15",
                operation="BUY",
                quantity="3",
                price="130.56",
                transaction_id="buy-vwrl-1",
                currency="EUR",
            )
        ],
    )

    tax_dir = tmp_path / "tax"
    tax_dir.mkdir(parents=True, exist_ok=True)
    _write_tax_xml(
        tax_dir / "2025.xml",
        cash_rows=[
            _cash_dividend_row(
                ticker="VWRL",
                isin="IE00B3RBWM25",
                settle_date="2025-04-02",
                ex_date="2025-03-20",
                amount="1.39",
                action_id="vwrl-2025-q1",
                report_date="2025-04-03",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VWRL",
                isin="IE00B3RBWM25",
                report_date="2025-03-20",
                effective_date="2025-03-19",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="3",
                code="Po",
                action_id="vwrl-2025-q1",
                gross_rate="0.46446",
                gross_amount="1.39",
            ),
            _accrual_row(
                ticker="VWRL",
                isin="IE00B3RBWM25",
                report_date="2025-04-03",
                effective_date="2025-04-02",
                ex_date="2025-03-20",
                pay_date="2025-04-02",
                quantity="3",
                code="Re",
                action_id="vwrl-2025-q1",
                gross_rate="0.46446",
                gross_amount="-1.39",
            ),
        ],
    )

    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2025" / "vwrl_q1.csv",
        isin="IE00B3RBWM25",
        meldedatum="01.04.2025",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="02.04.2025",
        ex_tag="20.03.2025",
        value_10286="0,46446",
        currency="USD",
    )

    output_paths = run_workflow(
        person="oryna",
        tax_year=2025,
        ibkr_tax_xml_path=tax_dir,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        strict_unresolved_payouts=False,
    )

    income_df = pl.read_csv(output_paths["income_events"])
    state_df = pl.read_csv(output_paths["state"])

    assert income_df.filter(pl.col("ticker") == "VWRL").height > 0
    assert state_df.filter(pl.col("ticker") == "VWRL")["currency"].to_list() == ["EUR"]


def test_reporting_funds_carryforward_only_skips_pre_move_in_2024_activity(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-05-01", "USD", 1.0),
            ("2024-06-01", "USD", 1.0),
            ("2024-06-13", "USD", 1.0),
            ("2024-06-26", "USD", 1.0),
            ("2024-12-12", "USD", 1.0),
            ("2024-12-27", "USD", 1.0),
        ],
    )
    opening_state_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_state_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "ETF",
                "ticker": "VUSD",
                "isin": "IE00B3XXRP09",
                "lot_id": "open-1",
                "buy_date": "2024-05-01",
                "original_quantity": 10.0,
                "remaining_quantity": 10.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 1000.0,
                "adjusted_basis_eur": 1000.0,
                "source_trade_id": "seed",
                "source_statement_file": "seed.csv",
            }
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(
        trade_history_path,
        [
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-02-01",
                date_time="2024-02-01 10:00:00",
                operation="BUY",
                quantity="2",
                price="90",
                transaction_id="pre-move-buy",
            ),
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2024-06-01",
                date_time="2024-06-01 10:00:00",
                operation="BUY",
                quantity="4",
                price="110",
                transaction_id="post-move-buy",
            ),
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-03-27",
                ex_date="2024-03-14",
                amount="3.00",
                action_id="pre-move-payout",
                report_date="2024-03-28",
            ),
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-06-26",
                ex_date="2024-06-13",
                amount="3.63",
                action_id="post-move-payout",
                report_date="2024-06-27",
            ),
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-03-14",
                effective_date="2024-03-13",
                ex_date="2024-03-14",
                pay_date="2024-03-27",
                quantity="10",
                code="Po",
                action_id="pre-move-payout",
                gross_rate="0.3000",
                gross_amount="3.00",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-03-28",
                effective_date="2024-03-27",
                ex_date="2024-03-14",
                pay_date="2024-03-27",
                quantity="10",
                code="Re",
                action_id="pre-move-payout",
                gross_rate="0.3000",
                gross_amount="-3.00",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-13",
                effective_date="2024-06-12",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Po",
                action_id="post-move-payout",
                gross_rate="0.2593",
                gross_amount="3.63",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-27",
                effective_date="2024-06-26",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Re",
                action_id="post-move-payout",
                gross_rate="0.2593",
                gross_amount="-3.63",
            ),
        ],
    )
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2024" / "march.csv",
        isin="IE00B3XXRP09",
        meldedatum="25.03.2024",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="27.03.2024",
        ex_tag="14.03.2024",
        value_10286="0,3000",
        value_10288="0,0100",
        value_10289="-0,0500",
    )
    _write_oekb_file(
        oekb_root / "2024" / "june.csv",
        isin="IE00B3XXRP09",
        meldedatum="25.06.2024",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="26.06.2024",
        ex_tag="13.06.2024",
        value_10286="0,2593",
        value_10288="0,0303",
        value_10289="-0,0538",
        value_10759="0,0016",
        value_10760="0,0004",
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2024,
        ibkr_tax_xml_path=tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        opening_state_path=opening_state_path,
        authoritative_start_date=date(2024, 5, 1),
        carryforward_only=True,
    )

    state_df = pl.read_csv(output_paths["state"])
    income_df = pl.read_csv(output_paths["income_events"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    payout_df = pl.read_csv(output_paths["payout_state"])

    assert state_df["quantity"].sum() == 14.0
    assert "seed.csv" in state_df["source_file"].to_list()
    assert income_df.is_empty()
    assert basis_df["effective_date"].to_list() == ["2024-06-26"]
    assert payout_df["payout_key"].to_list() == ["post-move-payout"]


def test_reporting_funds_carryforward_only_applies_basis_without_distribution_match(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-05-01", "USD", 1.0),
            ("2024-06-13", "USD", 1.0),
            ("2024-06-26", "USD", 1.0),
        ],
    )
    opening_state_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_state_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "ETF",
                "ticker": "IDTL",
                "isin": "IE00BSKRJZ44",
                "lot_id": "open-1",
                "buy_date": "2024-05-01",
                "original_quantity": 10.0,
                "remaining_quantity": 10.0,
                "currency": "USD",
                "buy_price_ccy": 3.5,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 35.0,
                "adjusted_basis_eur": 35.0,
                "source_trade_id": "seed",
                "source_statement_file": "seed.csv",
            }
        ],
    )
    trade_history_path = tmp_path / "trade_history.xml"
    _write_trade_xml(trade_history_path, [])
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(tax_xml_path, cash_rows=[], accrual_rows=[])
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2024" / "idtl_june.csv",
        isin="IE00BSKRJZ44",
        meldedatum="25.06.2024",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="26.06.2024",
        ex_tag="13.06.2024",
        value_10286="0,0149",
        value_10289="-0,0609",
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2024,
        ibkr_tax_xml_path=tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        opening_state_path=opening_state_path,
        authoritative_start_date=date(2024, 5, 1),
        carryforward_only=True,
    )

    income_df = pl.read_csv(output_paths["income_events"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    state_df = pl.read_csv(output_paths["state"])

    assert income_df.is_empty()
    assert basis_df.is_empty()
    assert state_df["basis_adjustment_total_eur"].to_list() == [0.0]


def test_reporting_funds_2025_uses_2024_carryforward_ledger_once(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2024-05-01", "USD", 1.0),
            ("2024-06-01", "USD", 1.0),
            ("2024-06-13", "USD", 1.0),
            ("2024-06-26", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
        ],
    )
    opening_state_path = tmp_path / "opening.csv"
    _write_opening_lots_csv(
        opening_state_path,
        [
            {
                "snapshot_date": "2024-05-01",
                "asset_class": "ETF",
                "ticker": "VUSD",
                "isin": "IE00B3XXRP09",
                "lot_id": "open-1",
                "buy_date": "2024-05-01",
                "original_quantity": 10.0,
                "remaining_quantity": 10.0,
                "currency": "USD",
                "buy_price_ccy": 100.0,
                "buy_fx_to_eur": 1.0,
                "original_cost_eur": 1000.0,
                "adjusted_basis_eur": 1000.0,
                "source_trade_id": "seed",
                "source_statement_file": "seed.csv",
            }
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
                quantity="4",
                price="110",
                transaction_id="post-move-buy",
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
                settle_date="2024-06-26",
                ex_date="2024-06-13",
                amount="3.63",
                action_id="post-move-payout",
                report_date="2024-06-27",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-13",
                effective_date="2024-06-12",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Po",
                action_id="post-move-payout",
                gross_rate="0.2593",
                gross_amount="3.63",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-27",
                effective_date="2024-06-26",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Re",
                action_id="post-move-payout",
                gross_rate="0.2593",
                gross_amount="-3.63",
            ),
        ],
    )
    _write_tax_xml(tax_dir / "2025.xml", cash_rows=[], accrual_rows=[])
    oekb_root = tmp_path / "oekb"
    _write_oekb_file(
        oekb_root / "2024" / "june.csv",
        isin="IE00B3XXRP09",
        meldedatum="25.06.2024",
        jahresmeldung="NEIN",
        ausschuettungsmeldung="JA",
        ausschuettungstag="26.06.2024",
        ex_tag="13.06.2024",
        value_10286="0,2593",
        value_10288="0,0303",
        value_10289="-0,0538",
    )
    _write_oekb_file(
        oekb_root / "2025" / "annual.csv",
        isin="IE00B3XXRP09",
        meldedatum="15.01.2025",
        jahresmeldung="JA",
        ausschuettungsmeldung="NEIN",
        meldezeitraum_beginn="01.07.2024",
        meldezeitraum_ende="31.12.2024",
        value_10289="-0,1000",
    )

    state_dir = tmp_path / "state"
    run_workflow(
        person="eugene",
        tax_year=2024,
        ibkr_tax_xml_path=tax_dir / "2024.xml",
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=state_dir,
        output_dir=tmp_path / "output_2024",
        opening_state_path=opening_state_path,
        authoritative_start_date=date(2024, 5, 1),
        carryforward_only=True,
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=tax_dir,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=state_dir,
        output_dir=tmp_path / "output_2025",
    )

    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    state_df = pl.read_csv(output_paths["state"])

    assert basis_df["effective_date"].to_list() == ["2025-01-15"]
    assert basis_df["shares_held_on_eligibility_date"].to_list() == [14.0]
    assert state_df["quantity"].sum() == 14.0


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
        value_10759="0,0015",
        value_10760="0,0004",
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
    deemed_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]
    domestic_dividend_row = income_df.filter(pl.col("event_type") == "oekb_domestic_dividends_10759").to_dicts()[0]
    domestic_kest_row = income_df.filter(pl.col("event_type") == "oekb_domestic_dividend_kest_10760").to_dicts()[0]

    assert payout_state_df["status"].to_list() == ["resolved_same_year_distribution"]
    assert resolution_df["resolution_mode"].to_list() == ["matched_same_year_distribution"]
    assert basis_df["basis_stepup_total_eur"].to_list() == [-0.735]
    assert income_df.filter(pl.col("event_type") == "broker_dividend_event").is_empty()
    assert deemed_row["event_date"] == "2025-12-24"
    assert deemed_row["eligibility_date"] == "2025-12-11"
    assert deemed_row["amount_total_ccy"] == 0.12
    assert credit_row["event_date"] == "2025-12-24"
    assert credit_row["eligibility_date"] == "2025-12-11"
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.03
    assert domestic_dividend_row["event_date"] == "2025-12-24"
    assert domestic_dividend_row["eligibility_date"] == "2025-12-11"
    assert domestic_dividend_row["amount_total_ccy"] == 0.015
    assert domestic_kest_row["event_date"] == "2025-12-24"
    assert domestic_kest_row["eligibility_date"] == "2025-12-11"
    assert domestic_kest_row["domestic_dividend_kest_total_ccy"] == 0.004
    assert domestic_kest_row["creditable_foreign_tax_total_ccy"] == 0.0

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


def test_reporting_funds_sale_after_ex_date_realizes_oekb_basis_adjustment(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
            ("2025-12-11", "USD", 1.0),
            ("2025-12-18", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="100",
                transaction_id="buy-1",
            ),
            _trade_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                trade_date="2025-12-18",
                date_time="2025-12-18 10:00:00",
                operation="SELL",
                quantity="-10",
                price="101",
                transaction_id="sell-1",
            ),
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
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Po",
                action_id="idtl-1",
                gross_rate="0.0735",
                gross_amount="0.735",
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

    sales_df = pl.read_csv(output_paths["sales"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    state_df = pl.read_csv(output_paths["state"])

    assert basis_df["shares_held_on_eligibility_date"].to_list() == [10.0]
    assert basis_df["basis_stepup_total_eur"].to_list() == [-0.735]
    assert sales_df["sale_date"].to_list() == ["2025-12-18"]
    assert sales_df["realized_base_cost_eur"].to_list() == [1000.0]
    assert sales_df["realized_oekb_adjustment_eur"].to_list() == [-0.735]
    assert sales_df["taxable_total_basis_eur"].to_list() == [999.265]
    assert sales_df["taxable_gain_loss_eur"].to_list() == [10.735]
    assert state_df["quantity"].to_list() == [0.0]
    assert state_df["total_basis_eur"].to_list() == [0.0]


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


def test_reporting_funds_workflow_auto_applies_negative_deemed_distributed_income_with_single_linked_payout(tmp_path: Path) -> None:
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
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-06-26",
                ex_date="2024-06-13",
                amount="7.66",
                action_id="linked-payout",
                report_date="2024-06-27",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-13",
                effective_date="2024-06-12",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Po",
                action_id="linked-payout",
                gross_rate="0.5471",
                gross_amount="7.66",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-27",
                effective_date="2024-06-26",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Re",
                action_id="linked-payout",
                gross_rate="0.5471",
                gross_amount="-7.66",
            ),
        ],
    )
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
    assert review_df["status"].to_list() == ["applied_auto_reconciled_payout_set"]
    assert review_df["candidate_payout_count"].to_list() == [1]
    assert review_df["matched_payout_count"].to_list() == [1]

    income_df = pl.read_csv(output_paths["income_events"])
    negative_age_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]
    basis_df = pl.read_csv(output_paths["basis_adjustments"])

    assert negative_age_row["amount_total_ccy"] == -0.9856
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.14
    assert basis_df["basis_stepup_total_ccy"].to_list() == [-0.7]


def test_reporting_funds_workflow_uses_lookup_only_historical_ibkr_tax_xml_for_negative_deemed_validation(tmp_path: Path) -> None:
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
    main_tax_xml_path = tmp_path / "main_tax.xml"
    _write_tax_xml(main_tax_xml_path, cash_rows=[], accrual_rows=[])
    historical_tax_xml_path = tmp_path / "historical_tax.xml"
    _write_tax_xml(
        historical_tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-06-26",
                ex_date="2024-06-13",
                amount="7.66",
                action_id="linked-payout",
                report_date="2024-06-27",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-13",
                effective_date="2024-06-12",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Po",
                action_id="linked-payout",
                gross_rate="0.5471",
                gross_amount="7.66",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-06-27",
                effective_date="2024-06-26",
                ex_date="2024-06-13",
                pay_date="2024-06-26",
                quantity="14",
                code="Re",
                action_id="linked-payout",
                gross_rate="0.5471",
                gross_amount="-7.66",
            ),
        ],
    )
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
        ibkr_tax_xml_path=main_tax_xml_path,
        historical_ibkr_tax_xml_path=historical_tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        strict_unresolved_payouts=False,
    )

    review_df = pl.read_csv(output_paths["negative_deemed_distribution_review"])
    income_df = pl.read_csv(output_paths["income_events"])
    payout_df = pl.read_csv(output_paths["payout_state"])

    assert review_df["status"].to_list() == ["applied_auto_reconciled_payout_set"]
    assert income_df.filter(pl.col("event_type") == "broker_dividend_event").is_empty()
    assert payout_df.is_empty()


def test_reporting_funds_workflow_reconciles_negative_deemed_distribution_to_unique_multi_payout_subset(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2023-09-01", "USD", 1.0),
            ("2023-09-27", "USD", 1.0),
            ("2023-10-15", "USD", 1.0),
            ("2023-12-27", "USD", 1.0),
            ("2024-03-27", "USD", 1.0),
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
                trade_date="2023-09-01",
                date_time="2023-09-01 10:00:00",
                operation="BUY",
                quantity="4",
                price="100",
                transaction_id="buy-1",
            ),
            _trade_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                trade_date="2023-10-15",
                date_time="2023-10-15 10:00:00",
                operation="BUY",
                quantity="10",
                price="101",
                transaction_id="buy-2",
            ),
        ],
    )
    main_tax_xml_path = tmp_path / "main_tax.xml"
    _write_tax_xml(main_tax_xml_path, cash_rows=[], accrual_rows=[])
    historical_tax_xml_path = tmp_path / "historical_tax.xml"
    _write_tax_xml(
        historical_tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2023-09-27",
                ex_date="2023-09-14",
                amount="1.07",
                action_id="payout-1",
                report_date="2023-09-27",
            ),
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2023-12-27",
                ex_date="2023-12-14",
                amount="3.91",
                action_id="payout-2",
                report_date="2023-12-27",
            ),
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2024-03-27",
                ex_date="2024-03-14",
                amount="4.23",
                action_id="payout-3",
                report_date="2024-03-27",
            ),
        ],
        accrual_rows=[
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2023-09-27",
                effective_date="2023-09-26",
                ex_date="2023-09-14",
                pay_date="2023-09-27",
                quantity="4",
                code="Po",
                action_id="payout-1",
                gross_rate="0.267572",
                gross_amount="1.07",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2023-09-27",
                effective_date="2023-09-27",
                ex_date="2023-09-14",
                pay_date="2023-09-27",
                quantity="4",
                code="Re",
                action_id="payout-1",
                gross_rate="0.267572",
                gross_amount="-1.07",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2023-12-27",
                effective_date="2023-12-26",
                ex_date="2023-12-14",
                pay_date="2023-12-27",
                quantity="14",
                code="Po",
                action_id="payout-2",
                gross_rate="0.279226",
                gross_amount="3.91",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2023-12-27",
                effective_date="2023-12-27",
                ex_date="2023-12-14",
                pay_date="2023-12-27",
                quantity="14",
                code="Re",
                action_id="payout-2",
                gross_rate="0.279226",
                gross_amount="-3.91",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-03-27",
                effective_date="2024-03-26",
                ex_date="2024-03-14",
                pay_date="2024-03-27",
                quantity="14",
                code="Po",
                action_id="payout-3",
                gross_rate="0.302416",
                gross_amount="4.23",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2024-03-27",
                effective_date="2024-03-27",
                ex_date="2024-03-14",
                pay_date="2024-03-27",
                quantity="14",
                code="Re",
                action_id="payout-3",
                gross_rate="0.302416",
                gross_amount="-4.23",
            ),
        ],
    )
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
        value_10595="0,5468",
        value_10288="0,0671",
        value_10289="-0,1646",
        value_10047="0,5468",
        value_10055="0,5468",
    )

    output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=main_tax_xml_path,
        historical_ibkr_tax_xml_path=historical_tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=tmp_path / "state",
        output_dir=tmp_path / "output",
        strict_unresolved_payouts=False,
    )

    review_df = pl.read_csv(output_paths["negative_deemed_distribution_review"])
    income_df = pl.read_csv(output_paths["income_events"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])

    assert review_df["status"].to_list() == ["applied_auto_reconciled_payout_set"]
    assert review_df["candidate_payout_count"].to_list() == [3]
    assert review_df["matched_payout_count"].to_list() == [2]
    assert review_df["matched_payout_dates"].to_list() == ["2023-09-27|2023-12-27"]
    assert review_df["eligible_quantity_used"].to_list() == [4.0]
    assert review_df["target_distribution_per_share_ccy"].to_list() == [0.5468]

    negative_age_row = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287").to_dicts()[0]
    credit_row = income_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288").to_dicts()[0]

    assert negative_age_row["quantity"] == 4.0
    assert negative_age_row["amount_total_ccy"] == -0.2816
    assert credit_row["creditable_foreign_tax_total_ccy"] == 0.2684
    assert basis_df["basis_stepup_total_ccy"].to_list() == [-0.6584]


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


def test_reporting_funds_workflow_uses_confirmed_cash_events_directly_for_annual_10595(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
            ("2025-03-20", "USD", 1.0),
            ("2025-04-02", "USD", 1.0),
            ("2025-06-19", "USD", 1.0),
            ("2025-07-02", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
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
                action_id="vusd-q1",
                report_date="2025-04-03",
            ),
            _cash_dividend_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                settle_date="2025-07-02",
                ex_date="2025-06-19",
                amount="4.38",
                action_id="vusd-q2",
                report_date="2025-07-03",
            ),
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
                action_id="vusd-q1",
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
                action_id="vusd-q1",
                gross_rate="0.32063",
                gross_amount="-4.49",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-06-19",
                effective_date="2025-06-18",
                ex_date="2025-06-19",
                pay_date="2025-07-02",
                quantity="14",
                code="Po",
                action_id="vusd-q2",
                gross_rate="0.31292",
                gross_amount="4.38",
            ),
            _accrual_row(
                ticker="VUSD",
                isin="IE00B3XXRP09",
                report_date="2025-07-03",
                effective_date="2025-07-02",
                ex_date="2025-06-19",
                pay_date="2025-07-02",
                quantity="14",
                code="Re",
                action_id="vusd-q2",
                gross_rate="0.31292",
                gross_amount="-4.38",
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
        value_10595="0,63355",
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

    assert income_df["event_date"].to_list() == ["2025-04-02", "2025-07-02"]
    assert income_df["amount_total_ccy"].to_list() == [4.49, 4.38]
    assert income_df["quantity"].to_list() == [14.0, 14.0]


def test_reporting_funds_workflow_does_not_emit_10595_for_accrual_only_payouts(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
            ("2025-01-15", "USD", 1.0),
            ("2025-12-18", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
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
        cash_rows=[],
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
                action_id="vusd-accrual-only",
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
                action_id="vusd-accrual-only",
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

    income_df = pl.read_csv(output_paths["income_events"])
    payout_df = pl.read_csv(output_paths["payout_state"])

    assert income_df.filter(pl.col("event_type") == "oekb_non_reported_distribution_10595").is_empty()
    assert payout_df["status"].to_list() == ["accrual_realized_cash_missing"]


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
        value_10759="0,5000",
        value_10760="0,2000",
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
    filing_distribution_total_eur = income_df.filter(
        pl.col("event_type").is_in(["oekb_reported_distribution_10286", "oekb_non_reported_distribution_10595"])
        | (
            (pl.col("event_type") == "broker_dividend_event")
            & pl.col("matched_broker_event_id").cast(pl.String).is_in(
                payout_state_df.filter(pl.col("resolution_mode") == "broker_cash_outside_oekb_period")["payout_key"]
                .cast(pl.String)
                .to_list()
            )
        )
    )["amount_total_eur"].sum()
    filing_deemed_total_eur = income_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287")[
        "amount_total_eur"
    ].sum()
    assert f"`ETF distributions 27.5%`: `{filing_distribution_total_eur:.6f} EUR`" in summary_text
    assert f"`Ausschüttungsgleiche Erträge 27.5%`: `{filing_deemed_total_eur:.6f} EUR`" in summary_text
    assert "`Domestic dividends in loss offset (KZ 189)`: `1.250000 EUR`" in summary_text
    assert "`Austrian KESt on domestic dividends (KZ 899)`: `0.500000 EUR`" in summary_text
    assert "`Creditable foreign tax`: `2.317750 EUR`" in summary_text
    assert "`diagnostic_total_income_eur`: `33.419500 EUR`" in summary_text
    assert "`diagnostic_total_domestic_dividend_kest_eur`: `0.500000 EUR`" in summary_text
    assert "open positions `1`, open quantity `2.5`, base cost `250.000000 EUR`" in summary_text
    assert "OeKB basis adjustment `25.486000 EUR`, total basis `275.486000 EUR`" in summary_text
    assert "## Next Reporting Period Inputs" in summary_text
    assert "`" + str((tmp_path / "state" / "fund_tax_state_2025_final.csv").as_posix()) + "`" in summary_text
    assert "`" + str((tmp_path / "state" / "fund_tax_payout_state.csv").as_posix()) + "`" in summary_text
    assert income_df.filter(pl.col("event_type") == "oekb_domestic_dividends_10759")["amount_total_eur"].sum() == 1.25
    assert income_df.filter(pl.col("event_type") == "oekb_domestic_dividend_kest_10760")[
        "domestic_dividend_kest_total_eur"
    ].sum() == 0.5
    assert income_df["creditable_foreign_tax_total_eur"].sum() == 2.31775


def test_reporting_funds_workflow_defers_distribution_report_without_confirmed_cash(tmp_path: Path) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="3.5",
                transaction_id="buy-1",
            )
        ],
    )
    tax_xml_path = tmp_path / "tax.xml"
    _write_tax_xml(
        tax_xml_path,
        cash_rows=[],
        accrual_rows=[
            _accrual_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Po",
                action_id="idtl-deferred",
                gross_rate="0.0735",
                gross_amount="0.735",
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
                action_id="idtl-deferred",
                gross_rate="0.0735",
                gross_amount="-0.735",
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
        value_10286="0,0100",
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
    )

    income_df = pl.read_csv(output_paths["income_events"])
    basis_df = pl.read_csv(output_paths["basis_adjustments"])
    payout_df = pl.read_csv(output_paths["payout_state"])
    evidence_review_df = pl.read_csv(output_paths["payout_evidence_review"])

    assert income_df.is_empty()
    assert basis_df.is_empty()
    assert payout_df["status"].to_list() == ["accrual_realized_cash_missing"]
    assert payout_df["evidence_state"].to_list() == ["accrual_realized_cash_missing"]
    assert evidence_review_df["status"].to_list() == ["accrual_realized_cash_missing"]


def test_reporting_funds_workflow_clears_stale_resolved_status_when_rerun_has_only_pending_accrual_evidence(
    tmp_path: Path,
) -> None:
    rates_path = tmp_path / "rates.csv"
    _write_rates_csv(
        rates_path,
        [
            ("2025-01-01", "USD", 1.0),
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
                trade_date="2025-01-01",
                date_time="2025-01-01 10:00:00",
                operation="BUY",
                quantity="10",
                price="3.5",
                transaction_id="buy-1",
            )
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
        value_10286="0,0100",
        value_10288="0,0030",
        value_10289="-0,0735",
    )

    first_tax_xml_path = tmp_path / "first_tax.xml"
    _write_tax_xml(
        first_tax_xml_path,
        cash_rows=[
            _cash_dividend_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                settle_date="2025-12-24",
                ex_date="2025-12-11",
                amount="0.735",
                action_id="idtl-stale",
                report_date="2025-12-24",
            )
        ],
        accrual_rows=[
            _accrual_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Po",
                action_id="idtl-stale",
                gross_rate="0.0735",
                gross_amount="0.735",
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
                action_id="idtl-stale",
                gross_rate="0.0735",
                gross_amount="-0.735",
            ),
        ],
    )

    state_dir = tmp_path / "state"
    output_dir = tmp_path / "output-first"
    run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=first_tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=state_dir,
        output_dir=output_dir,
    )

    second_tax_xml_path = tmp_path / "second_tax.xml"
    _write_tax_xml(
        second_tax_xml_path,
        cash_rows=[],
        accrual_rows=[
            _accrual_row(
                ticker="IDTL",
                isin="IE00BSKRJZ44",
                report_date="2025-12-11",
                effective_date="2025-12-10",
                ex_date="2025-12-11",
                pay_date="2025-12-24",
                quantity="10",
                code="Po",
                action_id="idtl-stale",
                gross_rate="0.0735",
                gross_amount="0.735",
            )
        ],
    )

    rerun_output_paths = run_workflow(
        person="eugene",
        tax_year=2025,
        ibkr_tax_xml_path=second_tax_xml_path,
        ibkr_trade_history_path=trade_history_path,
        raw_exchange_rates_path=rates_path,
        oekb_root_dir=oekb_root,
        state_dir=state_dir,
        output_dir=tmp_path / "output-second",
    )

    payout_df = pl.read_csv(rerun_output_paths["payout_state"])
    evidence_review_df = pl.read_csv(rerun_output_paths["payout_evidence_review"])

    assert payout_df["status"].to_list() == ["accrual_pre_payout_only"]
    assert payout_df["evidence_state"].to_list() == ["accrual_pre_payout_only"]
    assert payout_df["resolved_tax_year"].to_list() == [""]
    assert payout_df["resolved_by_report_year"].to_list() == [""]
    assert payout_df["resolved_by_report_file"].to_list() == [""]
    assert payout_df["resolution_mode"].to_list() == [""]
    assert payout_df["notes"].to_list() == ["cash_row_missing"]
    assert evidence_review_df["status"].to_list() == ["accrual_pre_payout_only"]
    assert evidence_review_df["notes"].to_list() == ["cash_row_missing"]
