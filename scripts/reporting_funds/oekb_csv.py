from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from scripts.reporting_funds.models import OekbReport, round_money

REQUIRED_METADATA_LABELS = (
    "ISIN",
    "Währung",
    "Meldedatum",
    "Jahresmeldung",
    "Ausschüttungsmeldung",
)
REQUIRED_CODE_VALUES = ("10286", "10287", "10595", "10288", "10289")
PRIVATE_INVESTOR_SECTION_TITLE = "Kennzahlen ESt-Erklärung Privatanleger (je Anteil)"


def _parse_decimal(value: str) -> float:
    normalized = value.strip().replace(".", "").replace(",", ".")
    return float(normalized)


def _parse_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%d.%m.%Y").date()


def _parse_optional_date(value: str) -> date | None:
    value = value.strip()
    return _parse_date(value) if value else None


def _parse_bool_ja_nein(value: str) -> bool:
    normalized = value.strip().upper()
    if normalized == "JA":
        return True
    if normalized == "NEIN":
        return False
    raise ValueError(f"Unexpected JA/NEIN value: {value!r}")


def _load_lines(path: str | Path) -> list[list[str]]:
    return [line.rstrip("\n").split(";") for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def _find_value(lines: list[list[str]], label: str) -> str:
    for row in lines:
        if row and row[0].strip() == label:
            if len(row) < 2:
                raise ValueError(f"Missing value for '{label}' in OeKB file")
            return row[1].strip()
    raise ValueError(f"Missing '{label}' row in OeKB file")


def _find_optional_value(lines: list[list[str]], label: str) -> str:
    for row in lines:
        if row and row[0].strip() == label:
            return row[1].strip() if len(row) >= 2 else ""
    return ""


def _find_section_rows(lines: list[list[str]], section_title: str) -> list[list[str]]:
    for index, row in enumerate(lines):
        if row and row[0].strip() == section_title:
            start_index = index + 1
            if start_index < len(lines) and lines[start_index] and set(lines[start_index][0].strip()) == {"="}:
                start_index += 1

            section_rows: list[list[str]] = []
            for current in lines[start_index:]:
                if (
                    len(current) == 1
                    and current[0].strip()
                    and set(current[0].strip()) != {"="}
                ):
                    break
                section_rows.append(current)
            return section_rows
    return []


def _find_numeric_value_by_code_in_rows(rows: list[list[str]], code: str) -> float | None:
    for row in rows:
        if len(row) < 5 or row[-1].strip() != code:
            continue
        for field in row[1:-2]:
            if field.strip():
                try:
                    return round_money(_parse_decimal(field))
                except ValueError:
                    continue
        raise ValueError(f"Missing numeric value for OeKB code {code}")
    return None


def _find_numeric_value_by_code(lines: list[list[str]], code: str) -> float:
    private_investor_rows = _find_section_rows(lines, PRIVATE_INVESTOR_SECTION_TITLE)
    private_investor_value = _find_numeric_value_by_code_in_rows(private_investor_rows, code)
    if private_investor_value is not None:
        return private_investor_value

    fallback_value = _find_numeric_value_by_code_in_rows(lines, code)
    if fallback_value is not None:
        return fallback_value
    raise ValueError(f"Missing OeKB code {code}")


def _find_optional_numeric_value_by_code(lines: list[list[str]], code: str) -> float | None:
    for row in lines:
        if len(row) < 5 or row[-1].strip() != code:
            continue
        for field in row[1:-2]:
            if field.strip():
                try:
                    return round_money(_parse_decimal(field))
                except ValueError:
                    continue
        return None
    return None


def load_oekb_report(path: str | Path, tax_year: int | None = None, ticker_by_isin: dict[str, str] | None = None) -> OekbReport:
    report_path = Path(path)
    lines = _load_lines(report_path)

    for label in REQUIRED_METADATA_LABELS:
        _find_value(lines, label)
    for code in REQUIRED_CODE_VALUES:
        _find_numeric_value_by_code(lines, code)

    isin = _find_value(lines, "ISIN")
    currency = _find_value(lines, "Währung")
    meldedatum = _parse_date(_find_value(lines, "Meldedatum"))
    if tax_year is not None and meldedatum.year != tax_year:
        raise ValueError(f"OeKB Meldedatum {meldedatum.isoformat()} is outside reporting year {tax_year}")

    ticker = (ticker_by_isin or {}).get(isin, report_path.stem)

    return OekbReport(
        ticker=ticker,
        isin=isin,
        meldedatum=meldedatum,
        currency=currency,
        is_jahresmeldung=_parse_bool_ja_nein(_find_value(lines, "Jahresmeldung")),
        is_ausschuettungsmeldung=_parse_bool_ja_nein(_find_value(lines, "Ausschüttungsmeldung")),
        ausschuettungstag=_parse_optional_date(_find_optional_value(lines, "Ausschüttungstag")),
        ex_tag=_parse_optional_date(_find_optional_value(lines, "Ex-Tag")),
        meldezeitraum_beginn=_parse_optional_date(_find_optional_value(lines, "Meldezeitraum Beginn")),
        meldezeitraum_ende=_parse_optional_date(_find_optional_value(lines, "Meldezeitraum Ende")),
        geschaeftsjahres_beginn=_parse_optional_date(_find_optional_value(lines, "Geschäftsjahres-Beginn")),
        geschaeftsjahres_ende=_parse_optional_date(_find_optional_value(lines, "Geschäftsjahres-Ende")),
        reported_distribution_per_share_ccy=_find_numeric_value_by_code(lines, "10286"),
        age_per_share_ccy=_find_numeric_value_by_code(lines, "10287"),
        non_reported_distribution_per_share_ccy=_find_numeric_value_by_code(lines, "10595"),
        creditable_foreign_tax_per_share_ccy=_find_numeric_value_by_code(lines, "10288"),
        acquisition_cost_correction_per_share_ccy=_find_numeric_value_by_code(lines, "10289"),
        source_file=str(report_path),
        total_shares_at_inflow=(
            round_money(_parse_decimal(_find_optional_value(lines, "Anzahl Anteile zum Zuflusszeitpunkt")))
            if _find_optional_value(lines, "Anzahl Anteile zum Zuflusszeitpunkt")
            else None
        ),
        total_distributions_per_share_ccy=_find_optional_numeric_value_by_code(lines, "10047"),
        capital_repayment_per_share_ccy=_find_optional_numeric_value_by_code(lines, "10051"),
        basis_age_component_per_share_ccy=_find_optional_numeric_value_by_code(lines, "10054"),
        basis_distribution_component_per_share_ccy=_find_optional_numeric_value_by_code(lines, "10055"),
        withheld_tax_on_non_reported_distributions_per_share_ccy=_find_optional_numeric_value_by_code(lines, "10114"),
    )


def load_matching_oekb_reports(
    oekb_dir: str | Path,
    required_isins: set[str],
    *,
    tax_year: int | None = None,
    ticker_by_isin: dict[str, str] | None = None,
) -> list[OekbReport]:
    directory = Path(oekb_dir)
    if not directory.exists():
        return []

    reports: list[OekbReport] = []
    for path in sorted(directory.glob("*.csv")):
        report = load_oekb_report(path, tax_year=tax_year, ticker_by_isin=ticker_by_isin)
        if report.isin in required_isins:
            reports.append(report)

    return sorted(
        reports,
        key=lambda report: (
            report.payout_date or report.meldedatum,
            report.eligibility_date,
            report.ticker,
            report.source_file,
        ),
    )


def load_required_oekb_reports(
    oekb_dir: str | Path,
    tax_year: int,
    required_isins: set[str],
    ticker_by_isin: dict[str, str] | None = None,
) -> list[OekbReport]:
    directory = Path(oekb_dir)
    if not directory.exists():
        raise FileNotFoundError(f"OeKB directory does not exist: {directory}")

    reports = load_matching_oekb_reports(
        directory,
        required_isins,
        tax_year=tax_year,
        ticker_by_isin=ticker_by_isin,
    )

    discovered_isins = {report.isin for report in reports}
    missing_isins = sorted(required_isins - discovered_isins)
    if missing_isins:
        formatted_missing = [
            f"{ticker_by_isin[isin]} ({isin})" if ticker_by_isin and isin in ticker_by_isin else isin
            for isin in missing_isins
        ]
        raise FileNotFoundError(
            f"Missing OeKB CSV files for reporting-fund ISINs: {', '.join(formatted_missing)}"
        )
    return reports
