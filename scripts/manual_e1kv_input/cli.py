from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from scripts.manual_e1kv_input.workflow import (
    CoreInputs,
    NonReportingFundsInputs,
    ReportingFundsInputs,
    build_e1kv_computation,
    parse_money_input,
    render_output_markdown,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interactively build a manual E1kv filing note from literal inputs.")
    parser.add_argument("--tax-year", type=int, default=date.today().year)
    parser.add_argument("--output-path")
    return parser


def _prompt_money(label: str) -> object:
    raw_value = input(f"{label} [default 0]: ").strip()
    return parse_money_input(raw_value)


def _prompt_core_inputs() -> CoreInputs:
    print("\nCore app inputs")
    return CoreInputs(
        ordinary_capital_income=_prompt_money("Core ordinary capital income 27.5%"),
        trade_profit=_prompt_money("Core trade profit 27.5%"),
        trade_loss=_prompt_money("Core trade loss (enter positive or negative)"),
        fund_distributions=_prompt_money("Core ETF distributions 27.5%"),
        creditable_foreign_tax=_prompt_money("Core source-level creditable foreign tax"),
    )


def _prompt_reporting_funds_inputs() -> ReportingFundsInputs:
    print("\nReporting-funds inputs")
    return ReportingFundsInputs(
        fund_distributions=_prompt_money("Reporting-funds ETF distributions 27.5%"),
        deemed_distributed_income=_prompt_money("Reporting-funds AGE 27.5%"),
        domestic_dividends_kz189=_prompt_money("Reporting-funds domestic dividends (KZ 189)"),
        domestic_dividend_kest_kz899=_prompt_money("Reporting-funds Austrian KESt on domestic dividends (KZ 899)"),
        creditable_foreign_tax=_prompt_money("Reporting-funds creditable foreign tax"),
    )


def _prompt_non_reporting_funds_inputs() -> NonReportingFundsInputs:
    print("\nNon-reporting-funds inputs")
    return NonReportingFundsInputs(
        fund_distributions=_prompt_money("Non-reporting-funds ETF distributions 27.5%"),
        deemed_distributed_income=_prompt_money("Non-reporting-funds AGE 27.5%"),
        domestic_dividends_kz189=_prompt_money("Non-reporting-funds domestic dividends (KZ 189)"),
        domestic_dividend_kest_kz899=_prompt_money("Non-reporting-funds Austrian KESt on domestic dividends (KZ 899)"),
        creditable_foreign_tax=_prompt_money("Non-reporting-funds creditable foreign tax"),
    )


def main() -> None:
    args = build_parser().parse_args()
    default_output_path = Path(f"manual_e1kv_input_{args.tax_year}.md")
    output_path = Path(args.output_path) if args.output_path else default_output_path

    core = _prompt_core_inputs()
    reporting_funds = _prompt_reporting_funds_inputs()
    non_reporting_funds = _prompt_non_reporting_funds_inputs()

    result = build_e1kv_computation(
        core=core,
        reporting_funds=reporting_funds,
        non_reporting_funds=non_reporting_funds,
    )
    output = render_output_markdown(
        tax_year=args.tax_year,
        core=core,
        reporting_funds=reporting_funds,
        non_reporting_funds=non_reporting_funds,
        result=result,
    )
    output_path.write_text(output, encoding="utf-8")
    print(f"\nWrote {output_path}")


if __name__ == "__main__":
    main()
