from __future__ import annotations

import argparse
from pathlib import Path

from scripts.non_reporting_funds_exit.workflow import run_workflow

DEFAULT_PERSON = "eugene"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate non-reporting-fund exit artifacts for SCHD/TLT.")
    parser.add_argument("--person", default=DEFAULT_PERSON, help="Person key, for example eugene or oryna.")
    parser.add_argument("--statement-path")
    parser.add_argument("--price-input-path")
    parser.add_argument("--sale-plan-path")
    parser.add_argument("--output-dir")
    parser.add_argument(
        "--raw-exchange-rates-path",
        default="data/input/currencies/raw_exchange_rates.csv",
    )
    parser.add_argument("--tax-year", type=int, default=2025)
    return parser


def resolve_statement_path(person: str, tax_year: int, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path

    statement_dir = Path(f"data/input/{person}/{tax_year}/non_reporting_funds_exit")
    statement_paths = sorted(statement_dir.glob("*.json"))
    if len(statement_paths) == 1:
        return str(statement_paths[0])

    if not statement_paths:
        raise SystemExit(
            f"No statement JSON found under {statement_dir}. Pass --statement-path explicitly."
        )

    raise SystemExit(
        f"Multiple statement JSON files found under {statement_dir}. Pass --statement-path explicitly."
    )


def resolve_price_input_path(tax_year: int, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path
    return f"data/input/non_reporting_funds_exit/non_reporting_funds_{tax_year}_prices.csv"


def resolve_sale_plan_path(person: str, tax_year: int, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path
    return f"data/input/{person}/{tax_year}/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv"


def resolve_output_dir(person: str, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path
    return f"data/output/{person}/non_reporting_funds_exit"


def main() -> None:
    args = build_parser().parse_args()
    statement_path = resolve_statement_path(args.person, args.tax_year, args.statement_path)
    price_input_path = resolve_price_input_path(args.tax_year, args.price_input_path)
    sale_plan_path = resolve_sale_plan_path(args.person, args.tax_year, args.sale_plan_path)
    output_dir = resolve_output_dir(args.person, args.output_dir)

    output_paths = run_workflow(
        statement_path=statement_path,
        price_input_path=price_input_path,
        sale_plan_path=sale_plan_path,
        output_dir=output_dir,
        tax_year=args.tax_year,
        raw_exchange_rates_path=args.raw_exchange_rates_path,
    )
    for label, path in output_paths.items():
        print(f"{label}: {path}")


if __name__ == "__main__":
    main()
