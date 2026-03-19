from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from scripts.reporting_funds.workflow import run_workflow

DEFAULT_PERSON = "eugene"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate reporting-fund ETF ledger and OeKB event artifacts.")
    parser.add_argument("--person", default=DEFAULT_PERSON, help="Person key, for example eugene or oryna.")
    parser.add_argument("--tax-year", type=int, required=True)
    parser.add_argument("--ibkr-tax-xml-path", required=True)
    parser.add_argument(
        "--historical-ibkr-tax-xml-path",
        help="Optional additional IBKR tax XML file, directory, or glob used only as lookup-only historical payout evidence.",
    )
    parser.add_argument("--ibkr-trade-history-path")
    parser.add_argument("--oekb-root-dir")
    parser.add_argument("--state-dir")
    parser.add_argument("--output-dir")
    parser.add_argument(
        "--opening-lots-path",
        help="Optional CSV snapshot used as the opening Austrian lot state for the first bootstrap-based run.",
    )
    parser.add_argument(
        "--authoritative-start-date",
        help="Optional YYYY-MM-DD lower bound for authoritative processing inside the target tax year.",
    )
    parser.add_argument(
        "--carryforward-only",
        action="store_true",
        help="Reconstruct year-end ledger/state for carryforward purposes without treating the run as a normal filing year.",
    )
    parser.add_argument("--resolution-cutoff-date", help="Optional YYYY-MM-DD cutoff for next-year OeKB lookahead.")
    parser.add_argument(
        "--allow-unresolved-payouts",
        action="store_true",
        help="Do not fail the run when unresolved ETF payout rows remain after resolution passes.",
    )
    parser.add_argument(
        "--raw-exchange-rates-path",
        default="data/input/currencies/raw_exchange_rates.csv",
    )
    parser.add_argument(
        "--negative-deemed-income-overrides-path",
        help="Optional CSV with manual decisions for annual reports that contain negative deemed distributed income.",
    )
    return parser


def resolve_ibkr_trade_history_path(person: str, tax_year: int, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path

    trades_dir = Path(f"data/input/{person}/ibkr/trades")
    if trades_dir.exists() and trades_dir.is_dir():
        return str(trades_dir)

    default_path = Path(f"data/input/{person}/{tax_year}/ibkr_{tax_year}0101_{tax_year}1231.xml")
    if default_path.exists():
        return str(default_path)

    raise SystemExit(
        f"No default IBKR trade-history source found. Checked folder {trades_dir} and file {default_path}. "
        "Pass --ibkr-trade-history-path explicitly."
    )


def main() -> None:
    args = build_parser().parse_args()
    output_paths = run_workflow(
        person=args.person,
        tax_year=args.tax_year,
        ibkr_tax_xml_path=args.ibkr_tax_xml_path,
        historical_ibkr_tax_xml_path=args.historical_ibkr_tax_xml_path,
        ibkr_trade_history_path=resolve_ibkr_trade_history_path(args.person, args.tax_year, args.ibkr_trade_history_path),
        oekb_root_dir=args.oekb_root_dir,
        state_dir=args.state_dir,
        output_dir=args.output_dir,
        raw_exchange_rates_path=args.raw_exchange_rates_path,
        resolution_cutoff_date=args.resolution_cutoff_date,
        strict_unresolved_payouts=not args.allow_unresolved_payouts,
        negative_deemed_income_overrides_path=args.negative_deemed_income_overrides_path,
        opening_lots_path=args.opening_lots_path,
        authoritative_start_date=(
            date.fromisoformat(args.authoritative_start_date) if args.authoritative_start_date else None
        ),
        carryforward_only=args.carryforward_only,
    )
    for label, path in output_paths.items():
        print(f"{label}: {path}")


if __name__ == "__main__":
    main()
