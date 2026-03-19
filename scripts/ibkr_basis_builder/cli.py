from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from scripts.ibkr_basis_builder.workflow import build_opening_lot_snapshot

DEFAULT_PERSON = "eugene"
DEFAULT_CUTOFF_DATE = "2024-05-01"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an Austrian opening lot snapshot from historical IBKR trades.")
    parser.add_argument("--person", default=DEFAULT_PERSON, help="Person key, for example eugene or oryna.")
    parser.add_argument("--cutoff-date", default=DEFAULT_CUTOFF_DATE, help="Bootstrap cutoff date in YYYY-MM-DD format.")
    parser.add_argument("--ibkr-trade-history-path", required=True)
    parser.add_argument("--move-in-price-csv-path", required=True)
    parser.add_argument(
        "--move-in-price-template-path",
        help="Optional CSV template path. If move-in prices are missing, write the required holdings template there before failing.",
    )
    parser.add_argument("--raw-exchange-rates-path", default="data/input/currencies/raw_exchange_rates.csv")
    parser.add_argument("--output-path")
    return parser


def resolve_output_path(person: str, cutoff_date: str, explicit_path: str | None) -> str:
    if explicit_path:
        return explicit_path
    return str(Path(f"data/input/{person}/ibkr") / f"austrian_opening_lots_{cutoff_date}.csv")


def main() -> None:
    args = build_parser().parse_args()
    output_path = build_opening_lot_snapshot(
        person=args.person,
        cutoff_date=date.fromisoformat(args.cutoff_date),
        ibkr_trade_history_path=args.ibkr_trade_history_path,
        raw_exchange_rates_path=args.raw_exchange_rates_path,
        move_in_price_csv_path=args.move_in_price_csv_path,
        output_path=resolve_output_path(args.person, args.cutoff_date, args.output_path),
        move_in_price_template_path=args.move_in_price_template_path,
    )
    print(output_path)


if __name__ == "__main__":
    main()
