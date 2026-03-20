# Tax Calculator

Python tooling for Austrian tax preparation across multiple brokers and fund-tax workflows.

## Repository Map

This repo currently has three main tax-calculation surfaces:

- Core app in [`main.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/main.py)
- Reporting-fund ETF workflow in [`scripts/reporting_funds/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/reporting_funds/README.md)
- Non-reporting-fund exit workflow in [`scripts/non_reporting_funds_exit/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/non_reporting_funds_exit/README.md)

## Core App

The core app owns the normal yearly multi-broker summary flow.

Current relevant scope:

- IBKR cash dividends and withholding for non-ETF securities
- IBKR stock/ADR/REIT sales
- IBKR bond realized PnL
- Freedom Finance
- Revolut
- Wise

Important scope boundary:

- Austrian reporting-fund ETFs are excluded from the core IBKR trade/dividend path and are handled only by the standalone reporting-funds workflow
- Austrian non-reporting-fund deemed-income and later exit calculations are handled only by the standalone non-reporting-funds workflow

## Core IBKR Stock Mode

The current IBKR stock/ADR/REIT path uses one Austrian-authoritative raw-trade model for post-move sales.

When configured:

- raw IBKR trade-history XML is required
- the Austrian opening-lot snapshot CSV is the tax-basis authority for positions already held at move-in
- raw IBKR trade-history XML is replayed internally after the authoritative start date
- broker closed-lot XML is used only for reconciliation
- if there were no pre-move open lots, the raw-trade authoritative path can also run without an opening-lot snapshot file

Current Eugene case:

- Austrian tax residence started on `2024-05-01`
- the move-in opening snapshot is built by [`scripts/ibkr_basis_builder/cli.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/ibkr_basis_builder/cli.py)

Current IBKR artifacts produced by `main.py` include:

- `trades_tax_df`
- `trades_reconciliation`
- `stock_tax_lot_state_full`
- `stock_tax_open_lots_final`
- `dividends_country_agg`
- `bonds_tax_df`
- `bonds_tax_country_agg_df`
- `ibkr_summary`

## Core Run Examples

The core app is currently configured directly in [`main.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/main.py), not through a standalone CLI.

Typical copy-edit-run flow:

1. Open [`main.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/main.py)
2. Adjust:
   - `person`
   - `ibkr_input_path`
   - `ibkr_trade_history_path`
   - `austrian_opening_lots_path`
   - `authoritative_start_date`
   - `reporting_start_date`
   - `reporting_end_date`
   - `include_freedom_trades` if needed
3. Run from repo root:

```bash
poetry run python main.py
```

Sample Eugene 2025 stock-authoritative setup already present in `main.py`:

```python
person = "eugene"
ibkr_input_path = "data/input/eugene/2025/ibkr_20250101_20260101.xml"
ibkr_trade_history_path = "data/input/eugene/ibkr/trades/*.xml"
austrian_opening_lots_path = "data/input/eugene/ibkr/austrian_opening_lots_2024-05-01.csv"
authoritative_start_date = date(2024, 5, 1)

reporting_start_date = date(2025, 1, 1)
reporting_end_date = date(2025, 12, 31)
```

If all buys are already post-move and no opening snapshot is needed, keep:

```python
ibkr_trade_history_path = "data/input/<person>/ibkr/trades/*.xml"
austrian_opening_lots_path = None
authoritative_start_date = None
```

Rule:

- `authoritative_start_date` is only valid together with an Austrian opening-lot snapshot
- without a snapshot, provide the full relevant raw trade history instead of a lower-bound cutoff

## Documentation

Workflow docs:

- Core + repo overview: this file
- Reporting-fund ETFs: [`scripts/reporting_funds/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/reporting_funds/README.md)
- Non-reporting-fund exit: [`scripts/non_reporting_funds_exit/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/non_reporting_funds_exit/README.md)

Schema and artifact glossary:

- Glossary index: [`docs/glossary/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/README.md)
- Core app glossary: [`docs/glossary/core-app.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/core-app.md)
- Reporting-funds glossary: [`docs/glossary/reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/reporting-funds.md)
- Non-reporting-funds glossary: [`docs/glossary/non-reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/non-reporting-funds.md)

Rule of thumb:

- README files explain workflow, scope, and how to run things
- glossary pages explain files, row grain, and column meaning

## Practical IBKR Note

When generating IBKR Flex Queries, it is still useful to extend the requested period slightly beyond calendar year-end.

Reason:

- brokers can post later cash confirmations, withholding adjustments, reversals, or close-lot artifacts after the original ex-date or pay-date window
