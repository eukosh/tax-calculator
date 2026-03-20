# Reporting Funds Workflow

Standalone workflow for Austrian reporting-fund ETFs held at IBKR.

## Scope

This workflow owns Austrian ETF tax treatment that the core app does not handle:

- reporting-fund payout reconciliation
- OeKB classification of ETF income
- OeKB acquisition-cost corrections
- ETF Austrian lot carryforward
- ETF sale basis using Austrian EUR lots plus cumulative OeKB corrections

The core app in [`main.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/main.py) excludes ETF trades and ETF cash/dividend rows from its normal IBKR path.

## Current Tax Model

Current operating rules:

- OeKB is the Austrian tax source of truth for reporting-fund ETF classification and basis correction
- IBKR cash transactions decide whether a payout actually hit the account and on which cash date
- accrual rows remain audit context, but do not by themselves create a taxable broker payout
- same-year `Ausschüttungsmeldung` resolves a confirmed broker payout in that payout year
- annual `10595` does not create a synthetic pooled amount; it classifies actual confirmed broker cash payouts inside the annual report period
- if broker cash is not confirmed yet, the payout stays deferred until the later year in which cash actually appears

## Person-Specific Austrian Opening State

The workflow supports a person-specific authoritative start date.

Important current case:

- `eugene` became Austrian tax resident on `2024-05-01`

Practical consequence:

- pre-`2024-05-01` broker acquisition basis is not Austrian tax basis for ETF positions already open on move-in
- the workflow can bootstrap from an Austrian opening-lot snapshot CSV as of `2024-05-01`
- a `2024` carryforward-only run can rebuild post-move ETF state without pretending to be a normal filing-year workflow

The opening snapshot is produced by:

- [`scripts/ibkr_basis_builder/workflow.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/ibkr_basis_builder/workflow.py)
- [`scripts/ibkr_basis_builder/cli.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/ibkr_basis_builder/cli.py)

## Inputs

Main inputs:

- yearly IBKR tax XML
- optional historical IBKR tax XML used only as lookup evidence
- raw IBKR ETF trade history
- OeKB root directory
- optional opening Austrian lot snapshot
- optional negative deemed-income override CSV

The CLI is defined in [`scripts/reporting_funds/cli.py`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/reporting_funds/cli.py).

Important source categories:

- yearly IBKR tax XML: `CashTransactions`, `ChangeInDividendAccruals`
- raw IBKR trade history: ETF `BUY`/`SELL` rows
- OeKB CSV exports: distribution and annual report data used for Austrian classification and basis correction

## Resolution Model

Broker payout evidence states:

- `confirmed_cash`
- `accrual_pre_payout_only`
- `accrual_realized_cash_missing`

Resolution rules:

- same-year `Ausschüttungsmeldung` resolves a confirmed broker payout in that year
- annual `10595` emits filing events only for confirmed broker cash payouts inside the annual report period
- broker cash outside relevant OeKB periods stays a broker payout tax event
- unresolved year-end accrual-only rows stay deferred and visible for review
- negative `10287` is handled conservatively and can remain review/override-driven

## Outputs

State files:

- `data/output/<person>/reporting_funds/fund_tax_ledger_<year>_final.csv`
- `data/output/<person>/reporting_funds/fund_tax_payout_state.csv`

Run artifacts:

- `data/output/<person>/reporting_funds/<year>/fund_tax_income_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_basis_adjustments_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_sales_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_payout_resolution_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_payout_evidence_review_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_negative_deemed_distribution_review_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/reporting_funds_<year>_summary.md`

How these differ from the core stock path:

- ETF sales still include `taxable_stepup_basis_eur` because OeKB basis adjustments are part of ETF sale basis
- ETF state is persisted year to year through the ETF ledger and payout-state files
- the core stock path instead rebuilds from opening lots plus raw trade history and writes stock ledger outputs only as audit artifacts

## CLI

The commands below are intended to be copy-pasted and then edited only where your paths or person differ.

Normal yearly run:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2025 \
  --ibkr-tax-xml-path data/input/eugene/2025/ibkr_20250101_20260101.xml \
  --historical-ibkr-tax-xml-path 'data/input/eugene/202[34]/ibkr_*.xml' \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb
```

Bootstrap / carryforward-only run:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2024 \
  --ibkr-tax-xml-path data/input/eugene/2024/ibkr_20250101_20241231.xml \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb \
  --opening-lots-path data/input/eugene/ibkr/austrian_opening_lots_2024-05-01.csv \
  --authoritative-start-date 2024-05-01 \
  --carryforward-only
```

Useful flags:

- `--historical-ibkr-tax-xml-path`
- `--opening-lots-path`
- `--authoritative-start-date YYYY-MM-DD`
- `--carryforward-only`
- `--resolution-cutoff-date YYYY-MM-DD`
- `--allow-unresolved-payouts`
- `--negative-deemed-income-overrides-path`

## Documentation

Workflow overview:

- top-level repo doc: [`README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/README.md)

Schema and artifact glossary:

- glossary index: [`docs/glossary/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/README.md)
- reporting-funds glossary: [`docs/glossary/reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/reporting-funds.md)

Use this README for:

- workflow boundaries
- operating rules
- CLI usage

Use the glossary for:

- file inventory
- row grain
- column meaning

## Current Interpretation Notes

- OeKB `10289` affects future ETF sale basis; it is not a direct E1kv filing line by itself
- broker ETF withholding remains audit evidence only; Austrian creditable foreign tax comes from OeKB-driven classification
- deferred payout-evidence rows are not filing rows
- summary markdown is filing-oriented, while the CSV artifacts also serve audit and reconciliation purposes
