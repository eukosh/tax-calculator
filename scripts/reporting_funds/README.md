# Reporting Funds Workflow

Standalone workflow for Austrian reporting-fund ETFs held at IBKR.

## Scope

This workflow handles the ETF tax logic that the core app does not:

- OeKB-driven ETF income classification
- OeKB acquisition-cost corrections (`10289`)
- broker payout evidence and cross-year payout resolution
- Austrian ETF carryforward state
- ETF sale basis using Austrian EUR state plus cumulative OeKB corrections

The core app in [`main.py`](/Users/eukosh/Desktop/projects/personal/tax-automation/main.py) excludes reporting-fund ETF trades and ETF payout rows from its normal IBKR path.

## Current Model

The active model is:

- append-only ETF position events
- yearly ETF state snapshots
- cross-year payout evidence state

It is not a FIFO lot-ledger workflow anymore.

Important operating rules:

- OeKB is the Austrian tax source of truth for ETF classification and basis correction
- raw IBKR ETF trade history is the authoritative trade source
- IBKR tax XML is payout evidence, not ETF basis truth
- confirmed broker cash decides whether a payout actually hit the account
- accrual rows are evidence only until broker cash is confirmed
- `10289` changes future ETF sale basis but is not a direct E1kv filing line

## History Requirements

Use as much history as you have.

This is strongly recommended, especially for:

- payout reconciliation across years
- annual `10595` handling
- negative `10287` review and auto-reconciliation

Practical rule:

- `--ibkr-trade-history-path`: provide full raw ETF trade history
- `--historical-ibkr-tax-xml-path`: provide broad historical IBKR tax XML coverage
- `--ibkr-tax-xml-path`: provide full filing-year IBKR tax XML coverage; it can be one file, a directory, or a glob
- if one filing year is split across multiple exports, pass a glob or directory that includes all of them

The workflow can resolve correctly with narrower inputs in simple cases, but broad history is safer and avoids false manual-review blockers.

## Eugene Move-In Case

Important current case:

- `eugene` became Austrian tax resident on `2024-05-01`

So for pre-move ETF holdings:

- broker acquisition basis is not Austrian tax basis
- the workflow should bootstrap from:
  - `data/input/eugene/ibkr/austrian_opening_state_2024-05-01.csv`

Use a `2024` carryforward-only run to build the first Austrian ETF carryforward state, then run `2025` normally.

## Inputs

Main inputs:

- filing-year IBKR tax XML coverage
- historical IBKR tax XML lookup coverage
- raw IBKR ETF trade history
- OeKB root directory
- optional opening Austrian ETF state snapshot
- optional negative deemed-income override CSV

Important source categories:

- IBKR tax XML:
  - `CashTransactions`
  - `ChangeInDividendAccruals`
- raw trade history:
  - ETF `BUY` / `SELL`
- OeKB CSVs:
  - distribution and annual reports

## Outputs

Carryforward state:

- `data/output/<person>/reporting_funds/fund_tax_state_<year>_final.csv`
- `data/output/<person>/reporting_funds/fund_tax_payout_state.csv`

Yearly artifacts:

- `data/output/<person>/reporting_funds/<year>/fund_tax_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_income_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_basis_adjustments_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_sales_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_payout_resolution_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_payout_evidence_review_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_negative_deemed_distribution_review_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/reporting_funds_<year>_summary.md`

The summary exposes the filing-oriented ETF subtotals:

- `ETF distributions 27.5%`
- `Ausschüttungsgleiche Erträge 27.5%`
- `Domestic dividends in loss offset (KZ 189)`
- `Austrian KESt on domestic dividends (KZ 899)`
- `Creditable foreign tax`

Important:

- `10759 / KZ 189` and `10760 / KZ 899` are separate filing fields
- they do not modify `10286`, `10287`, `10595`, or `10288`
- `10760` is Austrian KESt, not foreign withholding tax

## CLI

Normal yearly run:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2025 \
  --ibkr-tax-xml-path data/input/eugene/2025/ibkr_20250101_20260101.xml \
  --historical-ibkr-tax-xml-path 'data/input/eugene/202[34]/*.xml' \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb
```

Bootstrap / carryforward-only run:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2024 \
  --ibkr-tax-xml-path data/input/eugene/2024 \
  --historical-ibkr-tax-xml-path 'data/input/eugene/202[34]/*.xml' \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb \
  --opening-state-path data/input/eugene/ibkr/austrian_opening_state_2024-05-01.csv \
  --authoritative-start-date 2024-05-01 \
  --resolution-cutoff-date 2024-12-31 \
  --carryforward-only
```

Useful flags:

- `--historical-ibkr-tax-xml-path`
- `--opening-state-path`
- `--authoritative-start-date YYYY-MM-DD`
- `--carryforward-only`
- `--resolution-cutoff-date YYYY-MM-DD`
- `--allow-unresolved-payouts`
- `--negative-deemed-income-overrides-path`

## Notes

- If a negative `10287` row cannot be reconciled safely, the workflow writes a review CSV and blocks.
- If you want fewer manual-review cases, broaden `--historical-ibkr-tax-xml-path`.
- The yearly state snapshot is the carryforward input for the next run.
- The event log is the audit trail.
