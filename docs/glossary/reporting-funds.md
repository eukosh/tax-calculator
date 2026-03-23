# Reporting-Funds Glossary

This page documents the current reporting-fund ETF workflow.

The active model is:

- yearly ETF position state snapshots
- yearly ETF event logs
- yearly filing-oriented income and basis-adjustment exports
- cross-year payout evidence state

It is not a lot-ledger / FIFO workflow anymore.

## File Inventory

| File / Pattern | Kind | Purpose | Row Grain |
| --- | --- | --- | --- |
| `data/output/<person>/reporting_funds/<year>/fund_tax_events_<year>.csv` | Output | Chronological ETF position-event audit trail | one ETF state-changing event |
| `data/output/<person>/reporting_funds/<year>/fund_tax_income_events_<year>.csv` | Output | Filing-oriented ETF income rows | one taxable ETF income event |
| `data/output/<person>/reporting_funds/<year>/fund_tax_basis_adjustments_<year>.csv` | Output | OeKB `10289` basis-adjustment rows | one report-driven basis-adjustment event |
| `data/output/<person>/reporting_funds/<year>/fund_tax_sales_<year>.csv` | Output | ETF sale realizations | one ETF sale execution |
| `data/output/<person>/reporting_funds/<year>/fund_tax_payout_resolution_events_<year>.csv` | Output | High-level payout-resolution decisions | one payout resolution decision |
| `data/output/<person>/reporting_funds/<year>/fund_tax_payout_evidence_review_<year>.csv` | Output | Review rows for unresolved or deferred payout evidence | one payout evidence row |
| `data/output/<person>/reporting_funds/<year>/fund_tax_negative_deemed_distribution_review_<year>.csv` | Output | Review rows for negative deemed-income cases | one annual-report review case |
| `data/output/<person>/reporting_funds/<year>/reporting_funds_<year>_summary.md` | Output | Human summary of the ETF run | one markdown report |
| `data/output/<person>/reporting_funds/fund_tax_state_<year>_final.csv` | State | End-of-year ETF carryforward state | one ETF position |
| `data/output/<person>/reporting_funds/fund_tax_payout_state.csv` | State | Cross-year payout evidence and resolution state | one payout evidence key |
| `data/input/<person>/ibkr/austrian_opening_state_<date>.csv` | Input | Optional opening Austrian ETF state for move-in/bootstrap runs | one Austrian opening position |

## Important Inputs

### Target-Year IBKR Tax XML

Purpose:

- target-year broker payout evidence

Used for:

- `CashTransactions`
- `ChangeInDividendAccruals`

### Historical IBKR Tax XML

Purpose:

- lookup-only historical payout evidence

Recommendation:

- provide as much history as you have

This is especially important for:

- annual `10595` payout resolution
- negative `10287` reconciliation
- avoiding unnecessary manual-review blockers

### Raw IBKR ETF Trade History

Purpose:

- authoritative ETF `BUY` / `SELL` history

Recommendation:

- provide full history

### OeKB Root Directory

Purpose:

- Austrian ETF tax source of truth

Relevant OeKB fields:

- `10286`: distributions
- `10287`: deemed distributed income
- `10288`: creditable foreign tax
- `10289`: acquisition-cost correction
- `10595`: non-reported distributions to be taxed at cash-flow level
- `10759`: domestic dividends in loss offset (`KZ 189`)
- `10760`: Austrian KESt on those domestic dividends (`KZ 899`)

### Opening Austrian ETF State

Purpose:

- authoritative starting Austrian ETF basis after move-in

Used for:

- first bootstrap run
- carryforward reconstruction when pre-move broker basis is not Austrian basis

## Main Outputs

### `fund_tax_events_<year>.csv`

Purpose:

- chronological audit trail of ETF position state changes

Typical event types:

- `austrian_basis_reset`
- `buy`
- `sell`
- `oekb_basis_adjustment`

Important fields:

- `event_type`
- `event_date`
- `effective_date`
- `eligibility_date`
- `quantity_delta`
- `base_cost_delta_eur`
- `basis_adjustment_delta_eur`
- `quantity_after`
- `base_cost_total_eur_after`
- `basis_adjustment_total_eur_after`
- `total_basis_eur_after`
- `average_basis_eur_after`
- `source_file`
- `notes`

### `fund_tax_income_events_<year>.csv`

Purpose:

- filing-oriented ETF income rows after OeKB classification and payout-resolution logic

Important fields:

- `event_type`
- `event_date`
- `eligibility_date`
- `ticker`
- `isin`
- `quantity`
- `amount_total_eur`
- `creditable_foreign_tax_total_eur`
- `domestic_dividend_kest_total_eur`
- `matched_broker_event_id`
- `source_file`
- `notes`

### `fund_tax_basis_adjustments_<year>.csv`

Purpose:

- OeKB `10289` basis-adjustment rows in filing/audit form

Important fields:

- `report_type`
- `eligibility_date`
- `effective_date`
- `acquisition_cost_correction_per_share_ccy`
- `shares_held_on_eligibility_date`
- `basis_stepup_total_eur`
- `source_file`
- `notes`

### `fund_tax_sales_<year>.csv`

Purpose:

- ETF sale execution results under Austrian EUR basis

Important fields:

- `sale_date`
- `ticker`
- `isin`
- `quantity_sold`
- `taxable_proceeds_eur`
- `realized_base_cost_eur`
- `realized_oekb_adjustment_eur`
- `taxable_total_basis_eur`
- `taxable_gain_loss_eur`
- `sale_trade_id`
- `notes`

### `fund_tax_state_<year>_final.csv`

Purpose:

- carryforward ETF state for the next run

Important fields:

- `ticker`
- `isin`
- `asset_class`
- `currency`
- `quantity`
- `base_cost_total_eur`
- `basis_adjustment_total_eur`
- `total_basis_eur`
- `average_basis_eur`
- `status`
- `last_event_date`
- `basis_method`
- `source_file`
- `notes`

### `fund_tax_payout_state.csv`

Purpose:

- cross-year memory of payout evidence and payout resolution

Important fields:

- `payout_key`
- `ticker`
- `isin`
- `ex_date`
- `pay_date`
- `quantity`
- `evidence_state`
- `status`
- `resolved_tax_year`
- `resolved_by_report_year`
- `resolved_by_report_file`
- `resolution_mode`
- `source_statement_file`
- `notes`

### `fund_tax_payout_evidence_review_<year>.csv`

Purpose:

- review file for payout evidence that is unresolved, deferred, or otherwise important to inspect manually

### `fund_tax_negative_deemed_distribution_review_<year>.csv`

Purpose:

- review and override file for negative `10287` cases

Important fields:

- `decision`
- `status`
- `eligible_quantity_used`
- `quantity_held_on_report_date`
- `candidate_payout_count`
- `matched_payout_count`
- `target_distribution_per_share_ccy`
- `deemed_distributed_income_per_share_ccy`
- `source_file`
- `notes`

## Summary Markdown

### `reporting_funds_<year>_summary.md`

Purpose:

- filing-oriented ETF summary plus carryforward guidance

Key filing lines:

- `ETF distributions 27.5%`
- `Ausschüttungsgleiche Erträge 27.5%`
- `Domestic dividends in loss offset (KZ 189)`
- `Austrian KESt on domestic dividends (KZ 899)`
- `Creditable foreign tax`

Important interpretation:

- `10289` does not go into E1kv directly
- it only changes future ETF sale basis
- `10759` and `10760` stay separate from `10286`, `10287`, `10595`, and `10288`

## Operating Rules

- Use full raw ETF trade history whenever possible.
- Use broad historical IBKR tax XML coverage whenever possible.
- Use the target-year IBKR tax XML as the primary filing-year broker evidence.
- Use the yearly ETF state snapshot plus payout-state file as the carryforward inputs for the next year.
