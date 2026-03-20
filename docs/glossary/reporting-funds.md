# Reporting-Funds Glossary

This page documents the reporting-fund ETF workflow artifacts and the most important enabling inputs.

## File Inventory

| File / Pattern | Kind | Purpose | Row Grain |
| --- | --- | --- | --- |
| `data/output/<person>/reporting_funds/<year>/fund_tax_income_events_<year>.csv` | Output | Filing-oriented ETF income events | one taxable ETF income event |
| `data/output/<person>/reporting_funds/<year>/fund_tax_basis_adjustments_<year>.csv` | Output | OeKB basis-correction events attached to held ETF quantity | one report-driven basis-adjustment event |
| `data/output/<person>/reporting_funds/<year>/fund_tax_sales_<year>.csv` | Output | ETF sale allocations using Austrian ETF lots | one consumed ETF lot slice per sale |
| `data/output/<person>/reporting_funds/<year>/fund_tax_payout_resolution_events_<year>.csv` | Output | High-level payout resolution decisions | one payout resolution decision |
| `data/output/<person>/reporting_funds/<year>/fund_tax_payout_evidence_review_<year>.csv` | Output | Review artifact for unresolved or deferred payout evidence | one payout evidence row |
| `data/output/<person>/reporting_funds/<year>/fund_tax_negative_deemed_distribution_review_<year>.csv` | Output | Review artifact for negative deemed-income cases | one annual report review case |
| `data/output/<person>/reporting_funds/<year>/reporting_funds_<year>_summary.md` | Output | Human summary of the ETF run | one markdown report |
| `data/output/<person>/reporting_funds/fund_tax_ledger_<year>_final.csv` | State | End-of-year ETF lot ledger carried forward into later years | one ETF Austrian lot |
| `data/output/<person>/reporting_funds/fund_tax_payout_state.csv` | State | Cross-year payout evidence and resolution state | one payout evidence key |
| `data/input/<person>/ibkr/austrian_opening_lots_<date>.csv` | Input | Optional opening Austrian lot snapshot used when bootstrapping post-move ETF state | one Austrian opening lot |

## Important Inputs

### Yearly IBKR tax XML

Purpose:

- broker evidence source for ETF payout cash and accrual state

Important sections:

- `CashTransactions`
- `ChangeInDividendAccruals`

Use in workflow:

- cash transactions prove actual broker payout cash dates and amounts
- accrual rows provide payout evidence state and matching context

### Historical IBKR tax XML

Purpose:

- lookup-only evidence pool for backward-looking validation

Use in workflow:

- used for cases like negative `10287`
- does not create new current-year payout events by itself

### Raw IBKR ETF trade history

Purpose:

- ETF `BUY` and `SELL` source used to build and deplete Austrian ETF lots

Use in workflow:

- reconstructs ETF lot history
- sales consume ETF lots using internal FIFO

### OeKB root directory

Purpose:

- Austrian source of truth for ETF report classification and basis correction

Use in workflow:

- distribution reports classify payout events
- annual reports provide annual tax values such as `10595`
- basis-correction values update ETF lot basis

### Opening Austrian lot snapshot

Purpose:

- authoritative starting ETF lot state on the first bootstrap run

Use in workflow:

- especially important for Eugene’s move-in on `2024-05-01`
- supplies Austrian ETF basis instead of pre-move broker basis

## Reporting-Funds Outputs

### `fund_tax_income_events_<year>.csv`

Purpose:

- filing-oriented ETF income rows after OeKB classification and broker payout evidence resolution

Columns:

- `event_type`: type of ETF income event, for example broker payout or annual non-reported distribution classification
- `tax_year`: filing year of the event
- `event_date`: tax date used for the event
- `eligibility_date`: date used to determine the eligible quantity for the event; often report or ex-date driven
- `ticker`: ETF symbol
- `isin`: ETF identifier
- `currency`: original report or cash currency
- `quantity`: quantity relevant to the event
- `amount_per_share_ccy`: per-share amount in original currency
- `amount_total_ccy`: total amount in original currency
- `amount_total_eur`: total amount in EUR used by Austrian tax logic
- `creditable_foreign_tax_total_ccy`: foreign tax amount in original currency where applicable
- `creditable_foreign_tax_total_eur`: foreign tax amount in EUR where applicable
- `broker_gross_amount_ccy`: broker payout gross cash in original currency when the row is tied to broker cash
- `broker_net_amount_ccy`: broker payout net cash in original currency when visible
- `broker_tax_amount_ccy`: broker tax withheld in original currency when visible
- `matched_broker_event_id`: identifier of the resolved broker payout evidence row
- `source_file`: OeKB or broker source file used to create the row
- `notes`: explanation of how the row was produced

### `fund_tax_basis_adjustments_<year>.csv`

Purpose:

- report-level basis-correction events before they are distributed across individual ETF lots

Columns:

- `tax_year`: year in which the basis event is recorded
- `ticker`
- `isin`
- `report_type`: OeKB report category that created the adjustment
- `eligibility_date`: date used to determine eligible quantity
- `effective_date`: date on which the basis correction is treated as effective in the lot model
- `currency`: source currency
- `acquisition_cost_correction_per_share_ccy`: per-share OeKB basis-correction amount in original currency
- `shares_held_on_eligibility_date`: total ETF shares eligible for the adjustment
- `basis_stepup_total_ccy`: total basis increase in original currency
- `basis_stepup_total_eur`: total basis increase in EUR
- `fx_to_eur`: FX used for conversion
- `source_file`: source OeKB file
- `notes`: explanation of matching or treatment

### `fund_tax_sales_<year>.csv`

Purpose:

- ETF sale allocations after internal FIFO depletion of Austrian ETF lots

Columns:

- `sale_date`: ETF sale date
- `ticker`
- `isin`
- `quantity_sold`: full sale quantity
- `sale_price_ccy`: per-share sale price in original currency
- `sale_fx`: sale-date FX
- `lot_id`: consumed ETF lot id
- `lot_buy_date`: lot acquisition date
- `quantity_from_lot`: quantity taken from the lot
- `taxable_proceeds_eur`: gross sale proceeds allocated to this row
- `taxable_original_basis_eur`: original EUR basis allocated from the lot
- `taxable_stepup_basis_eur`: cumulative ETF step-up basis allocated from the lot; this is what makes ETF sale rows different from core stock rows
- `taxable_total_basis_eur`: `taxable_original_basis_eur + taxable_stepup_basis_eur`
- `taxable_gain_loss_eur`: `taxable_proceeds_eur - taxable_total_basis_eur`
- `notes`: sale-audit notes

### `fund_tax_payout_resolution_events_<year>.csv`

Purpose:

- compact record of how payout-evidence rows were resolved

Columns:

- `payout_key`: stable payout evidence key
- `ticker`
- `isin`
- `pay_date`: broker cash date associated with the payout
- `report_year`: OeKB report year that resolved the payout
- `resolution_mode`: matching mode used to resolve the payout
- `status`: current resolution status
- `notes`: explanation of the decision

### `fund_tax_payout_evidence_review_<year>.csv`

Purpose:

- review file for payout-evidence rows that are unresolved, deferred, or otherwise useful for manual inspection

Columns:

- `payout_key`: stable payout evidence key
- `ticker`
- `isin`
- `ex_date`: distribution ex-date
- `pay_date`: expected or observed payout date
- `quantity`: quantity associated with the payout evidence
- `currency`: source currency
- `evidence_state`: evidence classification such as `confirmed_cash`, `accrual_pre_payout_only`, or `accrual_realized_cash_missing`
- `status`: workflow status of the evidence row
- `broker_gross_amount_ccy`: broker gross payout cash
- `broker_net_amount_ccy`: broker net payout cash
- `broker_tax_amount_ccy`: broker withheld tax amount
- `action_id`: broker action identifier if available
- `source_statement_file`: broker source file
- `notes`: why the row still needs review or why it was deferred

### `fund_tax_negative_deemed_distribution_review_<year>.csv`

Purpose:

- review and override support for annual reports with negative deemed distributed income

Columns:

- `report_key`: stable annual-report review key
- `ticker`
- `isin`
- `report_date`: date of the annual report under review
- `decision`: current decision or override choice
- `status`: workflow state of the review row
- `eligible_quantity_used`: quantity the workflow used in the current reconciliation attempt
- `quantity_held_on_report_date`: held quantity on the report date
- `candidate_payout_count`: number of candidate payout rows considered
- `candidate_payout_dates`: candidate payout dates inspected
- `candidate_payout_quantities`: candidate payout quantities inspected
- `candidate_payout_gross_rates_ccy`: candidate payout per-share gross rates
- `candidate_payout_gross_amounts_ccy`: candidate payout gross totals
- `matched_payout_count`: number of payouts matched by the current decision logic
- `matched_payout_dates`: matched payout dates
- `matched_payout_quantities`: matched payout quantities
- `matched_payout_gross_rates_ccy`: matched payout per-share gross rates
- `target_distribution_per_share_ccy`: target broker payout amount implied by the review logic
- `deemed_distributed_income_per_share_ccy`: annual report deemed-income amount per share
- `non_reported_distribution_per_share_ccy`: annual report non-reported distribution amount per share
- `creditable_foreign_tax_per_share_ccy`: per-share creditable foreign tax
- `basis_correction_per_share_ccy`: per-share acquisition-cost correction
- `basis_age_component_per_share_ccy`: per-share age component where present
- `basis_distribution_component_per_share_ccy`: per-share distribution component where present
- `capital_repayment_per_share_ccy`: per-share capital repayment component where present
- `withheld_tax_on_non_reported_distributions_per_share_ccy`: per-share withheld tax on non-reported distributions
- `source_file`: OeKB file used for the review row
- `notes`: reasoning, caveats, or override notes

### `reporting_funds_<year>_summary.md`

Purpose:

- human-readable filing and audit summary for the ETF workflow

Key sections:

- `Filing Inputs`
  ETF filing-oriented subtotals meant to help transfer the result into the tax form
- `ETF Income Events`
  detailed event rows that explain where the filing subtotals came from
- `Basis Adjustments`
  per-report `10289` basis deltas
- `Ledger State`
  end-of-run ETF lot state
- `Sales`
  ETF sale gain/loss total for the year

Important filing lines:

- `ETF distributions 27.5%`
  subtotal of `10286`, `10595`, and only those broker cash payout rows that remain the tax event because no OeKB report period covered the pay date
- `Ausschüttungsgleiche Erträge 27.5%`
  subtotal of OeKB `10287` rows
- `Creditable foreign tax`
  subtotal of OeKB `10288` rows

## Reporting-Funds State Files

### `fund_tax_ledger_<year>_final.csv`

Purpose:

- persisted ETF lot ledger carried forward into later years

Columns:

- `ticker`
- `isin`
- `lot_id`: stable ETF lot identifier
- `buy_date`
- `original_quantity`
- `remaining_quantity`
- `currency`
- `buy_price_ccy`
- `buy_fx_to_eur`
- `original_cost_eur`: remaining original EUR basis on the lot
- `cumulative_oekb_stepup_eur`: total ETF basis step-up still carried by the lot
- `adjusted_basis_eur`: `original_cost_eur + cumulative_oekb_stepup_eur`
- `status`: `open`, `partially_sold`, or `closed`
- `broker`
- `account_id`
- `notes`
- `last_adjustment_year`: last year that changed the lot basis
- `last_adjustment_reference`: reference of the last basis event
- `last_sale_date`
- `sold_quantity_ytd`
- `source_trade_id`
- `source_statement_file`

### `fund_tax_payout_state.csv`

Purpose:

- cross-year payout evidence store used to remember what happened to ETF payouts

Columns:

- `payout_key`: stable payout identifier across workflow runs
- `ticker`
- `isin`
- `ex_date`
- `pay_date`
- `quantity`
- `currency`
- `broker_gross_amount_ccy`
- `broker_net_amount_ccy`
- `broker_tax_amount_ccy`
- `source_tax_year`: year from which the broker evidence row originated
- `evidence_state`: payout evidence classification
- `status`: payout resolution state
- `resolved_tax_year`: tax year in which the row became resolved
- `resolved_by_report_year`: OeKB report year that resolved the row
- `resolved_by_report_file`: exact OeKB file that resolved the row
- `resolution_mode`: matching mode that resolved the row
- `action_id`: broker action id if available
- `source_statement_file`: source broker file
- `notes`: audit notes and resolution commentary
