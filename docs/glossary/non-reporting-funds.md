# Non-Reporting-Funds Glossary

This page documents the non-reporting-funds exit workflow artifacts and the important manual inputs that drive them.

Current workflow scope is the standalone Freedom-funds exit process described in [`scripts/non_reporting_funds_exit/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/non_reporting_funds_exit/README.md).

## File Inventory

| File / Pattern | Kind | Purpose | Row Grain |
| --- | --- | --- | --- |
| `data/input/non_reporting_funds_exit/non_reporting_funds_<year>_prices.csv` | Input | Manual annual price source for deemed-income calculation | one ticker-year input |
| `data/input/<person>/<year>/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv` | Input | Optional sale plan for later exit simulation | one planned sale |
| `data/output/<person>/non_reporting_funds_exit/non_reporting_funds_working_ledger.csv` | State output | End-of-run lot ledger after deemed-income step-up allocation and optional simulated sales | one fund lot |
| `data/output/<person>/non_reporting_funds_exit/non_reporting_funds_<year>_calc.csv` | Output | Annual deemed-income calculation per target fund | one ticker-year result |
| `data/output/<person>/non_reporting_funds_exit/non_reporting_funds_<year>_basis_adjustments.csv` | Output | Lot-level allocation of the annual deemed-income step-up | one lot-adjustment row |
| `data/output/<person>/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv` | Output | Later sale simulation output | one consumed lot slice per sale |
| `data/output/<person>/non_reporting_funds_exit/non_reporting_funds_exit_summary.md` | Output | Human-readable summary of calc, ledger, and sale results | one markdown report |

## Important Inputs

### `non_reporting_funds_<year>_prices.csv`

Purpose:

- manual annual price source used to calculate deemed income and per-share step-up

Columns:

- `tax_year`: calendar year the prices apply to
- `ticker`: fund symbol
- `isin`: fund identifier
- `trade_currency`: quote currency
- `first_price_ccy`: first relevant annual price in original currency
- `last_price_ccy`: last relevant annual price in original currency
- `notes`: source or justification for the chosen prices

### `non_reporting_funds_exit_sales.csv` input

Purpose:

- optional manual sale plan used to simulate a later exit

Columns:

- `ticker`: fund symbol
- `sale_date`: simulated sale date
- `quantity`: quantity to sell
- `sale_price_ccy`: per-share sale price in original currency

## Non-Reporting-Funds Outputs

### `non_reporting_funds_working_ledger.csv`

Purpose:

- lot ledger after trade reconstruction, split handling, annual deemed-income basis step-up, and any simulated sales

Columns:

- `ticker`
- `isin`
- `lot_id`: stable lot identifier inside the workflow
- `buy_date`
- `original_quantity`
- `remaining_quantity`
- `trade_currency`: original trading currency
- `buy_price_ccy`: per-share buy price in original currency
- `buy_commission_ccy`: original buy commission in currency; informational only
- `total_cost_ccy`: original buy cost including commission in currency; informational only
- `buy_fx`: buy-date FX used for EUR conversion
- `original_cost_eur`: remaining original EUR basis
- `cumulative_stepup_eur`: deemed-income step-up basis still attached to the lot
- `adjusted_basis_eur`: `original_cost_eur + cumulative_stepup_eur`
- `status`: lot state such as `open`, `partially_sold`, or `closed`
- `source_trade_id`
- `source_statement_file`
- `last_adjustment_year`: most recent year that changed basis
- `last_adjustment_type`: type of most recent basis change
- `last_adjustment_amount_eur`: EUR amount of most recent basis change
- `notes`

### `non_reporting_funds_<year>_calc.csv`

Purpose:

- main annual deemed-income calculation artifact

Columns:

- `tax_year`
- `event_date`: calculation date, usually year-end
- `ticker`
- `isin`
- `trade_currency`
- `first_price_ccy`: first annual price used in the deemed-income formula
- `last_price_ccy`: last annual price used in the deemed-income formula
- `shares_held_year_end`: quantity held at year-end that is eligible for the deemed-income calculation
- `deemed_amount_per_share_ccy`: deemed-income amount per share in original currency
- `deemed_amount_ccy`: total deemed-income amount in original currency
- `year_end_fx`: year-end FX used to convert deemed income into EUR
- `deemed_amount_eur`: total deemed-income amount in EUR
- `per_share_stepup_eur`: EUR basis increase per share that will be allocated into the ledger
- `notes`

### `non_reporting_funds_<year>_basis_adjustments.csv`

Purpose:

- audit trail showing how the annual deemed-income step-up was attached to specific lots

Columns:

- `tax_year`
- `event_date`
- `ticker`
- `lot_id`
- `eligible_quantity`: quantity on the lot that participated in the step-up
- `per_share_stepup_eur`: per-share EUR step-up amount
- `stepup_eur`: total step-up allocated to the lot
- `year_end_fx`: FX used for the annual EUR conversion
- `notes`

### `non_reporting_funds_exit_sales.csv` output

Purpose:

- sale simulation artifact for later exits

How to read it:

- taxable columns are the Austrian tax result
- informational columns show broker-style economic amounts and fees for audit only

Columns:

- `sale_date`
- `ticker`
- `quantity_sold`: total quantity in the simulated sale
- `sale_price_ccy`: per-share sale price in original currency
- `sale_fx`: sale-date FX
- `lot_id`
- `lot_buy_date`
- `quantity_from_lot`: quantity consumed from the lot
- `taxable_proceeds_eur`: Austrian gross sale proceeds in EUR
- `taxable_original_basis_eur`: original EUR basis allocated from the lot
- `taxable_stepup_basis_eur`: deemed-income step-up basis allocated from the lot
- `taxable_total_basis_eur`: `taxable_original_basis_eur + taxable_stepup_basis_eur`
- `taxable_gain_loss_eur`: `taxable_proceeds_eur - taxable_total_basis_eur`
- `informational_sale_proceeds_ccy`: original-currency sale proceeds shown for audit
- `informational_buy_cost_ccy_excl_fees`: original-currency buy cost excluding fees
- `informational_buy_commission_ccy_allocated`: allocated original-currency buy commission
- `informational_buy_cost_ccy_incl_fees`: original-currency buy cost including fees
- `notes`

### `non_reporting_funds_exit_summary.md`

Purpose:

- short human-readable rollup of the annual deemed-income result, current ledger state, and sale simulation result

How to read it:

- use it as a narrative checkpoint
- use the CSVs above as the actual audit and calculation artifacts
