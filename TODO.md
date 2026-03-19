# TODO

### Handle Reporting Funds Later

Reporting funds should be solved only after:

1. direct REIT cleanup
2. non-reporting-fund workflow is stable

For reporting funds, the recommended near-term plan is:

- keep them mostly outside the current trade engine as well
- use OeKB data manually first
- only later decide whether to automate scraping/import

Two acceptable near-term options:

1. Manual / semi-manual:
   - use the app for ordinary broker-side dividend and withholding extraction where useful
   - look up OeKB data manually
   - keep a spreadsheet or ledger of annual fund tax adjustments
   - calculate later sale results manually from adjusted EUR basis

2. Small ledger-based extension:
   - maintain a local trade-history / fund-tax-adjustment file as a mini database
   - append trades over time
   - record annual OeKB adjustments there
   - compute future sale basis from that ledger

Given the currently small number of reporting funds, the manual or semi-manual route is acceptable for now.

#### Explicit plan for reporting funds

Reporting funds should be handled with a **long-lived standardized ledger layout**.

Reason:

- they are more likely to stay in the portfolio for multiple years
- yearly OeKB-based adjustments must accumulate cleanly over time
- this is the better place for durable lot state and cumulative EUR basis corrections

Target model:

- one standardized ledger structure
- one yearly frozen final snapshot
- optional yearly OeKB import/calculation artifacts kept alongside it

This is different from the short-term non-reporting-fund workflow, which can stay in separate purpose-built files.

#### Reporting-fund ledger files

The long-lived reporting-fund workflow should use the following file model:

- `fund_tax_ledger_<YEAR>_final.csv`
- optional yearly support files such as:
  - `fund_tax_oekb_adjustments_<YEAR>.csv`
  - `fund_tax_events_<YEAR>.csv`
  - `fund_tax_sales_<YEAR>.csv`

Purpose of each file:

- `fund_tax_ledger_<YEAR>_final.csv`
  - frozen year-end / filing snapshot
  - archival checkpoint after that tax year's reporting-fund adjustments are fully applied
  - becomes the starting point for the next year's processing run

- `fund_tax_oekb_adjustments_<YEAR>.csv`
  - yearly evidence file for imported or manually entered OeKB values
  - should capture the values actually used to update the ledger for that year

- `fund_tax_events_<YEAR>.csv`
  - optional yearly event log if needed
  - can record buys, sales, splits, mergers, ticker changes, ISIN changes, or manual corrections

- `fund_tax_sales_<YEAR>.csv`
  - optional yearly sale artifact
  - useful if realized sales are easier to audit separately than by reconstructing them later from the ledger alone

Important naming simplification:

- there is **no need** for a separate `after_sales` snapshot naming convention
- if the annual workflow is followed consistently, `fund_tax_ledger_<YEAR>_final.csv` is enough
- for example, by the time 2027 filing work starts, all 2026 sales should already be reflected in `fund_tax_ledger_2026_final.csv`

#### Reporting-fund processing workflow

The intended workflow is immutable and year-based:

1. start from `fund_tax_ledger_<PREVIOUS_YEAR>_final.csv`
2. load it in memory
3. apply the current year's buys, sells, and corporate actions
4. read the current year's OeKB data
5. apply the current year's OeKB-based basis corrections to the relevant open lots
6. write `fund_tax_ledger_<CURRENT_YEAR>_final.csv`

Example:

- after filing 2025, save `fund_tax_ledger_2025_final.csv`
- when preparing 2026 / filing in 2027:
  - read `fund_tax_ledger_2025_final.csv`
  - apply 2026 buys and sells
  - apply 2026 OeKB adjustments
  - write `fund_tax_ledger_2026_final.csv`

There is no need for a persistent `working` file if the script can handle the full transformation in memory.

#### What belongs in the reporting-fund ledger

The standardized reporting-fund ledger should store **durable lot state**, not one-off market-data inputs.

Minimum durable columns:

- `ticker`
- `isin`
- `lot_id`
- `buy_date`
- `original_quantity`
- `remaining_quantity`
- `currency`
- `buy_price_ccy`
- `buy_fx_to_eur`
- `original_cost_eur`
- `cumulative_oekb_stepup_eur`
- `adjusted_basis_eur`
- `status`

Useful additional columns:

- `broker`
- `account_id`
- `notes`
- `last_adjustment_year`
- `last_adjustment_reference`
- `last_sale_date`
- `sold_quantity_ytd`

Core rule:

- the reporting-fund ledger is an **EUR tax ledger**
- original buys are stored in EUR using buy-date FX
- annual OeKB-based basis corrections are stored in EUR using the relevant Austrian tax-event date
- future sales are computed in EUR using sale-date FX

#### Important modeling rule for reporting funds

When reporting funds are handled later, use:

- OeKB-published tax data
- OeKB acquisition-cost correction data

Do not rely only on a naive “add agE” shortcut without checking the OeKB correction amounts.
