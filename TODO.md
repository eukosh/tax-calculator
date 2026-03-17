# TODO

### Handle Non-Reporting Funds Outside Core Trade Logic

For now, non-reporting funds should be handled as a **special workflow outside the normal trade calculator**.

Recommended approach:

1. Keep using the existing app for **raw dividend/distribution extraction and withholding-tax extraction** where it is still useful.
2. Add an **ad hoc script or small standalone workflow** for non-reporting funds such as `SCHD` and `TLT`.
3. Do **not** change the general trade logic of the app just to support this one-off Austrian fund-tax treatment.

#### 2025 filing workflow for non-reporting funds

Recommended practical workflow:

1. Use broker/app output to identify actual 2025 cash distributions and foreign withholding tax.
2. Calculate the 2025 default deemed amount manually or via ad hoc script:
   - higher of:
     - `90% x (last price - first price in calendar year)`
     - `10% x last price`
   - multiplied by shares held on `2025-12-31`
3. Use the result for Austrian filing:
   - actual fund distributions -> manual filing workflow / correct fund-distribution field
   - deemed amount -> manual filing workflow / correct agE field
4. Record the **gross deemed amount**, not the tax paid, as a basis increase for the open lots held on `2025-12-31`.

#### Basis-adjustment rule for non-reporting funds

Important implementation rule:

- the basis increase is the **gross deemed amount**
- not the tax paid on that deemed amount

For future sales, the clean model is:

- maintain a per-lot ledger
- for every lot that was still open on the relevant year-end, add the per-share EUR step-up
- later sell under FIFO using those adjusted EUR lot bases

#### Explicit plan for non-reporting funds

Non-reporting funds such as `SCHD` and `TLT` should have:

- a **separate script**
- their **own separate ledger/artifact files**
- no attempt to force them into the same long-lived standardized ledger used for normal long-term reporting-fund handling

Reason:

- the non-reporting-fund yearly inputs are special to that regime
- the relevant year-end price fields are one-off calculation inputs, not durable lot state
- the current intention is to exit these positions soon, not maintain them as a long-lived portfolio workflow

Recommended file concept for non-reporting funds:

1. Year-specific calculation file
   - example: `non_reporting_funds_2025_calc.csv`
   - contains the inputs and outputs for the 2025 deemed-income calculation
   - this is where fields such as `first price in year` and `last price in year` belong

2. Year-specific basis-adjustment / sale-support file
   - example: `non_reporting_funds_2025_basis_adjustments.csv`
   - records how the 2025 deemed amount was converted into EUR step-ups attached to the open lots
   - used later when preparing the 2026 sale reporting in the 2027 return

3. Optional short-lived working ledger for the exit workflow
   - example: `non_reporting_funds_working_ledger.csv`
   - only needed if the sale calculation is easier with a temporary per-lot file
   - can remain separate from the long-lived standardized fund ledger

Important:

- yearly price inputs such as `first price in year` and `last price in year` should **not** be stored in the standardized long-lived ledger
- those belong only in the annual non-reporting-fund calculation file
- the standardized long-lived ledger is reserved for durable lot/basis state

#### Recommended storage model

Preferred approach:

- keep local files as the live working source of truth
- back up frozen yearly snapshots together with other tax documents in cloud storage such as Dropbox

This section applies to the **long-lived standardized ledger** used for durable fund-tax tracking.

It is primarily intended for reporting funds and any future workflow where fund positions remain open for multiple years.

Recommended minimal file set:

1. `fund_tax_ledger_working.csv`
   - the live file that is edited when working on a tax return
   - contains the current state of all still-relevant fund lots and their cumulative EUR tax basis adjustments

2. `fund_tax_ledger_<YEAR>_final.csv`
   - frozen end-of-cycle snapshot after the filing work for that year is complete
   - examples:
     - `fund_tax_ledger_2025_final.csv`
     - `fund_tax_ledger_2026_final.csv`
   - this is the archival state you can later restore if the working file is changed incorrectly

3. Optional yearly artifact files
   - examples:
     - `fund_tax_events_2025.csv`
     - `fund_tax_deemed_income_2025.csv`
     - `fund_tax_sales_2026.csv`
   - these are not the source of truth for remaining basis
   - they are audit/supporting files showing how yearly adjustments or sales were derived

Recommended folder concept:

- working files locally during return preparation
- yearly frozen snapshots and artifacts archived with the tax package after filing
- Dropbox used as backup/archive, not as the live transactional database

##### Meaning of the files

`fund_tax_ledger_working.csv`

- current live state
- can be updated multiple times while preparing the return
- should eventually be copied/frozen into the yearly final snapshot

`fund_tax_ledger_<YEAR>_final.csv`

- the state of the ledger after all events relevant for that year have been recorded
- becomes the starting point for the next year
- examples:
  - `fund_tax_ledger_2025_final.csv`
    - contains all open lots after 2025 fund-tax adjustments
    - includes the 2025 deemed-income step-up for non-reporting funds
  - `fund_tax_ledger_2026_final.csv`
    - contains all open lots after 2026 buys, 2026 sales, and any 2026 fund-tax adjustments
    - lots sold in 2026 should already be reflected there via reduced or zero remaining quantity

There is no need for a separate `after_sales` file if yearly `final` snapshots are maintained consistently.

##### Required ledger columns

At minimum the ledger should include:

- `ticker`
- `isin` if available
- `lot_id`
- `buy_date`
- `original_quantity`
- `remaining_quantity`
- `trade_currency`
- `buy_price_ccy`
- `total_cost_ccy`
- `buy_fx`
- `original_cost_eur`
- `cumulative_stepup_eur`
- `adjusted_basis_eur`
- `status`
  - suggested values: `open`, `partially_sold`, `closed`
- `notes`

Recommended additional audit columns:

- `source_broker`
- `source_trade_id`
- `source_statement_file`
- `last_adjustment_year`
- `last_adjustment_type`
- `last_adjustment_amount_eur`

##### Optional yearly event files

If you want supporting artifacts, keep them separate from the ledger.

Examples:

`fund_tax_deemed_income_2025.csv`

- ticker
- event date
- first price in year
- last price in year
- shares at year end
- deemed amount in trade currency
- FX used
- deemed amount in EUR
- tax paid
- per-share EUR step-up

`fund_tax_sales_2026.csv`

- ticker
- sale date
- shares sold
- sale price in trade currency
- sale proceeds in EUR
- FIFO lots consumed
- original EUR basis consumed
- step-up EUR consumed
- realized gain/loss in EUR

##### Source-of-truth rule

Important:

- for the long-lived standardized workflow, the **ledger** is the source of truth for remaining lots and adjusted basis
- yearly event files are supporting calculations only
- yearly final snapshots are the archival checkpoints
- for non-reporting funds, the annual calc and basis-adjustment artifacts may remain separate and do not need to be folded into the standardized long-lived ledger if those positions are expected to be exited soon

##### Why this model is preferred

- avoids changing the core trade engine for a narrow Austrian fund-tax problem
- keeps the bookkeeping understandable and inspectable
- makes yearly restoration possible if the working file is damaged
- fits the low-frequency annual workflow
- is easy to archive together with filed returns, tax decisions, and supporting reports

Reason:

- this avoids polluting the general trade engine
- it provides a durable audit trail
- it is less error-prone than one-off spreadsheet edits over multiple years

Spreadsheet-only handling is acceptable for a very small number of positions, but the preferred future-safe path is a persistent ledger file.

#### FX rule for non-reporting funds

Use EUR conversion at the time of the legally relevant event:

- buys: convert each lot to EUR on the **buy date**
- non-reporting-fund deemed amount: convert to EUR on the **year-end deemed-income date**
- sales: convert proceeds to EUR on the **sale date**

Do not keep the full lifecycle in USD and convert only at the end.

### Priority 3: Handle Reporting Funds Later

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
- one working ledger file
- one yearly frozen final snapshot
- optional yearly OeKB import/calculation artifacts kept alongside it

This is different from the short-term non-reporting-fund workflow, which can stay in separate purpose-built files.

#### Important modeling rule for reporting funds

When reporting funds are handled later, use:

- OeKB-published tax data
- OeKB acquisition-cost correction data

Do not rely only on a naive “add agE” shortcut without checking the OeKB correction amounts.

## Current Recommended Practical Workflow

Short version of the intended order:

1. Fix REIT classification bug.
2. Keep non-reporting funds (`SCHD`, `TLT`) outside the core trade engine.
3. For non-reporting funds, use a separate script and separate yearly ledger/artifact files for deemed-income and basis-step-up logic.
4. For reporting funds, build a long-lived standardized ledger with durable EUR lot-basis tracking.
5. Leave reporting-fund automation for later, using manual OeKB lookup first.
