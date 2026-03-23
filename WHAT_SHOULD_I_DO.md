# What Should I Do?

Very simple step-by-step guide for the parts of this repo that actually need explanation:

- IBKR ordinary dividends and stock/ADR/REIT trades in the core app
- Austrian reporting-fund ETFs
- non-reporting funds

Everything else in the repo is simpler and can usually be ignored unless you know you need it.

## Start Here

Getting ready to file taxes for last year?

Ask yourself these three questions:

1. Did you receive dividends or do any stock/ADR/REIT trades in IBKR?
2. Did you hold EU-domiciled reporting-fund ETFs?
3. Did you hold non-reporting funds like `SCHD.US` or `TLT.US` in Freedom?

If the answer is `no` to a section, skip that section.

## Part 1: Core IBKR Run

Use this if you had:

- ordinary IBKR dividends
- IBKR stock trades
- IBKR ADR trades
- IBKR REIT trades

Do not use this part for reporting-fund ETFs. Those are handled separately below.

### Step 1: Export the IBKR files

In IBKR Flex Queries, export:

- tax XML coverage for the full filing year
- full raw trade-history XML files for the account

In practice you want:

- `Tax_automation` style tax XML with closed lots, cash transactions, dividends, withholding tax, bonds, etc.
- `Tax_automation_trades` style raw trade-history XML files with `TradeConfirm` rows

Put them under:

- yearly tax XML: `data/input/<person>/<year>/`
- raw trade history: `data/input/<person>/ibkr/trades/`

Important:

- `ibkr_input_path` is used for dividends, withholding tax, and bonds
- because of that, `ibkr_input_path` must cover the full filing year
- this can be:
  - one yearly XML file
  - a wildcard matching several overlapping XML files
  - a directory
  - a Python list of XML files in `main.py`
- if your IBKR exports are split into chunks, that is fine, but you must supply all relevant chunks

### Step 2: Special rule for Eugene

If you are `eugene`, also make sure this file exists:

- `data/input/eugene/ibkr/austrian_opening_state_2024-05-01.csv`

Reason:

- Eugene became Austrian tax resident on `2024-05-01`
- pre-move broker buy prices are not the Austrian tax basis for pre-move holdings
- the opening-state CSV is the Austrian starting basis

If you are `oryna` and all relevant buys are post-move, there should be no opening-state file for this part.

### Step 3: Configure `main.py`

Open:

- [`main.py`](main.py)

Set:

- `person`
- `ibkr_input_path`
- `ibkr_trade_history_path`
- `reporting_start_date`
- `reporting_end_date`

Set the opening-state fields like this:

For Eugene:

```python
austrian_opening_state_path = "data/input/eugene/ibkr/austrian_opening_state_2024-05-01.csv"
authoritative_start_date = date(2024, 5, 1)
```

For Oryna:

```python
austrian_opening_state_path = None
authoritative_start_date = None
```

Important:

- `ibkr_trade_history_path` is required
- `ibkr_input_path` must include all IBKR tax XML files needed to cover the filing year for dividends/cash/bonds
- `authoritative_start_date` only makes sense together with an opening-state snapshot

### Step 4: Run the core app

From repo root:

```bash
poetry run python main.py
```

### Step 5: Review the important output files

Look under:

- `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/`

The most important files are:

- `stock_tax_sales__<start>_<end>.csv`
  Use this to see the realized taxable stock/ADR/REIT sale results.
- `stock_tax_position_events__<start>_<end>.csv`
  Use this as the chronological audit trail of stock position changes.
- `stock_tax_position_state_full__<start>_<end>.csv`
  Use this to see the full year-end moving-average position state.
- `dividends_country_agg__<start>_<end>.csv`
  Use this to review ordinary non-ETF IBKR dividends by country.
- `ibkr_summary__<start>_<end>.csv`
  Use this as the final IBKR rollup.

Also look under:

- `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/finanzonline/`

Important helper files there:

- `finanzonline_buckets__<start>_<end>.csv`
- `finanzonline_estimate__<start>_<end>.csv`

These are the helper outputs for entering the final numbers into FinanzOnline.

### Step 6: Simple sanity check

For stock sales:

- `stock_tax_sales` should contain one row per realized sale execution
- `stock_tax_position_events` should show a clean chronological audit trail per security
- `stock_tax_position_state_full` should match your expected year-end remaining positions and average basis

If that does not look right, stop and inspect the inputs before filing.

## Part 2: Reporting-Fund ETFs

Use this if you held Austrian reporting-fund ETFs, for example EU-domiciled ETFs that need OeKB handling.

Do not rely on the core app for these.

### Step 1: Collect the broker files

You need:

- the yearly IBKR tax XML for the filing year
- as much historical IBKR tax XML as you have
- raw IBKR ETF trade history

Put them under:

- yearly tax XML: `data/input/<person>/<year>/`
- raw trade history: `data/input/<person>/ibkr/trades/`

Strong recommendation:

- give the ETF workflow broad historical IBKR tax XML coverage, not just the target year
- give it full raw ETF trade history

Reason:

- payout resolution can need older broker evidence
- negative `10287` reconciliation can need older payout rows
- broader history avoids unnecessary manual-review blockers

### Step 2: Collect the OeKB reports

You need OeKB reports for the filing year.

In practice, keep the OeKB root complete for all relevant years, not just the current one.

Put them under:

- `data/input/oekb/<year>/`

Very important:

- if you already filed earlier years, keep the old OeKB downloads too
- if you are missing them, redownload them
- do not only collect the current year and ignore history

### Step 3: Special rule for Eugene

If you are `eugene`, the ETF workflow can also use the Austrian opening-state snapshot at move-in:

- `data/input/eugene/ibkr/austrian_opening_state_2024-05-01.csv`

This matters when bootstrapping Austrian ETF state after the move.

### Step 4: Run the ETF workflow

Normal yearly run example:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2025 \
  --ibkr-tax-xml-path data/input/eugene/2025/ibkr_20250101_20260101.xml \
  --historical-ibkr-tax-xml-path 'data/input/eugene/202[34]/*.xml' \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb
```

Bootstrap / move-in carryforward example for Eugene:

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

### Step 5: Review the important ETF output files

Look under:

- `data/output/<person>/reporting_funds/<year>/`
- plus the carryforward files directly under `data/output/<person>/reporting_funds/`

The most important files are:

- `reporting_funds_<year>_summary.md`
  Start here. It gives the filing-oriented ETF totals.
- `fund_tax_income_events_<year>.csv`
  The actual ETF income events used for Austrian tax logic.
- `fund_tax_basis_adjustments_<year>.csv`
  OeKB basis corrections that affect future ETF sales.
- `fund_tax_sales_<year>.csv`
  ETF sale allocations.
- `fund_tax_events_<year>.csv`
  The chronological ETF position-event audit trail.
- `fund_tax_state_<year>_final.csv`
  The ETF carryforward state snapshot.
- `fund_tax_payout_state.csv`
  The cross-year payout-resolution state.

If the workflow asks for manual review, also inspect:

- `fund_tax_payout_evidence_review_<year>.csv`
- `fund_tax_negative_deemed_distribution_review_<year>.csv`

### Step 6: What number do I actually use?

Start from:

- `reporting_funds_<year>_summary.md`

For example, the current 2025 Eugene summary gives:

- `ETF distributions 27.5%`
- `Ausschüttungsgleiche Erträge 27.5%`
- `Domestic dividends in loss offset (KZ 189)`
- `Austrian KESt on domestic dividends (KZ 899)`
- `Creditable foreign tax`

Use the summary as your filing input for the reporting-fund ETF section.

Important:

- `10289` basis corrections are not entered separately in the tax return
- they only increase or decrease future ETF sale basis
- `10759` and `10760` stay separate from the other ETF filing fields

## Part 3: Non-Reporting Funds

Use this only for non-reporting funds, currently the narrow Freedom workflow such as:

- `SCHD.US`
- `TLT.US`

### Step 1: Prepare the input files

You need:

- the full Freedom lifetime statement JSON
- the annual price input CSV
- optionally a sale-plan CSV if you want to simulate a sale

Files:

- statement JSON under `data/input/<person>/<year>/non_reporting_funds_exit/`
- annual prices in `data/input/non_reporting_funds_exit/non_reporting_funds_<year>_prices.csv`
- optional sale plan in `data/input/<person>/<year>/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv`

### Step 2: Fill the annual price CSV

Open:

- `data/input/non_reporting_funds_exit/non_reporting_funds_<year>_prices.csv`

Replace the placeholder prices with the actual prices you want to rely on for filing.

If these prices are wrong, the result is wrong.

### Step 3: Run the workflow

Basic run:

```bash
poetry run python -m scripts.non_reporting_funds_exit.cli
```

Explicit-path example:

```bash
poetry run python -m scripts.non_reporting_funds_exit.cli \
  --person eugene \
  --statement-path "data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json" \
  --price-input-path "data/input/non_reporting_funds_exit/non_reporting_funds_2025_prices.csv" \
  --sale-plan-path "data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv" \
  --output-dir "data/output/eugene/non_reporting_funds_exit"
```

### Step 4: Review the important output files

Look under:

- `data/output/<person>/non_reporting_funds_exit/`

Important files:

- `non_reporting_funds_2025_calc.csv`
  Main deemed-income result for the year.
- `non_reporting_funds_2025_basis_adjustments.csv`
  Shows how the deemed-income step-up was attached to lots.
- `non_reporting_funds_working_ledger.csv`
  Shows the adjusted lot ledger after the annual step-up.
- `non_reporting_funds_exit_sales.csv`
  Only relevant if you supplied a sale plan.
- `non_reporting_funds_exit_summary.md`
  Simple human-readable summary.

### Step 5: What do I enter in the tax return?

For the non-reporting-fund deemed-income amount:

- take the `deemed_amount_eur` total from `non_reporting_funds_<year>_calc.csv`
- enter that amount in `E1kv` under Kennzahl `937`

Important:

- do not enter the basis step-up separately
- the step-up exists only so that a later sale uses a higher tax basis

Also remember:

- actual fund cash distributions on a foreign depot belong separately in Kennzahl `898`
- this workflow does not calculate those distributions

## Very Short Version

If you want the shortest possible checklist:

1. Export the IBKR yearly tax XML and raw IBKR trade-history XML files.
2. Put them into the right `data/input/<person>/...` folders.
3. If you hold EU-domiciled reporting-fund ETFs, collect all relevant OeKB files too.
4. If you are Eugene, make sure the Austrian opening-state CSV exists and is wired into the places that need it.
5. Run:
   - `poetry run python main.py`
   - `poetry run python -m scripts.reporting_funds.cli ...` if you have reporting-fund ETFs
   - `poetry run python -m scripts.non_reporting_funds_exit.cli ...` if you have non-reporting funds
6. Review:
   - core IBKR outputs under `data/output/<person>/tax_report_.../artifacts/ibkr/`
   - ETF outputs under `data/output/<person>/reporting_funds/`
   - non-reporting-fund outputs under `data/output/<person>/non_reporting_funds_exit/`
7. Use the summary / helper outputs for filing, not random intermediate files.

## If You Get Lost

Open these files:

- [`README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/README.md)
- [`scripts/reporting_funds/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/reporting_funds/README.md)
- [`scripts/non_reporting_funds_exit/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/scripts/non_reporting_funds_exit/README.md)
- [`docs/glossary/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/README.md)
