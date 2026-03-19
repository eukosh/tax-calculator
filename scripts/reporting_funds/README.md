# Reporting Funds Workflow

This folder contains the standalone workflow for Austrian reporting-fund ETFs held at IBKR.

## Scope

The core app no longer owns ETF tax treatment:

- ETF trade P/L is excluded from the core IBKR trade summary
- ETF cash/dividend rows are excluded from the core IBKR cash/dividend path
- `scripts/reporting_funds` owns:
  - long-lived ETF lot state
  - OeKB income classification
  - OeKB basis corrections
  - broker payout reconciliation
  - FIFO ETF sales using adjusted EUR basis

## Chosen Tax Model

This workflow uses the following operating rule:

- OeKB is the Austrian tax source of truth for reporting-fund ETF classification and basis correction
- IBKR broker data is the source of truth for whether cash actually hit the account, and on which payout date
- later OeKB annual reports may resolve an older payout, but they must not move that payout into the later tax year

## Authoritative Algorithm

For Austrian reporting ETFs, this workflow applies the following logic.

### 1. Broker Cash Payouts

Broker statements decide:

- whether a cash payout actually happened
- the broker cash payout date
- the broker cash payout amount

Broker cash payouts are emitted as `broker_dividend_event`.

If a broker payout:

- matches an OeKB same-year `Ausschüttungsmeldung`, the payout is resolved by that distribution report
- is covered by an OeKB annual-report period and annual `10595` is present, the payout may be resolved by that annual report
- is not covered by any OeKB annual-report period and does not match a distribution report, the broker cash payout remains the tax event and is not blocked as unresolved

### 2. OeKB Report Data

OeKB reports decide Austrian ETF tax classification fields whenever those fields are present in the report.

In practice:

- deemed distributed income comes from OeKB `10287`
- basis correction comes from OeKB `10289`
- creditable foreign tax comes from OeKB `10288`
- reported cash distribution classification comes from OeKB `10286`
- non-reported distribution reconciliation comes from OeKB `10595`

The event timing depends on the report type:

- for `Ausschüttungsmeldung`, use the report's payout and ex-date fields
- for `Jahresmeldung`, apply the annual-report values on the OeKB `Meldedatum`, using the quantity held on the relevant eligibility date

So the rule is not "annual reports always decide these fields"; it is "OeKB decides these fields, and annual reports are the fallback or annual-cleanup source when no same-year distribution report covers the event."

An annual report remains authoritative for Austrian ETF tax classification even if the underlying economic cash payout happened in an earlier period.

### 3. Broker Withholding Tax

For reporting ETFs:

- broker withholding is preserved only as audit evidence in `broker_tax_amount_ccy`
- broker withholding does not populate Austrian creditable foreign tax
- Austrian creditable foreign tax comes only from OeKB `10288`
- if `10288` appears in an `Ausschüttungsmeldung`, it is applied with distribution-report timing
- if `10288` appears in a `Jahresmeldung`, it is applied on `Meldedatum`

### 4. Historical Jan-2025 Style Annual Reports

If an annual report is published in the target tax year but its covered business year ends before the historical lock date:

- the old broker cash payout stays in the old cash year
- the annual report is still applied in the target tax year as annual cleanup
- this annual cleanup may create `10287`, `10288`, and `10289` effects in the target tax year
- the workflow does not reopen pre-2025 filed tax years or rewrite opening basis before the 2025 workflow start

### 5. Negative Deemed Distributed Income

If annual `10287` is negative:

- historical pre-lock reports are auto-applied as annual cleanup using quantity held on report date
- non-historical cases go through the manual review override workflow
- overrides may still explicitly choose `ignore_as_frozen_history`, `apply_full`, `apply_partial`, or `unresolved_block`

Practical consequence:

- unresolved ETF broker payouts are never silently ignored
- unresolved post-2024 payouts block the run by default
- next-year annual reports may be read as lookahead evidence to resolve prior-year payouts, especially for `10595`
- negative deemed distributed income is handled through a review-and-override workflow

## Historical Lock

`2024` is treated as already filed historical context.

- 2024 ETF payouts are loaded from broker XML only for audit and matching context
- they are marked as locked history
- the workflow does not reopen 2024 tax treatment
- the workflow does not retro-apply 2024 OeKB basis corrections

This means:

- `2025` is the first year this ETF workflow is treated as authoritative
- opening 2025 lot basis is not rewritten using pre-2025 OeKB corrections

## Inputs

### Shared Annual IBKR Tax XML

Pass a file, wildcard, or directory that resolves to one or more annual Flex exports.

The annual tax XML must include:

- `CashTransactions`
- `ChangeInDividendAccruals`

The workflow reads only ETF rows from these sections.

Current intended usage:

- 2024 annual file for historical lock context
- 2025 annual file for 2025 broker payouts

Examples:

- [ibkr_20250101_20241231.xml](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/data/input/eugene/2024/ibkr_20250101_20241231.xml)
- [ibkr_20250101_20260101.xml](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/data/input/eugene/2025/ibkr_20250101_20260101.xml)

Important:

- exact duplicate raw XML rows are dropped immediately after parse
- the loader supports overlapping annual files
- broker-side matching uses `actionID` first
- symbol is not a safe identifier; real data includes drift such as `IDTLz` vs `IDTL`

### Negative Deemed Distributed Income Overrides

If an annual OeKB report contains negative deemed distributed income, the workflow writes:

- `fund_tax_negative_deemed_distribution_review_<year>.csv`

and expects manual decisions in:

- `fund_tax_negative_deemed_distribution_overrides.csv`

Default location:

- `data/output/<person>/reporting_funds/fund_tax_negative_deemed_distribution_overrides.csv`

Optional CLI argument:

- `--negative-deemed-income-overrides-path`

Override CSV columns:

- `report_key`
- `decision`
- `eligible_quantity`
- `notes`

Supported decisions:

- `ignore_as_frozen_history`
- `apply_full`
- `apply_partial`
- `unresolved_block`

Rules:

- pre-2025 historical annual reports whose covered period ends before the historical lock date are applied as annual cleanup using the quantity held on the report date
- `apply_full` applies the annual-report negative deemed distributed income, foreign-tax credit, and basis correction using the reviewed quantity
- `apply_partial` does the same, but requires `eligible_quantity`
- `unresolved_block` leaves the report unapplied and blocks finalization after the review CSV is written

This is a rare fallback path, not normal carryforward state:

- most runs do not need any override file
- use it only if the workflow writes a negative-review CSV row with unresolved status and asks for a manual decision

### ETF Trade History XML

Pass a trade-history source containing raw ETF BUY/SELL rows.

This can be:

- a single XML file
- a wildcard
- a directory of XML files

Important:

- the initial bootstrap must include raw ETF BUY/SELL history, not only closed lots
- overlapping files are supported
- exact duplicate raw trade rows are removed after merge
- FX conversion rows with `assetCategory="CASH"` are ignored

### OeKB Root Directory

Pass the OeKB root, not only a single-year directory.

Expected structure:

- `data/input/oekb/2025/`
- `data/input/oekb/2026/`

The workflow reads:

- all required reports from `data/input/oekb/<tax_year>/`
- optional annual lookahead reports from `data/input/oekb/<tax_year + 1>/` up to `resolution_cutoff_date`

Matching is by parsed `ISIN`, not by filename.

The parser supports both:

- reduced/private-investor OeKB CSV exports
- full OeKB CSV exports

When a full export contains the same tax code in multiple sections, the workflow explicitly prefers the
`Kennzahlen ESt-Erklärung Privatanleger (je Anteil)` section for `10286`, `10287`, `10595`, `10288`, and `10289`.

The workflow uses these OeKB fields:

- `ISIN`
- `Währung`
- `Meldedatum`
- `Jahresmeldung`
- `Ausschüttungsmeldung`
- `Ausschüttungstag`
- `Ex-Tag`
- `Meldezeitraum Beginn`
- `Meldezeitraum Ende`
- `Geschäftsjahres-Beginn`
- `Geschäftsjahres-Ende`
- `10286`
- `10287`
- `10595`
- `10288`
- `10289`

## Broker Matching Rules

One logical ETF payout event is built from:

- `ChangeInDividendAccrual`
- matching ETF `CashTransaction`

Matching precedence:

- primary key: `actionID`
- required sanity checks:
  - same `ISIN`
  - same payout date
  - compatible `exDate`
  - gross amount within a small tolerance
- fallback only if `actionID` is missing:
  - `ISIN + exDate + payDate + quantity + grossRate`

Accrual lifecycle interpretation:

- `Po` = accrual posting / entitlement side
- `Re` = finalized payout-side reversal
- the cash row is the authoritative broker cash posting

If cash and accrual materially disagree, the run fails.

## Resolution Rules

### Same-Year Distribution Reports

If a payout matches an `Ausschüttungsmeldung` for the same `ISIN` and payout date:

- the payout is resolved in the payout year
- OeKB drives Austrian tax classification
- basis correction still follows the OeKB report

### Annual `10595`

If a payout does not have a same-year distribution report:

- it remains unresolved first
- annual reports may resolve it via `10595`
- a later annual report can resolve an older payout, but the payout remains in the original cash year

The workflow uses:

- `Meldezeitraum` first
- `Geschäftsjahres-Beginn/Ende` as fallback

If `10595 > 0` and there is no usable report-period metadata, the run fails rather than guessing.

### Historical Lock Interaction

Historical lock does not override the authoritative algorithm above.

It means only:

- pre-2025 broker payouts are not reopened as broker-year tax events
- opening 2025 basis is not rewritten from older OeKB history
- but a target-year annual report may still create target-year annual cleanup entries

## Outputs

State:

- `data/output/<person>/reporting_funds/fund_tax_ledger_<year>_final.csv`
- `data/output/<person>/reporting_funds/fund_tax_payout_state.csv`

Run artifacts:

- `data/output/<person>/reporting_funds/<year>/fund_tax_income_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_basis_adjustments_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_sales_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_payout_resolution_events_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/fund_tax_negative_deemed_distribution_review_<year>.csv`
- `data/output/<person>/reporting_funds/<year>/reporting_funds_<year>_summary.md`

The summary markdown contains:

- filing-oriented totals for ETF distributions and OeKB creditable foreign tax
- diagnostic workflow totals
- aggregated open-lot ledger state
- next reporting-period carryforward inputs
- manual-override fallback notes for negative deemed-distribution edge cases

Clarification notes are written into the CSV outputs and the summary markdown to explain:

- same-year distribution matches
- next-year annual `10595` resolutions
- broker cash payouts kept because no OeKB annual-report period covered the pay date
- historical locked rows
- ignored pre-2025 references
- unresolved blocking payouts
- cash/accrual mismatch handling

Current manual-review trigger:

- annual reports with negative deemed distributed income

## CLI

From repo root:

```bash
poetry run python -m scripts.reporting_funds.cli \
  --person eugene \
  --tax-year 2025 \
  --ibkr-tax-xml-path 'data/input/eugene/*/ibkr_20250101_202[46]*.xml' \
  --ibkr-trade-history-path data/input/eugene/ibkr/trades \
  --oekb-root-dir data/input/oekb \
  --resolution-cutoff-date 2026-04-30
```

Useful flags:

- `--resolution-cutoff-date YYYY-MM-DD`
- `--historical-lock-start-date 2025-01-01`
- `--allow-unresolved-payouts`

Default behavior is strict:

- unresolved post-2024 ETF payouts block the run
