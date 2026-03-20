# Non-Reporting Funds Exit Workflow

This folder contains a standalone workflow for Austrian non-reporting funds held in Freedom Finance.

Current scope:
- `SCHD.US`
- `TLT.US`

It does **not** handle ordinary fund distributions for filing. Those stay in the normal/core tax workflow.

## What This Script Does

The workflow does 3 things:

1. Rebuilds your Freedom buy lots from the lifetime statement
2. Calculates the `2025-12-31` deemed-income step-up for non-reporting funds
3. Simulates later FIFO sales using the adjusted EUR basis

Important tax treatment used here:
- the deemed-income basis increase uses the **gross** deemed amount in EUR
- buy/sell fees are kept only as informational fields
- buy/sell fees are **not** used in Austrian taxable basis/proceeds

Operating rule:
- keep the input Freedom statement frozen for this workflow; when you later sell, add the actual sale manually to the sale-plan CSV instead of replacing the statement with a newer one

## Files You Need

### 1. Lifetime Freedom statement

Default path for `eugene`:

```text
../../data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json
```

Clickable link:

[`data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json`](../../data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26%2023_59_59_2026-03-17%2023_59_59_all.json)

This should cover the whole lot history from inception through the intended exit period.

### 2. Annual price input file

Shared path:

[`data/input/non_reporting_funds_exit/non_reporting_funds_2025_prices.csv`](../../data/input/non_reporting_funds_exit/non_reporting_funds_2025_prices.csv)

Columns:

- `tax_year`
- `ticker`
- `isin`
- `trade_currency`
- `first_price_ccy`
- `last_price_ccy`
- `notes`

What to edit:
- replace the current working/default prices with the final prices you want to rely on for filing

Current file is only a starting point.

### 3. Optional sale plan

Default path for `eugene`:

[`data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv`](../../data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv)

Columns:

- `ticker`
- `sale_date`
- `quantity`
- `sale_price_ccy`

Leave it empty if you only want the 2025 deemed-income artifacts.

Fill it in if you want a sale simulation.

Example:

```csv
ticker,sale_date,quantity,sale_price_ccy
SCHD.US,2026-03-17,375,30.87
TLT.US,2026-03-17,43,87.21
```

## How To Run

The commands below are intended to be copy-pasted and then edited only where your paths or person differ.

From repo root:

```bash
poetry run python -m scripts.non_reporting_funds_exit.cli
```

This uses:
- `--person eugene`
- the shared annual price file
- the single statement JSON found under `data/input/eugene/2025/non_reporting_funds_exit/`
- `data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv`
- `data/output/eugene/non_reporting_funds_exit`

To run it for another person:

```bash
poetry run python -m scripts.non_reporting_funds_exit.cli --person oryna
```

If a person folder contains multiple statement JSON files, pass `--statement-path` explicitly.

You can also pass explicit paths:

```bash
poetry run python -m scripts.non_reporting_funds_exit.cli \
  --person eugene \
  --statement-path "data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json" \
  --price-input-path "data/input/non_reporting_funds_exit/non_reporting_funds_2025_prices.csv" \
  --sale-plan-path "data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv" \
  --output-dir "data/output/eugene/non_reporting_funds_exit"
```

## Output Files

Outputs are written to:

```text
../../data/output/<person>/non_reporting_funds_exit/
```

### `non_reporting_funds_working_ledger.csv`

This is the lot ledger after:
- lot reconstruction
- split handling
- 2025 deemed-income step-up allocation

Use this as the main working state for the exit workflow.

### `non_reporting_funds_2025_calc.csv`

Per ticker:
- shares held at `2025-12-31`
- first and last annual prices
- deemed amount in trade currency
- year-end FX
- deemed amount in EUR
- per-share EUR step-up

This is the main 2025 calculation artifact.

### `non_reporting_funds_2025_basis_adjustments.csv`

Shows how the 2025 EUR deemed amount was attached to specific open lots.

This is the audit trail for the basis step-up.

### `non_reporting_funds_exit_sales.csv`

Sale simulation output.

Shows, per consumed lot:
- taxable EUR proceeds
- taxable EUR original basis
- taxable EUR step-up basis
- taxable EUR gain/loss
- informational non-tax columns for buy cost and fees

### `non_reporting_funds_exit_summary.md`

Short human-readable summary of:
- 2025 deemed-income results
- current adjusted ledger totals
- sale simulation summary

## Recommended Workflow

### For 2025 filing work

1. Open [`non_reporting_funds_2025_prices.csv`](../../data/input/non_reporting_funds_exit/non_reporting_funds_2025_prices.csv)
2. Replace the placeholder prices with your final verified prices
3. Run the CLI
4. Review:
   - [`non_reporting_funds_2025_calc.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_2025_calc.csv) under your selected person's output folder
   - [`non_reporting_funds_2025_basis_adjustments.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_2025_basis_adjustments.csv) under your selected person's output folder
   - [`non_reporting_funds_working_ledger.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_working_ledger.csv) under your selected person's output folder

### For later sale work

1. Fill [`non_reporting_funds_exit_sales.csv`](../../data/input/eugene/2025/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv)
2. Run the CLI again
3. Review:
   - [`non_reporting_funds_exit_sales.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_exit_sales.csv) under your selected person's output folder
   - [`non_reporting_funds_working_ledger.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_working_ledger.csv) under your selected person's output folder
   - [`non_reporting_funds_exit_summary.md`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_exit_summary.md) under your selected person's output folder

## How This Connects To The Core App

The core Freedom workflow now has an `include_trades` switch in [`process_freedom_statement(...)`](../../src/providers/freedom.py).

If you want the standalone workflow to be the only source of truth for Freedom sale calculations in a given run/year:
- set `include_freedom_trades = False` in [`main.py`](../../main.py)

That will keep Freedom distributions/dividends in the core app while excluding Freedom trade rows.

## Sanity Checks

For the current statement, the important expected checkpoints are:
- `SCHD.US` shares at `2025-12-31`: `330`
- `TLT.US` shares at `2025-12-31`: `43`
- the later `2026-02-04` `SCHD` lot of `45` shares should **not** receive the 2025 step-up

If those are wrong, stop and inspect the inputs before relying on the outputs.

## Caveats

- The annual price CSV is manual input. If the prices are wrong, the deemed-income result will be wrong.
- This workflow is intentionally narrow and one-off. It is not a general fund-tax engine.
- It assumes the Freedom lifetime statement contains the full relevant trade history.

## What To Enter In E1kv

For the non-reporting-fund lump-sum result from this script:

- take the `deemed_amount_eur` total from [`non_reporting_funds_2025_calc.csv`](../../data/output/eugene/non_reporting_funds_exit/non_reporting_funds_2025_calc.csv)
- enter that amount in `E1kv` under Kennzahl `937`

Meaning of Kennzahl `937`:
- `ausschüttungsgleiche Erträge aus Fondsanteilen`, when the fund units are held on an `ausländisches Depot` and there is no Austrian KESt withholding agent

For your case, this is the field for the 2025 non-reporting-fund deemed-income amount for `SCHD.US` and `TLT.US`.

Important:

- do **not** enter the lot-by-lot basis step-up anywhere separately in `E1kv`
- the basis step-up exists only so that your later sale calculation uses a higher tax basis
- the ledger and basis-adjustment CSVs are your audit trail for that later sale, not a separate filing line item for 2025

Also separate this clearly from actual cash distributions:

- actual fund cash distributions on a foreign depot belong separately in Kennzahl `898`
- this script does **not** calculate those distributions; they stay in your normal/core workflow

If FinanzOnline shows labels instead of just Kennzahlen, search for Kennzahl `937`.

Official source basis:

- [`E1kv` form instructions](https://formulare.bmf.gv.at/service/formulare/inter-Steuern/pdfs/2015/E1kv.pdf?open=download): actual fund distributions on foreign depots go to Kennzahl `898`, and `ausschüttungsgleiche Erträge` on foreign depots go to Kennzahl `937`
- [`InvFR 2018` BMF guidance](https://findok.bmf.gv.at/findok/iwg/74/74706/74706.1.pdf): the `ausschüttungsgleiche Erträge` are captured in `E1kv` under Kennzahl `937`, while the acquisition-cost adjustment is used later for sale-basis calculation

## Documentation

Workflow overview:

- top-level repo doc: [`README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/README.md)

Schema and artifact glossary:

- glossary index: [`docs/glossary/README.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/README.md)
- non-reporting-funds glossary: [`docs/glossary/non-reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/non-reporting-funds.md)
