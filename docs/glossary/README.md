# Repository Glossary

This folder is the schema and artifact reference for the repository.

Use it when you need to know:

- which files exist
- what each file is for
- what one row represents
- what specific columns mean

Use README files elsewhere in the repo for:

- workflow boundaries
- tax-model explanations
- CLI usage

Use glossary pages here for:

- file inventories
- state vs output distinctions
- column definitions

## Subsystem Map

- Core app glossary:
  [`core-app.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/core-app.md)
- Reporting-funds glossary:
  [`reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/reporting-funds.md)
- Non-reporting-funds glossary:
  [`non-reporting-funds.md`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/docs/glossary/non-reporting-funds.md)

## Common Conventions

- `date` columns are calendar dates in `YYYY-MM-DD`.
- `datetime` columns are timestamp-like execution or event times.
- `ticker` is the human-readable security symbol used by the workflow.
- `isin` is the stable security identifier used for matching and reconciliation.
- `currency` or `trade_currency` is the original transaction/report currency.
- `quantity` is the amount relevant to the row itself.
- `remaining_quantity` is the unsold quantity still left on a persisted lot.
- `basis` means acquisition-cost basis for Austrian tax logic.
- `proceeds` means sale or payout amount before subtracting basis.
- `gain_loss` means `proceeds - basis`.
- `notes` is a human-readable audit field; it is not a stable machine key.

## Naming Rules

- Files under `data/output/<person>/...` are generated artifacts or persisted state.
- `ledger` files describe lot state.
- `summary` files are human-readable rollups, usually markdown.
- `reconciliation` or `review` files are audit/supporting artifacts, not filing outputs.
- Core stock `trades_tax_df` is tax-only.
- Core stock broker matching lives in `trades_reconciliation`.
- ETF sales and non-reporting-fund sales still expose `taxable_stepup_basis_eur` because step-up basis is part of those workflows.
