# Manual E1kv Reconciliation 2025

## Inputs

Use these files:

- `data/output/eugene/tax_report_eugene_2025-01-01_2025-12-31/tax_report_eugene_2025-01-01_2025-12-31.pdf`
- `data/output/eugene/old_reporting_funds/2025/reporting_funds_2025_summary.md`
- `data/output/eugene/non_reporting_funds_exit/non_reporting_funds_exit_summary.md`

For exact foreign-tax credit support, also keep:

- `data/output/eugene/tax_report_eugene_2025-01-01_2025-12-31/artifacts/finanzonline/finanzonline_buckets__2025-01-01_2025-12-31.csv`

## Core Rule

Use foreign-tax credit only up to:

- `Austrian tax on the final combined post-loss 27.5% base`

Formula:

- `final_creditable_foreign_tax = min(sum_source_creditable_foreign_tax, final_post_loss_base * 0.275)`

## Step 1. Take Income Fields From The Three Workflows

From the core PDF:

- ordinary capital income: `840.1220`
- trade profit: `15.4604`
- trade loss: `-1350.8942`
- ETF distributions: `291.7672`

From reporting funds:

- ETF distributions: `21.453814`
- AGE: `62.424798`
- creditable foreign tax: `6.016800`

From non-reporting funds:

- AGE: `1089.5114`

Use `0` for non-reporting-funds distributions, trade result, and foreign-tax credit unless that workflow later produces them explicitly.

## Step 2. Build Final Filing Totals

- ordinary capital income = `840.1220`
- trade profit = `15.4604`
- trade loss = `-1350.8942`
- ETF distributions = `291.7672 + 21.453814 = 313.221014`
- AGE = `62.424798 + 1089.5114 = 1151.936198`

## Step 3. Build Final Post-Loss Base

Positive total:

- `840.1220 + 15.4604 + 313.221014 + 1151.936198 = 2320.739612`

Loss total:

- `1350.8942`

Remaining base:

- `2320.739612 - 1350.8942 = 969.845412`

Austrian tax ceiling:

- `969.845412 * 0.275 = 266.7075`

## Step 4. Use Source-Level Creditable Foreign Tax

Use only source amounts that are already `creditable`, not raw withholding.

For 2025:

- core pre-loss creditable foreign tax: `121.9311`
- reporting-funds creditable foreign tax: `6.0168`
- non-reporting-funds creditable foreign tax: `0.0`

Source sum:

- `121.9311 + 6.0168 = 127.9479`

Apply the ceiling:

- `min(127.9479, 266.7075) = 127.9479`

So the final usable creditable foreign tax is:

- `127.95`

## Important Note About The Core PDF Zero

The core PDF shows `Creditable foreign tax = 0.0` only because inside the core-only scope the losses fully wiped out the core positive base.

Once you add:

- reporting-funds income
- non-reporting-funds AGE

the combined base becomes positive again, so foreign-tax credit is usable again.

## Final 2025 Filing Note

```md
##### Einkünfte aus der Überlassung von Kapital (§ 27 Abs. 2; insbesondere Dividenden, Zinserträge aus Wertpapieren 27,5%) = 840.12

##### Einkünfte aus realisierten Wertsteigerungen von Kapitalvermögen (§ 27 Abs. 3; insbesondere Veräußerungsgewinne aus Aktien, Forderungswertpapieren und Fondsanteilen)
Überschüsse 27,5% = 15.46

Verluste = -1350.89


##### Einkünfte aus Investmentfonds und Immobilieninvestmentfonds
Ausschüttungen 27,5% = 291.7672 + 21.453814 = 313.22
Ausschüttungsgleiche Erträge 27,5% = 1089.5114 + 62.424798 = 1151.94

#### Anzurechnende ausländische
(Quellen) Steuer auf Einkünfte, die dem besonderen Steuersatz von 27,5% unterliegen = 121.9311 + 6.016800 = 127.95
```

## Audit Support

If asked during audit, show:

1. the three source summaries
2. the core `finanzonline_buckets` artifact
3. the final combined post-loss base calculation
4. that `127.9479 <= 266.7075`

That is the key proof that the final foreign-tax credit is within the allowed ceiling.
