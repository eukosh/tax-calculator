# Manual E1kv Reconciliation Guide

Use this as the final manual reconciliation step when you combine:

- the core app report
- the reporting-funds ETF workflow
- the non-reporting-funds workflow

The document is year-agnostic. `2025` below is only a worked example.

## Optional Helper Script

If you want a root-level filing note generated from literal values instead of editing markdown manually, run:

```bash
poetry run python -m scripts.manual_e1kv_input.cli --tax-year 2025
```

The script:

- prompts for the source values you want to use
- keeps `KZ 189` and `KZ 899` separate
- applies the foreign-tax ceiling rule automatically
- writes `manual_e1kv_input_<year>.md` in the project root by default

## Inputs

For a given filing year, collect:

- the core app PDF report
- the reporting-funds yearly summary
- the non-reporting-funds ETF summary, if used
- the non-reporting-funds REIT summary, if used

For foreign-tax-credit support, also keep:

- the core `finanzonline_buckets` artifact

Example for 2025:

- `data/output/eugene/tax_report_eugene_2025-01-01_2025-12-31/tax_report_eugene_2025-01-01_2025-12-31.pdf`
- `data/output/eugene/reporting_funds/2025/reporting_funds_2025_summary.md`
- `data/output/eugene/non_reporting_funds_exit/freedom/non_reporting_funds_exit_summary.md`
- `data/output/eugene/non_reporting_funds_exit/ibkr/ibkr_reit_exit_summary.md`
- `data/output/eugene/tax_report_eugene_2025-01-01_2025-12-31/artifacts/finanzonline/finanzonline_buckets__2025-01-01_2025-12-31.csv`

## Core Rule

Use foreign-tax credit only up to:

- `Austrian tax on the final combined post-loss 27.5% base`

Operational formula:

- `final_creditable_foreign_tax = min(sum_source_creditable_foreign_tax, final_post_loss_base * 0.275)`

This assumes the source-level foreign-tax amounts you use are already the `creditable` amounts, not raw withheld tax.

## What To Combine

Take these filing fields from the source workflows:

- ordinary capital income 27.5%
- trade profits 27.5%
- trade losses
- ETF distributions 27.5%
- REIT distributions 27.5%
- deemed distributed income 27.5%
- domestic dividends in loss offset (`KZ 189`)
- Austrian KESt on domestic dividends (`KZ 899`)
- source-level creditable foreign tax

Keep these rules:

- `KZ 189` and `KZ 899` come only from reporting funds
- `KZ 899` stays separate from foreign-tax credit
- `10289` basis corrections are not entered separately in E1kv
- non-reporting funds only contribute AGE; their distributions are handled by the core app
- use `0` for any source field that a workflow does not produce

## Reconciliation Steps

1. Sum source values by filing field.

2. Build the positive 27.5% base:

- ordinary capital income
- trade profits
- ETF distributions
- deemed distributed income
- domestic dividends in loss offset (`KZ 189`)

3. Subtract trade losses:

- `final_post_loss_base = max(positive_total - abs(loss_total), 0)`

4. Compute the foreign-tax ceiling:

- `foreign_tax_ceiling = final_post_loss_base * 0.275`

5. Sum the source-level creditable foreign tax amounts.

6. Final usable foreign-tax credit:

- `min(sum_source_creditable_foreign_tax, foreign_tax_ceiling)`

7. Keep `KZ 899` separate. It is Austrian KESt on domestic dividends, not foreign tax.

## Why The Core PDF Can Show Zero Foreign-Tax Credit

The core PDF can legitimately show:

- `Creditable foreign tax = 0`

when the core-only losses fully wipe out the core-only positive 27.5% base.

After you add reporting-funds or non-reporting-funds income, the combined base may become positive again. In that case, foreign-tax credit can become usable again at the combined filing level.

## Worked Example: 2025

### Example Inputs

From the core PDF:

- ordinary capital income: `707.0305`
- trade profit: `15.4604`
- trade loss: `-1350.8942`
- ETF distributions: `291.7672`
- REIT distributions: `133.0915`
- source-level creditable foreign tax: `121.9313`

From reporting funds:

- ETF distributions: `21.453814`
- AGE: `62.424798`
- domestic dividends (`KZ 189`): `0.000000`
- Austrian KESt on domestic dividends (`KZ 899`): `0.000000`
- creditable foreign tax: `6.016800`

From non-reporting funds (ETFs):

- AGE: `1089.5114`

From non-reporting funds (REITs):

- AGE: `300.4264`

### Example Totals

- ordinary capital income = `707.0305`
- trade profit = `15.4604`
- trade loss = `-1350.8942`
- ETF/REIT distributions = `291.7672 + 133.0915 + 21.453814 = 446.312514`
- AGE = `62.424798 + 1089.5114 + 300.4264 = 1452.362598`
- domestic dividends (`KZ 189`) = `0`
- Austrian KESt on domestic dividends (`KZ 899`) = `0`

Positive total:

- `707.0305 + 15.4604 + 446.312514 + 1452.362598 + 0 = 2621.166012`

Post-loss base:

- `2621.166012 - 1350.8942 = 1270.271812`

Foreign-tax ceiling:

- `1270.271812 * 0.275 = 349.3247`

Source-level creditable foreign tax:

- `121.9313 + 6.0168 = 127.9481`

Final usable foreign-tax credit:

- `min(127.9481, 349.3247) = 127.9481`

### Example Filing Note

```md
##### Einkünfte aus der Überlassung von Kapital (§ 27 Abs. 2; insbesondere Dividenden, Zinserträge aus Wertpapieren 27,5%) = 707.03

##### Einkünfte aus realisierten Wertsteigerungen von Kapitalvermögen (§ 27 Abs. 3; insbesondere Veräußerungsgewinne aus Aktien,
Forderungswertpapieren und Fondsanteilen)
Überschüsse 27,5% = 15.46

Verluste = -1350.89


##### Einkünfte aus Investmentfonds und Immobilieninvestmentfonds
Ausschüttungen 27,5% = 291.7672 + 133.0915 + 21.453814 = 446.31
Ausschüttungsgleiche Erträge 27,5% = 62.424798 + 1089.5114 + 300.4264 = 1452.36
Inländische Dividenden im Verlustausgleich (KZ 189) = 0.00
KESt auf inländische Dividenden (KZ 899) = 0.00

#### Anzurechnende ausländische
(Quellen) Steuer auf Einkünfte, die dem besonderen Steuersatz von 27,5% unterliegen = min(121.9313 + 6.016800, 349.3247) = 127.95
```

## Audit Support

If asked during audit, keep:

1. the source summaries
2. the core `finanzonline_buckets` artifact
3. the combined post-loss base calculation
4. the foreign-tax ceiling calculation
5. the separate `KZ 189` and `KZ 899` totals, if non-zero

That is the minimum proof chain for how the final filing inputs were derived.
