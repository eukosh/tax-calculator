# FinanzOnline Loss Offset and Foreign Tax Credit

## Scope

This document describes the logic used for the FinanzOnline helper and tax estimate in this repo.

It covers:

- raw foreign tax withheld
- preliminary creditable foreign tax before loss offset
- favorable loss allocation
- estimated Austrian tax at 27.5%

It does not cover:

- donations
- other deductions outside capital income
- unrelated tax-return fields

## Core Fields

- `Capital income 27.5% (dividends/interest)`
  Sum of ordinary capital-income buckets.
- `Trade profits 27.5%`
  Sum of positive trade-profit buckets.
- `Trade losses 27.5% (enter as negative)`
  Sum of negative trade-loss buckets.
- `ETF distributions 27.5%`
  Sum of ETF distribution buckets.
- `Foreign tax withheld`
  Raw foreign tax actually withheld by brokers.
- `Creditable foreign tax`
  Foreign tax that remains creditable after treaty cap and favorable loss allocation.

## Bucket Model

The helper works on normalized buckets.

Each bucket has:

- `amount_eur`
- `withheld_foreign_tax_eur`
- `creditable_foreign_tax_before_loss_eur`
- `category`

Current categories:

- `ordinary_income`
- `etf_distribution`
- `trade_profit`
- `trade_loss`

## Preliminary Foreign Tax Credit

For each positive foreign-taxed bucket:

```text
preliminary_credit_i = min(withheld_tax_i, treaty_cap_i)
```

For the currently supported dividend/distribution rows, the treaty cap is generally:

```text
treaty_cap_i = 15% * gross_income_i
```

Raw withheld foreign tax:

```text
raw_withheld_total = sum(withheld_tax_i)
```

Preliminary credit before loss offset:

```text
preliminary_credit_total = sum(preliminary_credit_i)
```

## Favorable Loss Allocation

This repo uses `favorable` loss allocation by default.

Reason:

- it is closer to the beneficial allocation described in the official guidance
- it avoids the unnecessary conservatism of the proportional simplification
- it is still implementable with stable deterministic code

### Rule

For each positive bucket:

```text
credit_ratio_i = preliminary_credit_i / amount_i
```

Losses are allocated to positive buckets in ascending `credit_ratio_i` order.

That means:

1. zero-credit income is consumed first
2. low-credit income is consumed next
3. high-credit income is consumed last

For a bucket hit by loss:

```text
remaining_amount_i = max(amount_i - allocated_loss_i, 0)
remaining_credit_i = preliminary_credit_i * remaining_amount_i / amount_i
```

Final creditable foreign tax:

```text
final_creditable_foreign_tax = sum(remaining_credit_i)
```

## Proportional Simplification

The officially accepted simplification is:

```text
credit_after_loss
= preliminary_credit_total
- (preliminary_credit_total / total_positive_income * offset_losses)
```

Equivalent:

```text
credit_after_loss = preliminary_credit_total * (1 - offset_losses / total_positive_income)
```

This method is accepted, but it can be more conservative than favorable allocation.

## Estimated Tax Base and Estimated Austrian Tax

Total tax base:

```text
total_tax_base = max(sum(all bucket amounts), 0)
```

Estimated Austrian tax:

```text
estimated_austrian_tax = max(total_tax_base * 27.5% - final_creditable_foreign_tax, 0)
```

## Why Detailed Buckets Matter

Favorable allocation should not be performed on overly coarse summary rows.

Bad example:

- one summary row mixes zero-credit and 15%-credit income

That can distort the allocation order.

Current implementation therefore uses:

- detailed dividend/distribution buckets for IBKR
- detailed dividend/distribution buckets for Freedom
- coarse summary buckets only for zero-credit or low-risk rows such as:
  - bonds
  - Revolut interest
  - trade profit/loss summaries
  - Wise cashback summaries

## Practical Consequences

- If losses can be fully absorbed by zero-credit income, foreign tax credit is preserved.
- If losses must hit foreign-taxed income, creditable foreign tax is reduced.
- If all positive income has the same credit ratio, favorable and proportional results converge.
- If income mixes zero-credit and foreign-taxed buckets, favorable allocation is usually better than proportional.

## Sources

- BMF: https://www.bmf.gv.at/themen/steuern/sparen-veranlagen/kapitalertraege-im-engeren-sinn.html
- Findok guidance: https://findok.bmf.gv.at/findok/iwg/83/83598/83598.1.pdf
