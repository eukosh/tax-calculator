# SCHD / TLT Austrian Tax Note for 2025 Filing and 2026 Sale

This note is a repo-backed explanation of the `SCHD.US` and `TLT.US` scenario discussed in chat.

It is meant to answer 2 questions:

1. What does the fully by-the-book Austrian route look like for 2025?
2. Why does `TLT` show a much larger 2026 tax loss after the 2025 step-up than the simple `313 USD` economic loss?

## Scope and Assumptions

This note uses the data currently present in the repo:

- Freedom statement for the whole lifetime of account: /Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/data/input/eugene/2025/non_reporting_funds_exit/freedom_2024-03-26 23_59_59_2026-03-17 23_59_59_all.json
- ECB/OeNB USD/EUR rates:
  [`data/input/currencies/raw_exchange_rates.csv`](/Users/eugene.kosharnyi/Desktop/projects/personal/tax-calculator/data/input/currencies/raw_exchange_rates.csv)

Assumed sale prices for the 2026 exit:

- `SCHD`: `30.87 USD`
- `TLT`: `87.14 USD`

Assumed sale FX for the comparison:

- nearest verified official reference available locally: `1 EUR = 1.1478 USD` on `2026-03-16`

Repo-backed year-end holdings used here:

- `SCHD`: `330` shares at `2025-12-31`
- `TLT`: `43` shares at `2025-12-31`

Important:

- this note does **not** include the extra `45 SCHD` shares mentioned later in chat, because those 2026 buys are not present in the repo
- this is a working calculation note, not legal advice

## The Key Distinction

There are 4 different numbers that are easy to mix up:

1. **Economic sale loss/profit**
   This is the intuitive broker-style result in USD.
2. **2025 deemed amount**
   For a non-reporting fund, Austria taxes a default annual amount even if you do not sell.
3. **2025 tax paid**
   This is `27.5%` of the deemed amount.
4. **2026 tax gain/loss after basis step-up**
   On the later sale, you increase basis by the **gross deemed amount**, not by the tax paid.

That last point is exactly why `TLT` can show a much larger tax loss in 2026 than the simple `-313 USD` economic loss.

## TLT in Plain English

Your intuitive USD view was:

- raw sale loss now: about `-313.68 USD`
- 2025 deemed-tax cash paid: about `103.07 USD`
- practical downside if you cannot use the larger 2026 loss elsewhere: about `416.75 USD`

That intuition is fine for a "cash pain" view.

But it is **not** the same as the 2026 tax-loss line.

For the 2026 sale result, Austria does this:

- start with your normal basis
- add the **gross 2025 deemed amount** to basis
- only then compute the 2026 sale gain/loss

For `TLT`, the gross 2025 deemed amount is about `374.79 USD`, not `103.07 USD`.

So in simplified USD terms:

- raw sale loss: `-313.68 USD`
- extra basis from 2025 deemed amount: `-374.79 USD`
- sale result after step-up: about `-688.47 USD`

The EUR tax accounting result is even larger in absolute terms because Austrian tax uses historical EUR basis and EUR sale proceeds, so FX also moves the tax result.

## Repo-Backed Numbers

### 2025 actual cash distributions

| Ticker | Gross distributions | Gross distributions | Creditable foreign tax | Austrian tax due |
|---|---:|---:|---:|---:|
| `SCHD` | `170.11 USD` | `147.24 EUR` | `22.09 EUR` | `18.41 EUR` |
| `TLT` | `163.60 USD` | `144.53 EUR` | `0.00 EUR` | `39.75 EUR` |

Total 2025 tax on actual 2025 distributions: `58.15 EUR`

### 2025 default deemed amount for a Nichtmeldefonds

Formula used:

- higher of:
  - `90% x (last price - first price in the year)`, or
  - `10% x last price`

For the repo-backed 2025 year:

| Ticker | 2025 start price | 2025 end price | Shares on 2025-12-31 | Deemed amount | Deemed amount | 2025 tax paid | Basis increase for 2026 sale |
|---|---:|---:|---:|---:|---:|---:|---:|
| `SCHD` | `27.32 USD` | `27.43 USD` | `330` | `905.19 USD` | `770.37 EUR` | `211.85 EUR` | `770.37 EUR` |
| `TLT` | `87.33 USD` | `87.16 USD` | `43` | `374.79 USD` | `318.97 EUR` | `87.72 EUR` | `318.97 EUR` |

Total 2025 deemed tax: `299.57 EUR`

## 2026 Sale Comparison

### Broker-style / economic intuition in USD

Using the repo-backed lot history and the assumed sale prices:

| Ticker | Shares sold | USD basis | Avg cost | Sale proceeds | Raw sale result |
|---|---:|---:|---:|---:|---:|
| `SCHD` | `330` | `8,889.30 USD` | `26.9373 USD` | `10,187.10 USD` | `+1,297.80 USD` |
| `TLT` | `43` | `4,060.70 USD` | `94.4350 USD` | `3,747.02 USD` | `-313.68 USD` |

### 2026 sale result after applying the 2025 step-up

This is the table that explains the confusing `TLT` number.

| Ticker | Raw 2026 sale result | 2025 basis step-up | 2026 sale result after step-up |
|---|---:|---:|---:|
| `SCHD` in USD intuition | `+1,297.80 USD` | `905.19 USD` | `+392.61 USD` |
| `TLT` in USD intuition | `-313.68 USD` | `374.79 USD` | `-688.47 USD` |

And in the EUR tax calculation used for the Austrian comparison:

| Ticker | 2026 EUR proceeds | Historical EUR basis | 2025 EUR step-up | 2026 tax result after step-up |
|---|---:|---:|---:|---:|
| `SCHD` | `8,875.33 EUR` | `7,811.10 EUR` | `770.37 EUR` | `+293.85 EUR` |
| `TLT` | `3,264.52 EUR` | `3,727.64 EUR` | `318.97 EUR` | `-782.09 EUR` |

So the `-782.09 EUR` number is:

- **not** your economic loss
- **not** the cash tax paid in 2025
- it is the **2026 Austrian tax-loss line after adding the gross 2025 deemed amount to basis**

## Practical Downside: What Actually Hurts

There are two ways to think about the downside of doing TLT "by the book".

### 1. Cash-pain view

If you do not care about loss utilization for a moment:

- raw economic sale loss: about `313.68 USD`
- extra 2025 tax cash outflow: about `103.07 USD`

So your intuitive cash-pain number is about:

- `416.75 USD`

This is the intuition you described in chat, and it is reasonable as a cash-flow view.

### 2. Austrian tax-accounting view

In tax accounting, the 2025 deemed amount creates:

- immediate 2025 tax paid
- larger 2026 tax loss because basis is stepped up by the **gross** deemed amount

So the extra `TLT` downside is not necessarily permanent:

- if you can use that larger 2026 loss against other taxable capital gains, the 2025 tax pain can be recovered later
- if you cannot use that extra loss, then the 2025 deemed tax is closer to a deadweight cost

For `TLT`, the repo-backed 2025 deemed tax is about:

- `87.72 EUR`

That is the best single-number estimate of the extra immediate pain from doing the 2025 route fully by the book.

## Whole Scenario Summary

Using the repo-backed `330 SCHD / 43 TLT` scenario:

### If 2025 annual non-reporting-fund rule is ignored

- 2025 tax on actual cash distributions: `58.15 EUR`
- 2026 sale tax at assumed prices: `292.66 EUR`
- total: `350.82 EUR`

### If 2025 is done fully by the book

- 2025 tax on actual cash distributions: `58.15 EUR`
- 2025 deemed tax: `299.57 EUR`
- 2026 sale tax at assumed prices after step-up: `80.81 EUR`
- total: `438.53 EUR`

Incremental total cost of the by-the-book route in this repo-backed scenario:

- `87.72 EUR`

That incremental pain is basically the `TLT` issue.

`SCHD` mostly shifts tax from the 2026 sale back into 2025.

## Short Answer

If you ask:

> Why is TLT shown as `-782.09 EUR` when my actual position loss is only about `-313 USD`?

The answer is:

- because `-782.09 EUR` is the **2026 Austrian tax-loss line after increasing basis by the full 2025 deemed amount**
- you should not read that as "I lost 782 EUR economically"
- your simpler "cash pain" view of about `313 USD` sale loss plus `103 USD` 2025 tax is a valid intuition for immediate downside

The two numbers describe different things.
