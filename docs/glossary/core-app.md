# Core App Glossary

This page documents the current core-app file schemas that matter most for the IBKR stock/ADR/REIT moving-average flow.

Current glossary coverage inside the core app:

- IBKR artifacts written under `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/`
- the Austrian opening-state CSV used by authoritative stock runs

It does not yet try to fully document every non-IBKR broker artifact from the core app.

## File Inventory

| File / Pattern | Kind | Purpose | Row Grain |
| --- | --- | --- | --- |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/stock_tax_sales__<start>_<end>.csv` | Output | Realized Austrian tax results for stock/ADR/REIT sales | one sale execution |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/stock_tax_position_events__<start>_<end>.csv` | Output | Chronological audit trail of stock position events and resulting moving-average state | one position event |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/stock_tax_position_state_full__<start>_<end>.csv` | State output | End-of-run stock moving-average position state | one security position |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/dividends_country_agg__<start>_<end>.csv` | Output | Country aggregate for non-ETF IBKR cash dividends | one issuer-country plus currency aggregate |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/bonds_tax_df__<start>_<end>.csv` | Output | Bond/corporate-action realized PnL detail | one realized bond event |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/bonds_tax_country_agg_df__<start>_<end>.csv` | Output | Bond realized PnL aggregate | one issuer-country plus currency aggregate |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/ibkr_summary__<start>_<end>.csv` | Output | Final IBKR summary bucket set used by the PDF/report flow | one summary bucket |
| `data/input/<person>/ibkr/austrian_opening_state_<date>.csv` | Input | Austrian-authoritative opening stock position state at move-in date | one opening security position |

## Important Inputs

### `austrian_opening_state_<date>.csv`

Purpose:

- provides Austrian-authoritative opening stock/ADR/REIT state for positions already held at move-in
- replaces pre-move broker acquisition basis for Austrian tax purposes

How it is used:

- authoritative stock runs load this file first
- the runtime then replays only post-start raw IBKR trades on top of that opening state

Columns:

- `snapshot_date`: date at which the opening Austrian state is frozen
- `asset_class`: stock-like IBKR subtype such as `COMMON`, `ADR`, `REIT`, or `ETF`
- `ticker`: security symbol
- `isin`: security identifier
- `quantity`: opening quantity at the snapshot date
- `currency`: trade currency
- `base_cost_total_eur`: base EUR cost before later basis adjustments
- `basis_adjustment_total_eur`: cumulative post-acquisition basis adjustments already embedded in the opening state
- `total_basis_eur`: total Austrian basis at the snapshot date
- `average_basis_eur`: per-share moving-average basis at the snapshot date
- `status`: position state at snapshot time
- `broker`: broker/source label
- `notes`: explanation of how the lot was created
- `basis_method`: why the Austrian basis was chosen, for example `move_in_fmv_reset`
- `source_file`: source file used to derive the state

Relationship to outputs:

- `stock_tax_position_state_full` is the same position-state concept after post-move trades have been applied
- `stock_tax_position_events` shows the chronological replay that produced that state
- `stock_tax_sales` is the sale-only realized tax view

## Core IBKR Outputs

### `stock_tax_sales`

Purpose:

- tax-only stock/ADR/REIT sale artifact
- shows the realized taxable basis, proceeds, and gain/loss for each sale execution

How to read it:

- one row corresponds to one sale execution
- this file is a concise realized-sales view derived from the full event log

Columns:

- `sale_date`: trade date of the sale
- `sale_trade_id`: raw broker-side sale identifier from trade history
- `ticker`: sold security
- `isin`: sold security ISIN
- `quantity_sold`: full sale quantity of the execution
- `sale_price_ccy`: per-share sale price in original currency
- `sale_fx`: sale-date FX used to convert gross sale proceeds into EUR
- `taxable_proceeds_eur`: Austrian taxable gross sale proceeds in EUR allocated to this row
- `taxable_original_basis_eur`: base EUR basis realized on the sale
- `taxable_stepup_basis_eur`: basis-adjustment component realized on the sale
- `taxable_total_basis_eur`: total taxable basis realized on the sale
- `taxable_gain_loss_eur`: `taxable_proceeds_eur - taxable_total_basis_eur`
- `notes`: short explanation of the tax interpretation used for the row
- `buy_fee_eur_total`: total informational buy-fee amount attached to the lot
- `cumulative_oekb_stepup_eur`: cumulative ETF-style step-up basis; currently `0` for core stock lots
- `adjusted_basis_eur`: `original_cost_eur + cumulative_oekb_stepup_eur`
- `status`: `open`, `partially_sold`, or `closed`
- `broker`: broker/source label
- `account_id`: source account id
- `notes`: lot-level audit notes
- `last_adjustment_year`: last basis-adjustment year
- `last_adjustment_reference`: reference for the last basis change
- `last_sale_date`: last sale date that consumed this lot
- `sold_quantity_ytd`: quantity sold during the tracked period
- `source_trade_id`: source trade id used to create the lot
- `source_statement_file`: source file used to create the lot
- `asset_class`: IBKR subtype such as `COMMON`, `ADR`, `REIT`
- `broker_buy_date`: original broker acquisition date for audit
- `broker_buy_price_ccy`: original broker acquisition price for audit
- `broker_buy_fx_to_eur`: original broker acquisition FX for audit
- `broker_original_cost_eur`: original broker gross basis in EUR for audit
- `broker_buy_fee_eur`: original broker buy fee in EUR for audit
- `austrian_basis_method`: how Austrian basis was chosen
- `austrian_basis_price_ccy`: Austrian basis price per share in original currency
- `austrian_basis_fx_to_eur`: Austrian basis FX
- `basis_origin`: `snapshot` or `post_move_buy`
- `buy_datetime`: source execution timestamp for the lot when available

### `dividends_country_agg`

Purpose:

- aggregate non-ETF dividend result by issuer country and currency

Columns:

- `issuer_country_code`: issuing-country code used for grouping
- `currency`: original cash currency
- `profit_total`: original-currency dividend total
- `dividends_euro_total`: gross dividends converted to EUR
- `dividends_euro_net_total`: gross dividends less Austrian KESt net effect
- `withholding_tax_euro_total`: foreign withholding tax in EUR
- `kest_gross_total`: Austrian KESt before netting foreign withholding
- `kest_net_total`: Austrian KESt still payable after offsetting foreign withholding where applicable

### `bonds_tax_df`

Purpose:

- detail-level bond/corporate-action realized result rows

Columns:

- `report_date`: event/report date used for taxation
- `isin`: security identifier
- `issuer_country_code`: issuing-country code
- `currency`: original event currency
- `proceeds`: original-currency proceeds/amount relevant to the event
- `realized_pnl`: original-currency realized PnL
- `realized_pnl_euro`: realized PnL converted to EUR
- `realized_pnl_euro_net`: EUR realized PnL after Austrian KESt effect
- `kest_gross`: gross Austrian KESt on the EUR amount
- `kest_net`: net Austrian KESt on the EUR amount

### `bonds_tax_country_agg_df`

Purpose:

- aggregate bond realized result by issuer country and currency

Columns:

- `issuer_country_code`
- `currency`
- `profit_total`
- `profit_euro_total`
- `profit_euro_net_total`
- `kest_gross_total`
- `kest_net_total`

### `ibkr_summary`

Purpose:

- final IBKR section rollup used by the PDF/report flow

Columns:

- `type`: summary bucket such as `dividends`, `bonds`, `trades profit`, `trades loss`
- `currency`: bucket currency
- `profit_total`: original-currency total where applicable
- `profit_euro_total`: gross EUR total
- `profit_euro_net_total`: EUR total after Austrian KESt effect
- `withholding_tax_euro_total`: foreign withholding component in EUR where relevant
- `kest_gross_total`: gross Austrian KESt
- `kest_net_total`: net Austrian KESt
