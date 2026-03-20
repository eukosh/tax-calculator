# Core App Glossary

This page documents the current core-app file schemas that matter most for the IBKR stock/ADR/REIT flow.

Current glossary coverage inside the core app:

- IBKR artifacts written under `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/`
- the Austrian opening-lot snapshot CSV used by authoritative stock runs

It does not yet try to fully document every non-IBKR broker artifact from the core app.

## File Inventory

| File / Pattern | Kind | Purpose | Row Grain |
| --- | --- | --- | --- |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/trades_tax_df__<start>_<end>.csv` | Output | Austrian tax sale allocations for stock/ADR/REIT sales | one consumed tax-lot slice per sale |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/trades_reconciliation__<start>_<end>.csv` | Output | Broker-validation view for the same stock sale allocations | one consumed tax-lot slice per sale |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/stock_tax_lot_state_full__<start>_<end>.csv` | State output | End-of-run stock lot state including closed and open lots | one Austrian stock lot |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/stock_tax_open_lots_final__<start>_<end>.csv` | State output | Open-only subset of the end-of-run stock lot state | one open Austrian stock lot |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/dividends_country_agg__<start>_<end>.csv` | Output | Country aggregate for non-ETF IBKR cash dividends | one issuer-country plus currency aggregate |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/bonds_tax_df__<start>_<end>.csv` | Output | Bond/corporate-action realized PnL detail | one realized bond event |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/bonds_tax_country_agg_df__<start>_<end>.csv` | Output | Bond realized PnL aggregate | one issuer-country plus currency aggregate |
| `data/output/<person>/tax_report_<person>_<start>_<end>/artifacts/ibkr/ibkr_summary__<start>_<end>.csv` | Output | Final IBKR summary bucket set used by the PDF/report flow | one summary bucket |
| `data/input/<person>/ibkr/austrian_opening_lots_<date>.csv` | Input | Austrian-authoritative opening stock lot state at move-in date | one opening Austrian lot |

## Important Inputs

### `austrian_opening_lots_<date>.csv`

Purpose:

- provides Austrian-authoritative opening stock/ADR/REIT lots for positions already held at move-in
- replaces pre-move broker acquisition basis for Austrian tax purposes

How it is used:

- authoritative stock runs load this file first
- the runtime then replays only post-start raw IBKR trades on top of that opening state

Columns:

- `snapshot_date`: date at which the opening Austrian state is frozen
- `asset_class`: stock-like IBKR subtype such as `COMMON`, `ADR`, `REIT`, or `ETF`
- `ticker`: security symbol
- `isin`: security identifier
- `lot_id`: stable lot identifier inside the Austrian lot system
- `buy_date`: Austrian acquisition date used by the authoritative lot engine; for move-in resets this is the snapshot date
- `original_quantity`: opening quantity on the lot
- `remaining_quantity`: quantity still left on the lot when the snapshot was created
- `currency`: trade currency
- `buy_price_ccy`: Austrian opening basis price per share in original currency
- `buy_fx_to_eur`: FX used to convert Austrian opening basis into EUR
- `original_cost_eur`: Austrian opening basis in EUR, excluding later step-ups
- `buy_fee_eur_total`: fee component attached to the Austrian lot; for move-in reset lots this is normally `0`
- `cumulative_oekb_stepup_eur`: cumulative ETF step-up basis carried on the lot
- `adjusted_basis_eur`: `original_cost_eur + cumulative_oekb_stepup_eur`
- `status`: lot state at snapshot time
- `broker`: broker/source label
- `account_id`: source account identifier used for audit
- `notes`: explanation of how the lot was created
- `last_adjustment_year`: last year that changed lot basis
- `last_adjustment_reference`: reference for the last basis adjustment
- `last_sale_date`: last sale date already applied to the lot
- `sold_quantity_ytd`: quantity already sold within the relevant year state
- `source_trade_id`: original source trade id for the economic lot
- `source_statement_file`: source file used to derive the lot
- `broker_buy_date`: original broker buy date kept for audit only
- `broker_buy_price_ccy`: original broker buy price kept for audit only
- `broker_buy_fx_to_eur`: original broker buy-date FX kept for audit only
- `broker_original_cost_eur`: original broker gross cost in EUR kept for audit only
- `broker_buy_fee_eur`: original broker buy fee in EUR kept for audit only
- `austrian_basis_method`: why the Austrian basis was chosen, for example `move_in_fmv_reset`
- `austrian_basis_price_ccy`: Austrian authoritative per-share basis in original currency
- `austrian_basis_fx_to_eur`: FX used for the Austrian authoritative basis

Relationship to outputs:

- `stock_tax_lot_state_full` is the same lot concept after post-move trades have been applied
- `trades_tax_df` shows how sales consume these lots

## Core IBKR Outputs

### `trades_tax_df`

Purpose:

- tax-only stock/ADR/REIT sale artifact
- shows how each sale consumed Austrian lots and what taxable basis/proceeds/gain were produced

How to read it:

- a single sale can create multiple rows if it consumes multiple lots
- this file does not contain broker validation fields

Columns:

- `sale_date`: trade date of the sale
- `sale_datetime`: sale execution timestamp used for matching and ordering
- `sale_trade_id`: raw broker-side sale identifier from trade history
- `ticker`: sold security
- `isin`: sold security ISIN
- `quantity_sold`: full sale quantity of the execution
- `sale_price_ccy`: per-share sale price in original currency
- `sale_fx`: sale-date FX used to convert gross sale proceeds into EUR
- `lot_id`: Austrian lot that supplied the sold quantity
- `lot_buy_date`: Austrian lot acquisition date
- `lot_buy_datetime`: source execution timestamp for the lot when available
- `lot_source_trade_id`: source trade id that created the lot
- `quantity_from_lot`: portion of the sale taken from this lot
- `taxable_proceeds_eur`: Austrian taxable gross sale proceeds in EUR allocated to this row
- `taxable_original_basis_eur`: original EUR basis allocated from the consumed lot
- `taxable_total_basis_eur`: total taxable basis allocated to the row; for core stocks this is the same as original basis because stock rows do not carry ETF-style step-up basis
- `taxable_gain_loss_eur`: `taxable_proceeds_eur - taxable_total_basis_eur`
- `allocated_buy_fee_eur`: informational allocation of original buy fees for audit/reconciliation only
- `allocated_sale_fee_eur`: informational allocation of sale fees for audit/reconciliation only
- `basis_origin`: where the lot basis came from; currently `snapshot` or `post_move_buy`
- `notes`: short explanation of the tax interpretation used for the row

### `trades_reconciliation`

Purpose:

- validation-only view comparing the authoritative stock sale engine to broker closed-lot output

How to read it:

- this file is parallel to `trades_tax_df`
- it proves sale-event alignment and, where applicable, exact post-move lot alignment

Columns:

- `sale_trade_id`: internal sale execution id from raw trade history
- `sale_date`: sale trade date
- `sale_datetime`: sale execution timestamp used to identify the sale event
- `ticker`: security symbol
- `isin`: security identifier
- `lot_id`: Austrian lot id for the consumed row
- `lot_buy_date`: Austrian lot acquisition date
- `lot_buy_datetime`: lot source execution timestamp when available
- `quantity_from_lot`: consumed quantity allocated from the lot
- `basis_origin`: `snapshot` or `post_move_buy`
- `reconciliation_segment`: classification of the matching rule; snapshot rows are informational, post-move rows are expected exact matches
- `reconciliation_status`: current match status for the row
- `sale_aggregate_status`: sale-level aggregate status after grouping the whole execution
- `sale_aggregate_quantity_internal`: total quantity sold according to the authoritative engine
- `sale_aggregate_quantity_broker`: total quantity sold according to broker closed lots
- `sale_proceeds_eur_internal`: authoritative gross EUR proceeds for the sale event
- `sale_proceeds_eur_broker_adjusted`: broker proceeds adjusted for fee treatment so they can be compared to Austrian gross proceeds
- `reconciliation_notes`: human-readable explanation of the match result

### `stock_tax_lot_state_full`

Purpose:

- full end-of-run stock lot state after all replayed trades

How to read it:

- includes `open`, `partially_sold`, and `closed` lots
- this is an audit artifact, not an input on future runs

Columns:

- `ticker`, `isin`, `lot_id`, `buy_date`, `currency`: lot identity
- `original_quantity`: original lot quantity when the Austrian lot was created
- `remaining_quantity`: quantity still left at end of run
- `buy_price_ccy`: Austrian per-share basis in original currency
- `buy_fx_to_eur`: FX used to convert the lot basis
- `original_cost_eur`: remaining original EUR basis still left on the lot after any partial sales
- `initial_original_cost_eur`: immutable original EUR basis when the Austrian lot was first created
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
- `snapshot_date`: opening snapshot date if the lot came from the move-in snapshot
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

### `stock_tax_open_lots_final`

Purpose:

- convenience subset of `stock_tax_lot_state_full`

How to read it:

- same schema as the full lot-state file
- contains only rows with `remaining_quantity > 0`

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
