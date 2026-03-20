import logging
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

import polars as pl

from src.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET, FLOAT_PRECISION, CurrencyCode, TransactionTypeIBKR
from src.const import Column as Col
from src.finanzonline import (
    BUCKET_AMOUNT_EUR_COL,
    BUCKET_CATEGORY_COL,
    BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL,
    BUCKET_LABEL_COL,
    BUCKET_SCHEMA,
    BUCKET_SOURCE_COL,
    BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL,
    ETF_DISTRIBUTION_BUCKET_CATEGORY,
    ORDINARY_INCOME_BUCKET_CATEGORY,
    empty_finanzonline_bucket_df,
)
from src.tax_lots import (
    TaxLot,
    build_fx_table_from_rates_df,
    build_tax_lot_from_trade,
    consume_fifo_sell,
    get_fx_rate,
    load_ibkr_stock_like_trades,
    load_opening_tax_lots,
    round_money,
    round_qty,
    tax_lots_to_df,
)
from src.utils import (
    build_separate_trade_profit_loss_rows,
    calculate_kest,
    convert_to_euro,
    extract_elements,
    has_rows,
    join_exchange_rates,
    read_xml_to_df,
)

IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES = {"COMMON", "ADR", "REIT"}


SummarySectionName = Literal["dividends", "bonds", "etf_dividends", "trades"]


@dataclass(frozen=True)
class IbkrSummarySection:
    name: SummarySectionName
    df: pl.DataFrame


def handle_dividend_adjustments(df: pl.DataFrame) -> pl.DataFrame:
    df = df.group_by(
        ["action_id", "settle_date", "issuer_country_code", "sub_category", "symbol", "currency", "type"]
    ).agg(pl.sum("amount").alias("amount"))

    return df


def apply_pivot(df: pl.DataFrame) -> pl.DataFrame:
    # Grouping keys
    index_keys = [
        "settle_date",
        "issuer_country_code",
        "sub_category",
        "symbol",
        "currency",
    ]

    duplicates_df = df.group_by(index_keys + ["type"]).agg(pl.len().alias("row_count")).filter(pl.col("row_count") > 1)

    if not duplicates_df.is_empty():
        logging.warning(f"\nWarning: Duplicate rows detected:\n{duplicates_df}")

    pivoted_df = df.pivot(
        values=["amount", "amount_euro"],  # Columns to aggregate
        index=index_keys,  # Grouping columns
        on="type",  # Values in this column become new column names
        aggregate_function="sum",  # Take unique values (default for pivot)
    )

    # Rename columns for clarity
    pivoted_df = pivoted_df.rename(
        {
            "amount_Dividends": "dividends",
            "amount_euro_Dividends": "dividends_euro",
        }
    ).with_columns(
        (pl.col("amount_Withholding Tax") if "amount_Withholding Tax" in pivoted_df.columns else pl.lit(0.0))
        .fill_null(0.0)
        .alias("withholding_tax"),
        (pl.col("amount_euro_Withholding Tax") if "amount_euro_Withholding Tax" in pivoted_df.columns else pl.lit(0.0))
        .fill_null(0.0)  # Also fill nulls
        .alias("withholding_tax_euro"),
    )

    return pivoted_df.select(
        [
            "settle_date",
            "issuer_country_code",
            "sub_category",
            "symbol",
            "currency",
            "dividends",
            "withholding_tax",
            "dividends_euro",
            "withholding_tax_euro",
        ]
    )


def agg_final_transactions(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("issuer_country_code", Col.currency)
        .agg(
            pl.sum("dividends").round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.sum("dividends_euro").round(FLOAT_PRECISION).alias(Col.dividends_euro_total),
            pl.sum("dividends_euro_net").round(FLOAT_PRECISION).alias(Col.dividends_euro_net_total),
            pl.sum("withholding_tax_euro").round(FLOAT_PRECISION).alias("withholding_tax_euro_total"),
            pl.sum("kest_gross").round(FLOAT_PRECISION).alias("kest_gross_total"),
            pl.sum("kest_net").round(FLOAT_PRECISION).alias("kest_net_total"),
        )
        .sort("dividends_euro_total", descending=True)
    )


def _build_cash_transactions_tax_df(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> pl.DataFrame | None:
    cash_transactions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CashTransactions"), "CashTransaction"),
    )
    if cash_transactions_df.is_empty():
        return None

    cash_transactions_df = (
        cash_transactions_df.rename({"subCategory": "sub_category", "actionID": "action_id"})
        .with_columns(
            pl.col("amount").cast(pl.Float64).alias("amount"),
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            pl.col("settleDate").str.strptime(pl.Date, "%Y-%m-%d").alias("settle_date"),
        )
        .filter(pl.col("settle_date").is_between(start_date, end_date))
    )
    if excluded_cash_transaction_subcategories:
        cash_transactions_df = cash_transactions_df.filter(
            ~pl.col("sub_category").is_in(sorted(excluded_cash_transaction_subcategories))
        )
    if cash_transactions_df.is_empty():
        return None
    cash_transactions_df = handle_dividend_adjustments(cash_transactions_df)

    types = cash_transactions_df["type"].unique().to_list()
    logging.info(f"Transaction Types: {types}")
    sum_per_type_df = cash_transactions_df.group_by("sub_category", "type").agg(pl.col("amount").sum())
    logging.info(f"\nSum per Transaction Category-Type:\n{sum_per_type_df}")

    cash_transactions_df = (
        cash_transactions_df.select(
            [
                "symbol",
                "sub_category",
                "currency",
                pl.when(pl.col("type") == TransactionTypeIBKR.pil)
                .then(pl.lit(TransactionTypeIBKR.dividend))
                .otherwise(pl.col("type"))
                .alias("type"),
                pl.col("amount").alias("amount"),
                "settle_date",
                "issuer_country_code",
            ]
        )
        .filter(pl.col("type").is_in(["Dividends", "Withholding Tax"]))
        .with_columns(
            pl.when(pl.col("type") == TransactionTypeIBKR.tax)
            .then(pl.col("amount").abs())
            .otherwise(pl.col("amount"))
            .alias("amount")
        )
    )

    joined_df = join_exchange_rates(
        df=cash_transactions_df,
        rates_df=exchange_rates_df,
        df_date_col="settle_date",
    )
    joined_df = convert_to_euro(joined_df, col_to_convert="amount")

    pivoted_df = apply_pivot(joined_df)
    logging.debug("\nPivoted DataFrame:\n {}".format(pivoted_df))

    pivoted_df = calculate_kest(pivoted_df, amount_col="dividends_euro", tax_withheld_col="withholding_tax_euro")
    logging.debug("\nPivoted with KeST DataFrame:\n {}".format(pivoted_df))
    return pivoted_df


def build_finanzonline_dividend_buckets_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> pl.DataFrame:
    tax_df = _build_cash_transactions_tax_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        excluded_cash_transaction_subcategories=excluded_cash_transaction_subcategories,
    )
    if tax_df is None or tax_df.is_empty():
        return empty_finanzonline_bucket_df()

    return tax_df.select(
        pl.lit("ibkr").alias(BUCKET_SOURCE_COL),
        pl.concat_str(
            [
                pl.lit("cash_dividend:"),
                pl.col("settle_date").cast(pl.String),
                pl.lit(":"),
                pl.col("symbol"),
                pl.lit(":"),
                pl.col("sub_category"),
            ]
        ).alias(BUCKET_LABEL_COL),
        pl.when(pl.col("sub_category") == "ETF")
        .then(pl.lit(ETF_DISTRIBUTION_BUCKET_CATEGORY))
        .otherwise(pl.lit(ORDINARY_INCOME_BUCKET_CATEGORY))
        .alias(BUCKET_CATEGORY_COL),
        pl.col("dividends_euro").alias(BUCKET_AMOUNT_EUR_COL),
        pl.col("withholding_tax_euro").alias(BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL),
        (pl.col("kest_gross") - pl.col("kest_net"))
        .clip(lower_bound=0.0)
        .alias(BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL),
    ).cast(BUCKET_SCHEMA)


def _load_closed_trade_lots_df(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    excluded_trade_subcategories: set[str] | None = None,
) -> pl.DataFrame:
    trades_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//Trades"), "Lot"),
    )
    if trades_df.is_empty():
        return pl.DataFrame()

    trades_df = trades_df.with_columns(
        (
            pl.col("subCategory").cast(pl.String).fill_null("")
            if "subCategory" in trades_df.columns
            else pl.lit("", dtype=pl.String)
        ).alias("subCategory"),
        (
            pl.col("assetCategory").cast(pl.String).fill_null("STK")
            if "assetCategory" in trades_df.columns
            else pl.lit("STK", dtype=pl.String)
        ).alias("assetCategory"),
        (
            pl.col("buySell").cast(pl.String).fill_null("SELL")
            if "buySell" in trades_df.columns
            else pl.lit("SELL", dtype=pl.String)
        ).alias("buySell"),
        (
            pl.col("levelOfDetail").cast(pl.String).fill_null("CLOSED_LOT")
            if "levelOfDetail" in trades_df.columns
            else pl.lit("CLOSED_LOT", dtype=pl.String)
        ).alias("levelOfDetail"),
        (
            pl.col("quantity").cast(pl.Float64).abs()
            if "quantity" in trades_df.columns
            else pl.lit(None, dtype=pl.Float64)
        ).alias("quantity"),
        (
            pl.col("transactionID").cast(pl.String).fill_null("")
            if "transactionID" in trades_df.columns
            else pl.lit("", dtype=pl.String)
        ).alias("sale_trade_id"),
        (
            pl.col("isin").cast(pl.String).fill_null("")
            if "isin" in trades_df.columns
            else (
                pl.col("securityID").cast(pl.String).fill_null("")
                if "securityID" in trades_df.columns
                else pl.lit("", dtype=pl.String)
            )
        ).alias("isin"),
    )

    trades_df = (
        trades_df.filter(pl.col("assetCategory") == "STK")
        .filter(pl.col("buySell") == "SELL")
        .filter(pl.col("levelOfDetail") == "CLOSED_LOT")
        .rename({"openDateTime": Col.buy_date, "tradeDate": Col.trade_date, "fifoPnlRealized": Col.profit})
        .select(
            Col.symbol,
            "isin",
            pl.col("cost").cast(pl.Float64),
            Col.currency,
            pl.col("subCategory").alias("sub_category"),
            pl.col("quantity"),
            pl.col("dateTime").str.strptime(pl.Datetime, format=IBKR_DATETIME_FORMAT).alias("sale_datetime"),
            pl.col(Col.buy_date).str.strptime(pl.Datetime, format=IBKR_DATETIME_FORMAT).alias("buy_datetime"),
            pl.col(Col.buy_date).str.to_date(format=IBKR_DATETIME_FORMAT).alias(Col.buy_date),
            pl.col(Col.trade_date).str.to_date().alias(Col.trade_date),
            pl.col(Col.profit).cast(pl.Float64),
            "sale_trade_id",
        )
        .filter(pl.col(Col.trade_date).is_between(start_date, end_date))
        .with_columns((pl.col("cost") + pl.col(Col.profit)).alias(Col.proceeds))
    )
    if excluded_trade_subcategories:
        trades_df = trades_df.filter(~pl.col("sub_category").is_in(sorted(excluded_trade_subcategories)))
    if trades_df.is_empty():
        return pl.DataFrame()

    joined_df = join_exchange_rates(
        df=trades_df,
        rates_df=exchange_rates_df,
        df_date_col=Col.trade_date,
    ).select(*trades_df.columns, pl.col(Col.exchange_rate).alias("sell_exchange_rate"))

    relevant_currencies = set(joined_df[Col.currency].unique().to_list())
    fx_table = build_fx_table_from_rates_df(exchange_rates_df, currencies=relevant_currencies)

    broker_rows: list[dict[str, object]] = []
    for row in joined_df.to_dicts():
        sell_exchange_rate = float(row["sell_exchange_rate"])
        proceeds_euro = round_money(
            row[Col.proceeds]
            if row[Col.currency] == CurrencyCode.euro
            else float(row[Col.proceeds]) / sell_exchange_rate
        )

        buy_exchange_rate: float | None = None
        cost_euro: float | None = None
        profit_euro: float | None = None
        if row[Col.currency] == CurrencyCode.euro:
            buy_exchange_rate = 1.0
            cost_euro = round_money(float(row["cost"]))
            profit_euro = round_money(proceeds_euro - cost_euro)
        else:
            try:
                buy_exchange_rate = round_money(get_fx_rate(fx_table, row[Col.currency], row[Col.buy_date]))
                cost_euro = round_money(float(row["cost"]) / buy_exchange_rate)
                profit_euro = round_money(proceeds_euro - cost_euro)
            except ValueError:
                buy_exchange_rate = None
                cost_euro = None
                profit_euro = None

        broker_rows.append(
            {
                **row,
                "buy_exchange_rate": buy_exchange_rate,
                "sell_exchange_rate": round_money(sell_exchange_rate),
                "cost_euro": cost_euro,
                Col.proceeds_euro: proceeds_euro,
                Col.profit_euro: profit_euro,
            }
        )

    return pl.DataFrame(broker_rows)


def _amount_matches(left: float, right: float, *, tolerance: float = 10 ** (-FLOAT_PRECISION)) -> bool:
    return abs(round(float(left), FLOAT_PRECISION) - round(float(right), FLOAT_PRECISION)) <= tolerance


def _apply_raw_trade_to_lots(
    lots: list[TaxLot],
    trade,
    *,
    fx_table: dict[str, tuple[list[date], list[float]]],
    track_ytd: bool,
) -> list[dict[str, object]]:
    if trade.operation == "buy":
        buy_fx = get_fx_rate(fx_table, trade.currency, trade.trade_date)
        lots.append(build_tax_lot_from_trade(trade, buy_fx=buy_fx))
        return []
    sale_fx = get_fx_rate(fx_table, trade.currency, trade.trade_date)
    return consume_fifo_sell(
        lots,
        trade,
        sale_fx=sale_fx,
        track_ytd=track_ytd,
    )


def _reconcile_authoritative_sales(
    sale_rows: list[dict[str, object]],
    broker_closed_lots_df: pl.DataFrame,
    *,
    snapshot_date: date | None,
) -> list[dict[str, object]]:
    if not sale_rows:
        return []

    broker_rows = broker_closed_lots_df.to_dicts() if not broker_closed_lots_df.is_empty() else []
    broker_by_sale: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in broker_rows:
        broker_key = (
            str(row.get("isin") or ""),
            row["sale_datetime"].strftime(IBKR_DATETIME_FORMAT),
        )
        broker_by_sale[broker_key].append(row)

    internal_by_sale: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in sale_rows:
        internal_key = (str(row["isin"]), str(row["sale_datetime"]))
        internal_by_sale[internal_key].append(row)

    reconciliation_rows: list[dict[str, object]] = []
    for sale_key, internal_rows in internal_by_sale.items():
        broker_group = broker_by_sale.get(sale_key)
        if not broker_group:
            raise ValueError(
                f"No broker closed lots found for authoritative sale {sale_key[0]} at {sale_key[1]}"
            )

        internal_total_qty = round_qty(sum(float(row["quantity_from_lot"]) for row in internal_rows))
        broker_total_qty = round_qty(sum(float(row["quantity"] or 0.0) for row in broker_group))
        if internal_total_qty != broker_total_qty:
            raise ValueError(
                f"Authoritative sale quantity mismatch for {sale_key[0]} at {sale_key[1]}: "
                f"internal={internal_total_qty}, broker={broker_total_qty}"
            )

        internal_total_proceeds_eur = round_money(sum(float(row["taxable_proceeds_eur"]) for row in internal_rows))
        total_sale_fee_eur = round_money(sum(float(row.get("allocated_sale_fee_eur") or 0.0) for row in internal_rows))
        broker_total_proceeds_eur = round_money(sum(float(row["proceeds_euro"]) for row in broker_group))
        broker_total_proceeds_eur_adjusted = round_money(broker_total_proceeds_eur + total_sale_fee_eur)
        if not _amount_matches(internal_total_proceeds_eur, broker_total_proceeds_eur_adjusted):
            raise ValueError(
                f"Authoritative sale proceeds mismatch for {sale_key[0]} at {sale_key[1]}: "
                f"internal={internal_total_proceeds_eur}, broker_adjusted={broker_total_proceeds_eur_adjusted}"
            )

        exact_internal_rows = [row for row in internal_rows if row["basis_origin"] == "post_move_buy"]
        if exact_internal_rows:
            exact_internal_groups: dict[tuple[str, float], dict[str, object]] = defaultdict(
                lambda: {
                    "quantity": 0.0,
                    "basis_eur": 0.0,
                    "proceeds_eur": 0.0,
                    "gain_eur": 0.0,
                    "buy_fee_eur": 0.0,
                    "sale_fee_eur": 0.0,
                }
            )
            for row in exact_internal_rows:
                group_key = (str(row["lot_buy_datetime"]), float(row["quantity_from_lot"]))
                exact_internal_groups[group_key]["quantity"] += float(row["quantity_from_lot"])
                exact_internal_groups[group_key]["basis_eur"] += float(row["taxable_original_basis_eur"])
                exact_internal_groups[group_key]["proceeds_eur"] += float(row["taxable_proceeds_eur"])
                exact_internal_groups[group_key]["gain_eur"] += float(row["taxable_gain_loss_eur"])
                exact_internal_groups[group_key]["buy_fee_eur"] += float(row.get("allocated_buy_fee_eur") or 0.0)
                exact_internal_groups[group_key]["sale_fee_eur"] += float(row.get("allocated_sale_fee_eur") or 0.0)

            if snapshot_date is None:
                exact_broker_rows = broker_group
            else:
                exact_broker_rows = [row for row in broker_group if row["buy_datetime"].date() >= snapshot_date]
            exact_broker_groups: dict[tuple[str, float], dict[str, object]] = defaultdict(
                lambda: {"quantity": 0.0, "basis_eur": 0.0, "proceeds_eur": 0.0, "gain_eur": 0.0}
            )
            for row in exact_broker_rows:
                if row["cost_euro"] is None or row["profit_euro"] is None:
                    raise ValueError(
                        f"Missing buy-side FX data for post-move broker lot {sale_key[0]} at {sale_key[1]} "
                        f"opened {row['buy_datetime'].strftime(IBKR_DATETIME_FORMAT)}"
                    )
                group_key = (row["buy_datetime"].strftime(IBKR_DATETIME_FORMAT), float(row["quantity"] or 0.0))
                exact_broker_groups[group_key]["quantity"] += float(row["quantity"] or 0.0)
                exact_broker_groups[group_key]["basis_eur"] += float(row["cost_euro"])
                exact_broker_groups[group_key]["proceeds_eur"] += float(row["proceeds_euro"])
                exact_broker_groups[group_key]["gain_eur"] += float(row["profit_euro"])

            if set(exact_internal_groups) != set(exact_broker_groups):
                raise ValueError(
                    f"Post-move exact-match broker lot keys differ for {sale_key[0]} at {sale_key[1]}: "
                    f"internal={sorted(exact_internal_groups)}, broker={sorted(exact_broker_groups)}"
                )

            for group_key, internal_group in exact_internal_groups.items():
                broker_group_values = exact_broker_groups[group_key]
                broker_basis_eur_adjusted = round_money(broker_group_values["basis_eur"] - internal_group["buy_fee_eur"])
                broker_proceeds_eur_adjusted = round_money(
                    broker_group_values["proceeds_eur"] + internal_group["sale_fee_eur"]
                )
                broker_gain_eur_adjusted = round_money(broker_proceeds_eur_adjusted - broker_basis_eur_adjusted)
                if not _amount_matches(internal_group["basis_eur"], broker_basis_eur_adjusted):
                    raise ValueError(
                        f"Post-move basis mismatch for {sale_key[0]} at {sale_key[1]} lot {group_key[0]}: "
                        f"internal={round_money(internal_group['basis_eur'])}, "
                        f"broker_adjusted={broker_basis_eur_adjusted}"
                    )
                if not _amount_matches(internal_group["proceeds_eur"], broker_proceeds_eur_adjusted):
                    raise ValueError(
                        f"Post-move proceeds mismatch for {sale_key[0]} at {sale_key[1]} lot {group_key[0]}: "
                        f"internal={round_money(internal_group['proceeds_eur'])}, "
                        f"broker_adjusted={broker_proceeds_eur_adjusted}"
                    )
                if not _amount_matches(internal_group["gain_eur"], broker_gain_eur_adjusted):
                    raise ValueError(
                        f"Post-move gain/loss mismatch for {sale_key[0]} at {sale_key[1]} lot {group_key[0]}: "
                        f"internal={round_money(internal_group['gain_eur'])}, "
                        f"broker_adjusted={broker_gain_eur_adjusted}"
                    )

        for row in internal_rows:
            is_snapshot_row = row["basis_origin"] == "snapshot"
            reconciliation_rows.append(
                {
                    "sale_trade_id": row["sale_trade_id"],
                    "sale_date": row["sale_date"],
                    "sale_datetime": row["sale_datetime"],
                    "ticker": row["ticker"],
                    "isin": row["isin"],
                    "lot_id": row["lot_id"],
                    "lot_buy_date": row["lot_buy_date"],
                    "lot_buy_datetime": row["lot_buy_datetime"],
                    "quantity_from_lot": row["quantity_from_lot"],
                    "basis_origin": row["basis_origin"],
                    "reconciliation_segment": (
                        "snapshot_derived_informational_segment" if is_snapshot_row else "post_move_exact_match_segment"
                    ),
                    "reconciliation_status": "informational" if is_snapshot_row else "matched",
                    "sale_aggregate_status": "matched",
                    "sale_aggregate_quantity_internal": internal_total_qty,
                    "sale_aggregate_quantity_broker": broker_total_qty,
                    "sale_aggregate_proceeds_eur_internal": round_money(internal_total_proceeds_eur),
                    "sale_aggregate_proceeds_eur_broker": round_money(broker_total_proceeds_eur_adjusted),
                    "reconciliation_notes": (
                        "Snapshot-derived basis differences are informational only."
                        if is_snapshot_row
                        else "Matched against broker closed-lot output."
                    ),
                }
            )

    return reconciliation_rows


def _build_authoritative_sales_detail_df(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    *,
    ibkr_trade_history_path: str,
    austrian_opening_lots_path: str | None,
    authoritative_start_date: date | None,
    excluded_trade_subcategories: set[str] | None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None, list[TaxLot] | None]:
    if authoritative_start_date is not None and not austrian_opening_lots_path:
        raise ValueError(
            "authoritative_start_date requires austrian_opening_lots_path; "
            "move-in authoritative start without opening lots is not supported"
        )

    opening_lots: list[TaxLot] = []
    snapshot_date: date | None = None
    if austrian_opening_lots_path:
        opening_lots, snapshot_date = load_opening_tax_lots(
            austrian_opening_lots_path,
            allowed_asset_classes=AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES,
        )
    raw_trades = load_ibkr_stock_like_trades(
        ibkr_trade_history_path,
        allowed_asset_classes=AUTHORITATIVE_STOCK_LIKE_SUBCATEGORIES,
    )
    if not raw_trades:
        return None, None, None

    processing_start_date = start_date
    if authoritative_start_date is not None:
        processing_start_date = max(processing_start_date, authoritative_start_date)
    if snapshot_date is not None and snapshot_date.year == processing_start_date.year:
        processing_start_date = max(processing_start_date, snapshot_date)

    opening_lower_bound = snapshot_date or authoritative_start_date
    opening_trades = [
        trade
        for trade in raw_trades
        if (opening_lower_bound is None or opening_lower_bound <= trade.trade_date) and trade.trade_date < start_date
    ]
    current_period_trades = [trade for trade in raw_trades if processing_start_date <= trade.trade_date <= end_date]
    if excluded_trade_subcategories:
        opening_trades = [trade for trade in opening_trades if trade.asset_class not in excluded_trade_subcategories]
        current_period_trades = [
            trade for trade in current_period_trades if trade.asset_class not in excluded_trade_subcategories
        ]

    working_lots = deepcopy(opening_lots)
    relevant_currencies = {lot.currency for lot in opening_lots}
    relevant_currencies.update(trade.currency for trade in opening_trades)
    relevant_currencies.update(trade.currency for trade in current_period_trades)
    if relevant_currencies:
        fx_table = build_fx_table_from_rates_df(exchange_rates_df, currencies=relevant_currencies)
        for trade in opening_trades:
            _apply_raw_trade_to_lots(working_lots, trade, fx_table=fx_table, track_ytd=False)
    else:
        fx_table = {}

    if not current_period_trades:
        return None, None, working_lots

    sale_rows: list[dict[str, object]] = []
    for trade in current_period_trades:
        allocations = _apply_raw_trade_to_lots(working_lots, trade, fx_table=fx_table, track_ytd=True)
        for allocation in allocations:
            allocation["reconciliation_segment"] = (
                "snapshot_derived_informational_segment"
                if allocation["basis_origin"] == "snapshot"
                else "post_move_exact_match_segment"
            )
            allocation["notes"] = (
                "Austrian-authoritative FIFO sale result uses move-in snapshot basis for pre-move holdings."
                if allocation["basis_origin"] == "snapshot"
                else "Austrian-authoritative FIFO sale result uses raw post-move buy lots and matches broker closed-lot output after fee adjustment."
            )
            sale_rows.append(allocation)

    if not sale_rows:
        return None, None, working_lots

    broker_closed_lots_df = _load_closed_trade_lots_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=processing_start_date,
        end_date=end_date,
        excluded_trade_subcategories=excluded_trade_subcategories,
    )
    reconciliation_rows = _reconcile_authoritative_sales(sale_rows, broker_closed_lots_df, snapshot_date=snapshot_date)
    reconciliation_df = pl.DataFrame(reconciliation_rows)
    sale_df = pl.DataFrame(sale_rows)
    detail_columns = [
        "sale_date",
        "sale_datetime",
        "sale_trade_id",
        "ticker",
        "isin",
        "quantity_sold",
        "sale_price_ccy",
        "sale_fx",
        "lot_id",
        "lot_buy_date",
        "lot_buy_datetime",
        "lot_source_trade_id",
        "quantity_from_lot",
        "taxable_proceeds_eur",
        "taxable_original_basis_eur",
        "taxable_total_basis_eur",
        "taxable_gain_loss_eur",
        "allocated_buy_fee_eur",
        "allocated_sale_fee_eur",
        "basis_origin",
        "notes",
    ]
    reconciliation_df = reconciliation_df.rename(
        {
            "sale_aggregate_proceeds_eur_internal": "sale_proceeds_eur_internal",
            "sale_aggregate_proceeds_eur_broker": "sale_proceeds_eur_broker_adjusted",
        }
    ).sort(["sale_date", "ticker", "lot_buy_date", "lot_id"])
    return (
        sale_df.select([column for column in detail_columns if column in sale_df.columns]).sort(
            ["sale_date", "ticker", "lot_buy_date", "lot_id"]
        ),
        reconciliation_df,
        working_lots,
    )


def _build_trade_summary_df(
    trades_detail_df: pl.DataFrame,
    *,
    separate_trade_profit_loss: bool,
) -> pl.DataFrame | None:
    trades_totals_df = trades_detail_df.select(
        pl.col("taxable_gain_loss_eur").sum().fill_null(0.0).round(FLOAT_PRECISION).alias(Col.profit_euro_total),
        pl.when(pl.col("taxable_gain_loss_eur") > 0)
        .then(pl.col("taxable_gain_loss_eur"))
        .otherwise(0.0)
        .sum()
        .round(FLOAT_PRECISION)
        .alias("trade_profit_euro_total"),
        (-pl.when(pl.col("taxable_gain_loss_eur") < 0).then(pl.col("taxable_gain_loss_eur")).otherwise(0.0).sum())
        .round(FLOAT_PRECISION)
        .alias("trade_loss_euro_total"),
    ).with_columns(
        pl.col(Col.profit_euro_total).alias(Col.profit_total),
        pl.col(Col.profit_euro_total).clip(lower_bound=0.0).alias("taxable_profit_euro"),
    )

    if not separate_trade_profit_loss:
        trades_tax_df = calculate_kest(
            df=trades_totals_df,
            amount_col="taxable_profit_euro",
            tax_withheld_col=None,
            net_col_name="taxable_profit_euro_net",
        )
        return trades_tax_df.select(
            pl.lit(CurrencyCode.euro.value).alias(Col.currency),
            pl.col(Col.profit_total),
            pl.col(Col.profit_euro_total),
            (pl.col(Col.profit_euro_total) - pl.col(Col.kest_net))
            .round(FLOAT_PRECISION)
            .alias(Col.profit_euro_net_total),
            pl.lit(0.0).alias(Col.withholding_tax_euro_total),
            pl.col(Col.kest_gross).round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net).round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )

    summary_frames = build_separate_trade_profit_loss_rows(trades_totals_df)
    return pl.concat(summary_frames, how="vertical_relaxed") if summary_frames else None


def process_trades_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    separate_trade_profit_loss: bool = True,
    excluded_trade_subcategories: set[str] | None = None,
    austrian_opening_lots_path: str | None = None,
    ibkr_trade_history_path: str | None = None,
    authoritative_start_date: date | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None]:
    """
    Returns:
    - trade tax detail dataframe
    - trade summary dataframe
    - final stock-like lot-state dataframe
    - reconciliation dataframe
    """
    logging.info("\n\n======================== Processing Trades ========================\n")
    if not ibkr_trade_history_path:
        raise ValueError("process_trades_ibkr requires ibkr_trade_history_path; broker closed-lot fallback has been removed")

    trades_detail_df, reconciliation_df, working_lots = _build_authoritative_sales_detail_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        ibkr_trade_history_path=ibkr_trade_history_path,
        austrian_opening_lots_path=austrian_opening_lots_path,
        authoritative_start_date=authoritative_start_date,
        excluded_trade_subcategories=excluded_trade_subcategories,
    )
    if trades_detail_df is None or trades_detail_df.is_empty():
        logging.warning("No authoritative stock-like sales matched the selected date range.")
        return None, None, tax_lots_to_df(working_lots or []), reconciliation_df
    trades_summary_df = _build_trade_summary_df(
        trades_detail_df,
        separate_trade_profit_loss=separate_trade_profit_loss,
    )
    return trades_detail_df, trades_summary_df, tax_lots_to_df(working_lots or []), reconciliation_df


def process_cash_transactions_ibkr(
    xml_file_path: str,
    exchange_rates_df: pl.DataFrame,
    start_date: date,
    end_date: date,
    extract_etf: bool = False,
    excluded_cash_transaction_subcategories: set[str] | None = None,
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load IBKR cash transactions from XML and normalize key columns/types.
    2. Filter by settle date and collapse adjustment records by action/type group.
    3. Keep only dividend/tax cash flows and normalize IBKR-specific PIL type.
    4. Convert withholding tax to absolute amount while keeping dividend sign as-is.
    5. Join FX by settle date and convert original amounts to EUR.
    6. Pivot dividend/tax rows into one row per security/date and compute KESt fields.
    7. Optionally split ETF rows into a separate aggregate bucket.
    8. Aggregate final country-level totals and return main + optional ETF outputs.
    """
    logging.info("\n\n======================== Processing Cash Transactions ========================\n")
    pivoted_df = _build_cash_transactions_tax_df(
        xml_file_path=xml_file_path,
        exchange_rates_df=exchange_rates_df,
        start_date=start_date,
        end_date=end_date,
        excluded_cash_transaction_subcategories=excluded_cash_transaction_subcategories,
    )
    if pivoted_df is None or pivoted_df.is_empty():
        return None, None

    etf_agg_df = None
    if extract_etf:
        etf_df = pivoted_df.filter(pl.col("sub_category") == "ETF")
        if has_rows(etf_df):
            pivoted_df = pivoted_df.filter(pl.col("sub_category") != "ETF")

            etf_agg_df = agg_final_transactions(etf_df)
            logging.info("Dividends from ETFs:\n{}".format(etf_agg_df))

    country_agg_df = agg_final_transactions(pivoted_df) if has_rows(pivoted_df) else None
    logging.info("Dividends by Country:\n{}".format(country_agg_df))

    return country_agg_df, etf_agg_df


def process_bonds_ibkr(
    xml_file_path: str, exchange_rates_df: pl.DataFrame, start_date: date, end_date: date
) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """
    1. Load corporate actions from XML and select bond-tax-relevant columns.
    2. Filter events by report date for the requested reporting period.
    3. Join FX by report date and convert realized PnL to EUR.
    4. Compute KESt on EUR realized PnL for each event.
    5. Produce detailed per-event output and aggregate country-level summary totals.
    6. Return detail dataframe and country aggregate dataframe.
    """
    logging.info("\n\n======================== Processing Corporate Actions ========================\n")

    # Convert the extracted data into Polars DataFrames
    corporate_actions_df = read_xml_to_df(
        file_path=xml_file_path,
        xml_extract_func=lambda root: extract_elements(root.find(".//CorporateActions"), "CorporateAction"),
    )
    if corporate_actions_df.is_empty():
        logging.warning("No Corporate Actions found in the XML file.")
        return None, None

    corporate_actions_df = corporate_actions_df.select(
        [
            pl.col("reportDate").str.strptime(pl.Date, "%Y-%m-%d").alias("report_date"),
            "isin",
            pl.col("issuerCountryCode").alias("issuer_country_code"),
            "currency",
            pl.col("proceeds").cast(pl.Float64),
            pl.col("fifoPnlRealized").cast(pl.Float64).alias("realized_pnl"),
        ]
    ).filter(pl.col("report_date").is_between(start_date, end_date))

    logging.debug("\nCorporate Actions DataFrame: %s\n", corporate_actions_df)

    joined_df = join_exchange_rates(
        df=corporate_actions_df,
        rates_df=exchange_rates_df,
        df_date_col="report_date",
    )

    joined_df = convert_to_euro(joined_df, "realized_pnl")
    tax_df = calculate_kest(joined_df, amount_col="realized_pnl_euro")

    tax_df = tax_df.select(
        "report_date",
        "isin",
        "issuer_country_code",
        "currency",
        "proceeds",
        "realized_pnl",
        "realized_pnl_euro",
        "realized_pnl_euro_net",
        "kest_gross",
        "kest_net",
    ).sort("realized_pnl", "isin", descending=True)
    logging.info(tax_df)

    country_agg_df = (
        tax_df.group_by("issuer_country_code", Col.currency)
        .agg(
            pl.sum("realized_pnl").round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.sum("realized_pnl_euro").round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.sum("realized_pnl_euro_net").round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            pl.sum("kest_gross").round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.sum("kest_net").round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        .sort(Col.profit_euro_total, descending=True)
    )
    logging.info(country_agg_df)

    return tax_df, country_agg_df


def calculate_summary_ibkr(
    sections: list[IbkrSummarySection],
) -> pl.DataFrame:
    """
    1. Validate section names, reject duplicates, and map each section to its aggregation config.
    2. For each section dataframe, aggregate totals by currency.
    3. Normalize section totals into a single summary schema with a type label.
    4. If no sections are provided, return an empty dataframe with summary schema.
    5. Concatenate all section summaries into one IBKR summary dataframe.
    """
    section_configs = {
        "dividends": {
            "label": "dividends",
            "profit_euro_col": "dividends_euro_total",
            "profit_euro_net_col": "dividends_euro_net_total",
            "withholding_col": "withholding_tax_euro_total",
        },
        "bonds": {
            "label": "bonds",
            "profit_euro_col": Col.profit_euro_total,
            "profit_euro_net_col": Col.profit_euro_net_total,
            "withholding_col": None,
        },
        "etf_dividends": {
            "label": "ETF div",
            "profit_euro_col": "dividends_euro_total",
            "profit_euro_net_col": "dividends_euro_net_total",
            "withholding_col": "withholding_tax_euro_total",
        },
        "trades": {
            "label": "trades",
            "profit_euro_col": Col.profit_euro_total,
            "profit_euro_net_col": Col.profit_euro_net_total,
            "withholding_col": Col.withholding_tax_euro_total,
        },
    }

    if not sections:
        return pl.DataFrame(
            schema={
                Col.type: pl.String,
                Col.currency: pl.String,
                Col.profit_total: pl.Float64,
                Col.profit_euro_total: pl.Float64,
                Col.profit_euro_net_total: pl.Float64,
                Col.withholding_tax_euro_total: pl.Float64,
                Col.kest_gross_total: pl.Float64,
                Col.kest_net_total: pl.Float64,
            }
        )

    seen_sections: set[str] = set()
    for section in sections:
        if section.name in seen_sections:
            raise ValueError(f"Duplicate IBKR summary section: {section.name}")
        seen_sections.add(section.name)

    merge_dfs = []
    for section in sections:
        config = section_configs[section.name]

        withholding_col = config["withholding_col"]
        withholding_expr = (
            pl.col(withholding_col).sum().round(FLOAT_PRECISION) if withholding_col else pl.lit(0.0)
        ).alias(Col.withholding_tax_euro_total)

        label_expr = pl.col(Col.type) if Col.type in section.df.columns else pl.lit(config["label"])

        section_summary_df = section.df.group_by(label_expr.alias(Col.type), Col.currency).agg(
            pl.col(Col.profit_total).sum().round(FLOAT_PRECISION).alias(Col.profit_total),
            pl.col(config["profit_euro_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_total),
            pl.col(config["profit_euro_net_col"]).sum().round(FLOAT_PRECISION).alias(Col.profit_euro_net_total),
            withholding_expr,
            pl.col(Col.kest_gross_total).sum().round(FLOAT_PRECISION).alias(Col.kest_gross_total),
            pl.col(Col.kest_net_total).sum().round(FLOAT_PRECISION).alias(Col.kest_net_total),
        )
        merge_dfs.append(section_summary_df)

    return pl.concat(merge_dfs, how="vertical_relaxed")
