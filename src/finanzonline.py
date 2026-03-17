from collections.abc import Mapping
from typing import Any

import polars as pl

from src.const import FLOAT_PRECISION, KEST_RATE
from src.const import Column as Col

INPUT_LABEL_COL = "Field"
ESTIMATE_LABEL_COL = "Metric"
AMOUNT_EUR_COL = "Amount (EUR)"

ORDINARY_CAPITAL_INCOME_LABEL = "Capital income 27.5% (dividends/interest)"
TRADE_PROFIT_LABEL = "Trade profits 27.5%"
TRADE_LOSS_LABEL = "Trade losses 27.5% (enter as negative)"
ETF_DISTRIBUTIONS_LABEL = "ETF distributions 27.5%"
WITHHELD_FOREIGN_TAX_LABEL = "Foreign tax withheld"
CREDITABLE_FOREIGN_TAX_LABEL = "Creditable foreign tax"

WITHHELD_FOREIGN_TAX_METRIC_LABEL = "Foreign tax withheld"
CREDITABLE_FOREIGN_TAX_METRIC_LABEL = "Creditable foreign tax"
ESTIMATED_BASE_LABEL = "Total tax base 27.5%"
ESTIMATED_TAX_LABEL = "Estimated Austrian tax"

LOSS_OFFSET_METHOD_FAVORABLE = "favorable"
LOSS_OFFSET_METHOD_PROPORTIONAL = "proportional"

ORDINARY_INCOME_BUCKET_CATEGORY = "ordinary_income"
ETF_DISTRIBUTION_BUCKET_CATEGORY = "etf_distribution"
TRADE_PROFIT_BUCKET_CATEGORY = "trade_profit"
TRADE_LOSS_BUCKET_CATEGORY = "trade_loss"

BUCKET_SOURCE_COL = "source"
BUCKET_LABEL_COL = "label"
BUCKET_CATEGORY_COL = "category"
BUCKET_AMOUNT_EUR_COL = "amount_eur"
BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL = "withheld_foreign_tax_eur"
BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL = "creditable_foreign_tax_before_loss_eur"

BUCKET_SCHEMA = {
    BUCKET_SOURCE_COL: pl.String,
    BUCKET_LABEL_COL: pl.String,
    BUCKET_CATEGORY_COL: pl.String,
    BUCKET_AMOUNT_EUR_COL: pl.Float64,
    BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: pl.Float64,
    BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: pl.Float64,
}


def _existing_bucket_df(buckets_df: pl.DataFrame) -> pl.DataFrame:
    return buckets_df if not buckets_df.is_empty() else empty_finanzonline_bucket_df()


def empty_finanzonline_bucket_df() -> pl.DataFrame:
    return pl.DataFrame(schema=BUCKET_SCHEMA)


def _round_amount(value: float) -> float:
    return round(float(value), FLOAT_PRECISION)


def _get_float(row: Mapping[str, Any], key: str) -> float:
    return float(row.get(key, 0.0) or 0.0)


def _sum_df_column(df: pl.DataFrame, column_name: str) -> float:
    if df.is_empty():
        return 0.0
    return float(df.select(pl.col(column_name).sum()).item(0, 0) or 0.0)


def _coarse_bucket_category(row_type: str, amount_eur: float) -> str:
    if row_type in {"", "dividends", "bonds"}:
        return ORDINARY_INCOME_BUCKET_CATEGORY
    if row_type == "ETF div":
        return ETF_DISTRIBUTION_BUCKET_CATEGORY
    if row_type == "trades profit":
        return TRADE_PROFIT_BUCKET_CATEGORY
    if row_type == "trades loss":
        return TRADE_LOSS_BUCKET_CATEGORY
    if row_type == "trades":
        return TRADE_PROFIT_BUCKET_CATEGORY if amount_eur >= 0 else TRADE_LOSS_BUCKET_CATEGORY
    raise ValueError(f"Unsupported summary row type for FinanzOnline helper: {row_type}")


def build_finanzonline_buckets_from_summary_df(source: str, summary_df: pl.DataFrame | None) -> pl.DataFrame:
    if summary_df is None or summary_df.is_empty():
        return empty_finanzonline_bucket_df()

    bucket_rows: list[dict[str, object]] = []
    has_type_col = Col.type.value in summary_df.columns

    for index, row in enumerate(summary_df.to_dicts()):
        amount_eur = _get_float(row, Col.profit_euro_total.value)
        row_type = str(row.get(Col.type.value, "")) if has_type_col else ""
        bucket_rows.append(
            {
                BUCKET_SOURCE_COL: source,
                BUCKET_LABEL_COL: f"{source}:{row_type or 'summary'}:{index}",
                BUCKET_CATEGORY_COL: _coarse_bucket_category(row_type, amount_eur),
                BUCKET_AMOUNT_EUR_COL: amount_eur,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: _get_float(row, Col.withholding_tax_euro_total.value),
                # Creditable foreign tax = Austrian KESt reduction from foreign withholding,
                # i.e. kest_gross (KESt on gross amount) minus kest_net (KESt after treaty credit).
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: max(
                    _get_float(row, Col.kest_gross_total.value) - _get_float(row, Col.kest_net_total.value),
                    0.0,
                ),
            }
        )

    return pl.DataFrame(bucket_rows, schema=BUCKET_SCHEMA, orient="row")


def build_finanzonline_buckets_from_provider_summaries(
    provider_summaries: Mapping[str, pl.DataFrame | None],
) -> pl.DataFrame:
    bucket_frames = [
        bucket_df
        for source, df in provider_summaries.items()
        if not (bucket_df := build_finanzonline_buckets_from_summary_df(source, df)).is_empty()
    ]
    return pl.concat(bucket_frames, how="vertical_relaxed") if bucket_frames else empty_finanzonline_bucket_df()


def _sum_bucket_amount_by_category(buckets_df: pl.DataFrame, category: str) -> float:
    return _sum_df_column(
        buckets_df.filter(pl.col(BUCKET_CATEGORY_COL) == category),
        BUCKET_AMOUNT_EUR_COL,
    )


def _get_total_positive_income(buckets_df: pl.DataFrame) -> float:
    return _sum_df_column(buckets_df.filter(pl.col(BUCKET_AMOUNT_EUR_COL) > 0), BUCKET_AMOUNT_EUR_COL)


def _get_total_offset_losses(buckets_df: pl.DataFrame) -> float:
    gross_losses = -_sum_df_column(buckets_df.filter(pl.col(BUCKET_AMOUNT_EUR_COL) < 0), BUCKET_AMOUNT_EUR_COL)
    return min(gross_losses, _get_total_positive_income(buckets_df))


def _calculate_creditable_foreign_tax_after_loss_favorable(buckets_df: pl.DataFrame) -> float:
    positive_df = buckets_df.filter(pl.col(BUCKET_AMOUNT_EUR_COL) > 0)
    if positive_df.is_empty():
        return 0.0

    remaining_loss = _get_total_offset_losses(buckets_df)
    if remaining_loss <= 0:
        return _sum_df_column(positive_df, BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL)

    allocation_rows = (
        positive_df.with_columns(
            pl.when(pl.col(BUCKET_AMOUNT_EUR_COL) > 0)
            .then(pl.col(BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL) / pl.col(BUCKET_AMOUNT_EUR_COL))
            .otherwise(0.0)
            .alias("_credit_per_euro")
        )
        .sort(["_credit_per_euro", BUCKET_SOURCE_COL, BUCKET_LABEL_COL])
        .to_dicts()
    )

    creditable_foreign_tax = 0.0
    for row in allocation_rows:
        bucket_amount = _get_float(row, BUCKET_AMOUNT_EUR_COL)
        bucket_credit = _get_float(row, BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL)
        if bucket_amount <= 0:
            continue

        allocated_loss = min(remaining_loss, bucket_amount)
        remaining_amount = bucket_amount - allocated_loss
        remaining_fraction = remaining_amount / bucket_amount
        creditable_foreign_tax += bucket_credit * remaining_fraction
        remaining_loss -= allocated_loss

    return creditable_foreign_tax


def _calculate_creditable_foreign_tax_after_loss_proportional(buckets_df: pl.DataFrame) -> float:
    total_positive_income = _get_total_positive_income(buckets_df)
    if total_positive_income <= 0:
        return 0.0

    total_pre_loss_credit = _sum_df_column(
        buckets_df.filter(pl.col(BUCKET_AMOUNT_EUR_COL) > 0),
        BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL,
    )
    total_offset_losses = _get_total_offset_losses(buckets_df)
    post_loss_ratio = max(total_positive_income - total_offset_losses, 0.0) / total_positive_income
    return total_pre_loss_credit * post_loss_ratio


def _calculate_creditable_foreign_tax_after_loss(buckets_df: pl.DataFrame, loss_offset_method: str) -> float:
    if loss_offset_method == LOSS_OFFSET_METHOD_FAVORABLE:
        return _calculate_creditable_foreign_tax_after_loss_favorable(buckets_df)
    if loss_offset_method == LOSS_OFFSET_METHOD_PROPORTIONAL:
        return _calculate_creditable_foreign_tax_after_loss_proportional(buckets_df)
    raise ValueError(
        f"Unsupported FinanzOnline loss offset method: {loss_offset_method}. "
        f"Expected one of: {LOSS_OFFSET_METHOD_FAVORABLE}, {LOSS_OFFSET_METHOD_PROPORTIONAL}"
    )


def build_finanzonline_report(
    buckets_df: pl.DataFrame,
    loss_offset_method: str = LOSS_OFFSET_METHOD_FAVORABLE,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    buckets_df = _existing_bucket_df(buckets_df)

    ordinary_income = _sum_bucket_amount_by_category(buckets_df, ORDINARY_INCOME_BUCKET_CATEGORY)
    etf_distributions = _sum_bucket_amount_by_category(buckets_df, ETF_DISTRIBUTION_BUCKET_CATEGORY)
    trade_profit = _sum_bucket_amount_by_category(buckets_df, TRADE_PROFIT_BUCKET_CATEGORY)
    trade_loss = _sum_bucket_amount_by_category(buckets_df, TRADE_LOSS_BUCKET_CATEGORY)
    withheld_foreign_tax = _sum_df_column(buckets_df, BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL)
    creditable_foreign_tax = _calculate_creditable_foreign_tax_after_loss(buckets_df, loss_offset_method)

    estimated_base = max(_sum_df_column(buckets_df, BUCKET_AMOUNT_EUR_COL), 0.0)
    estimated_tax = max(estimated_base * KEST_RATE - creditable_foreign_tax, 0.0)

    inputs_df = pl.DataFrame(
        {
            INPUT_LABEL_COL: [
                ORDINARY_CAPITAL_INCOME_LABEL,
                TRADE_PROFIT_LABEL,
                TRADE_LOSS_LABEL,
                ETF_DISTRIBUTIONS_LABEL,
                WITHHELD_FOREIGN_TAX_LABEL,
                CREDITABLE_FOREIGN_TAX_LABEL,
            ],
            AMOUNT_EUR_COL: [
                _round_amount(ordinary_income),
                _round_amount(trade_profit),
                _round_amount(trade_loss),
                _round_amount(etf_distributions),
                _round_amount(withheld_foreign_tax),
                _round_amount(creditable_foreign_tax),
            ],
        }
    )

    estimate_df = pl.DataFrame(
        {
            ESTIMATE_LABEL_COL: [
                WITHHELD_FOREIGN_TAX_METRIC_LABEL,
                CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                ESTIMATED_BASE_LABEL,
                ESTIMATED_TAX_LABEL,
            ],
            AMOUNT_EUR_COL: [
                _round_amount(withheld_foreign_tax),
                _round_amount(creditable_foreign_tax),
                _round_amount(estimated_base),
                _round_amount(estimated_tax),
            ],
        }
    )

    return inputs_df, estimate_df
