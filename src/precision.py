from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import polars as pl

MONEY_SCALE = 6
FX_SCALE = 6
QTY_SCALE = 8

MONEY_QUANTIZER = Decimal("0.000001")
FX_QUANTIZER = Decimal("0.000001")
QTY_QUANTIZER = Decimal("0.00000001")

PL_MONEY_DTYPE = pl.Decimal(scale=MONEY_SCALE)
PL_FX_DTYPE = pl.Decimal(scale=FX_SCALE)
PL_QTY_DTYPE = pl.Decimal(scale=QTY_SCALE)


def to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def quantize_money(value: Any) -> Decimal:
    return to_decimal(value).quantize(MONEY_QUANTIZER, rounding=ROUND_HALF_UP)


def quantize_fx(value: Any) -> Decimal:
    return to_decimal(value).quantize(FX_QUANTIZER, rounding=ROUND_HALF_UP)


def quantize_qty(value: Any) -> Decimal:
    return to_decimal(value).quantize(QTY_QUANTIZER, rounding=ROUND_HALF_UP)


def to_output_float(value: Any) -> float:
    return float(to_decimal(value))


def decimal_lit(value: Any) -> pl.Expr:
    return pl.lit(to_decimal(value))


def money_lit(value: Any = 0) -> pl.Expr:
    return pl.lit(quantize_money(value), dtype=PL_MONEY_DTYPE)


def fx_lit(value: Any = 0) -> pl.Expr:
    return pl.lit(quantize_fx(value), dtype=PL_FX_DTYPE)


def qty_lit(value: Any = 0) -> pl.Expr:
    return pl.lit(quantize_qty(value), dtype=PL_QTY_DTYPE)


def cast_decimal_columns_to_float(df: pl.DataFrame) -> pl.DataFrame:
    casts = [
        pl.col(name).cast(pl.Float64).alias(name)
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Decimal)
    ]
    return df.with_columns(casts) if casts else df
