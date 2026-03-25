import polars as pl
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.finanzonline import (
    AMOUNT_EUR_COL,
    BUCKET_AMOUNT_EUR_COL,
    BUCKET_CATEGORY_COL,
    BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL,
    BUCKET_LABEL_COL,
    BUCKET_SOURCE_COL,
    BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL,
    CREDITABLE_FOREIGN_TAX_LABEL,
    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
    ESTIMATE_LABEL_COL,
    ESTIMATED_BASE_LABEL,
    ESTIMATED_TAX_LABEL,
    ETF_DISTRIBUTIONS_LABEL,
    ETF_DISTRIBUTION_BUCKET_CATEGORY,
    REIT_DISTRIBUTIONS_LABEL,
    INPUT_LABEL_COL,
    LOSS_OFFSET_METHOD_FAVORABLE,
    LOSS_OFFSET_METHOD_PROPORTIONAL,
    ORDINARY_CAPITAL_INCOME_LABEL,
    ORDINARY_INCOME_BUCKET_CATEGORY,
    PRE_LOSS_CREDITABLE_FOREIGN_TAX_LABEL,
    TRADE_LOSS_BUCKET_CATEGORY,
    TRADE_LOSS_LABEL,
    TRADE_PROFIT_BUCKET_CATEGORY,
    TRADE_PROFIT_LABEL,
    WITHHELD_FOREIGN_TAX_LABEL,
    build_finanzonline_buckets_from_provider_summaries,
    build_finanzonline_report,
)


def _bucket_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            BUCKET_SOURCE_COL: pl.String,
            BUCKET_LABEL_COL: pl.String,
            BUCKET_CATEGORY_COL: pl.String,
            BUCKET_AMOUNT_EUR_COL: pl.Float64,
            BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: pl.Float64,
            BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: pl.Float64,
        },
        orient="row",
    )


def test_build_finanzonline_buckets_from_provider_summaries_and_report():
    ibkr_summary_df = pl.DataFrame(
        {
            Column.type: ["dividends", "bonds", "ETF div", "trades profit", "trades loss"],
            Column.currency: ["USD", "USD", "USD", "EUR", "EUR"],
            Column.profit_total: [100.0, 40.0, 50.0, 20.0, -5.0],
            Column.profit_euro_total: [100.0, 40.0, 50.0, 20.0, -5.0],
            Column.profit_euro_net_total: [72.5, 29.0, 40.0, 20.0, -5.0],
            Column.withholding_tax_euro_total: [15.0, 0.0, 5.0, 0.0, 0.0],
            Column.kest_gross_total: [27.5, 11.0, 13.75, 0.0, 0.0],
            Column.kest_net_total: [12.5, 11.0, 10.0, 0.0, 0.0],
        }
    )
    freedom_summary_df = pl.DataFrame(
        {
            Column.type: ["ETF div", "trades profit"],
            Column.currency: ["USD", "EUR"],
            Column.profit_total: [60.0, 10.0],
            Column.profit_euro_total: [60.0, 10.0],
            Column.profit_euro_net_total: [48.0, 10.0],
            Column.withholding_tax_euro_total: [6.0, 0.0],
            Column.kest_gross_total: [16.5, 99.0],
            Column.kest_net_total: [12.0, 99.0],
        }
    )
    revolut_summary_df = pl.DataFrame(
        {
            Column.currency: ["EUR"],
            Column.profit_total: [30.0],
            Column.profit_euro_total: [30.0],
            Column.profit_euro_net_total: [21.75],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [8.25],
            Column.kest_net_total: [8.25],
        }
    )

    buckets_df = build_finanzonline_buckets_from_provider_summaries(
        {
            "ibkr": ibkr_summary_df,
            "freedom": freedom_summary_df,
            "revolut": revolut_summary_df,
        }
    )

    inputs_df, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    expected_inputs_df = pl.DataFrame(
        {
            INPUT_LABEL_COL: [
                ORDINARY_CAPITAL_INCOME_LABEL,
                TRADE_PROFIT_LABEL,
                TRADE_LOSS_LABEL,
                ETF_DISTRIBUTIONS_LABEL,
                REIT_DISTRIBUTIONS_LABEL,
                WITHHELD_FOREIGN_TAX_LABEL,
                PRE_LOSS_CREDITABLE_FOREIGN_TAX_LABEL,
                CREDITABLE_FOREIGN_TAX_LABEL,
            ],
            AMOUNT_EUR_COL: [130.0, 70.0, -5.0, 110.0, 0.0, 26.0, 23.25, 23.25],
        }
    )
    expected_estimate_df = pl.DataFrame(
        {
            ESTIMATE_LABEL_COL: [
                CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                ESTIMATED_BASE_LABEL,
                ESTIMATED_TAX_LABEL,
            ],
            AMOUNT_EUR_COL: [23.25, 305.0, 60.625],
        }
    )

    assert_frame_equal(inputs_df, expected_inputs_df)
    assert_frame_equal(estimate_df, expected_estimate_df)


def test_build_finanzonline_report_favorable_allocates_losses_to_zero_credit_income_first():
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 15.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 15.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "bond",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 50.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    inputs_df, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        inputs_df,
        pl.DataFrame(
            {
                INPUT_LABEL_COL: [
                    ORDINARY_CAPITAL_INCOME_LABEL,
                    TRADE_PROFIT_LABEL,
                    TRADE_LOSS_LABEL,
                    ETF_DISTRIBUTIONS_LABEL,
                    REIT_DISTRIBUTIONS_LABEL,
                    WITHHELD_FOREIGN_TAX_LABEL,
                    PRE_LOSS_CREDITABLE_FOREIGN_TAX_LABEL,
                    CREDITABLE_FOREIGN_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [150.0, 0.0, -100.0, 0.0, 0.0, 15.0, 15.0, 7.5],
            }
        ),
    )
    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [7.5, 50.0, 6.25],
            }
        ),
    )


def test_build_finanzonline_report_proportional_matches_accepted_simplification():
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 15.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 15.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "bond",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 50.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    _, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_PROPORTIONAL,
    )

    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [5.0, 50.0, 8.75],
            }
        ),
    )


def test_build_finanzonline_report_keeps_full_credit_when_zero_credit_income_absorbs_losses():
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 15.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 15.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "bond",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 200.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -50.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    _, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [15.0, 250.0, 53.75],
            }
        ),
    )


def test_build_finanzonline_report_zeroes_credit_when_losses_eliminate_all_positive_income():
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ETF_DISTRIBUTION_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 15.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 15.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    inputs_df, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        inputs_df,
        pl.DataFrame(
            {
                INPUT_LABEL_COL: [
                    ORDINARY_CAPITAL_INCOME_LABEL,
                    TRADE_PROFIT_LABEL,
                    TRADE_LOSS_LABEL,
                    ETF_DISTRIBUTIONS_LABEL,
                    REIT_DISTRIBUTIONS_LABEL,
                    WITHHELD_FOREIGN_TAX_LABEL,
                    PRE_LOSS_CREDITABLE_FOREIGN_TAX_LABEL,
                    CREDITABLE_FOREIGN_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [0.0, 0.0, -100.0, 100.0, 0.0, 15.0, 15.0, 0.0],
            }
        ),
    )
    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [0.0, 0.0, 0.0],
            }
        ),
    )


def test_build_finanzonline_report_only_losses_no_positive_income():
    """When there is only loss and no positive income, credit should be 0 and tax base 0."""
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss-1",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -200.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    inputs_df, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [0.0, 0.0, 0.0],
            }
        ),
    )


def test_build_finanzonline_report_losses_exactly_equal_positive_income():
    """When losses exactly equal positive income, credit goes to 0 and tax base is 0."""
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 15.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 15.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    inputs_df, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [0.0, 0.0, 0.0],
            }
        ),
    )


def test_build_finanzonline_report_multiple_loss_buckets():
    """Multiple loss buckets should be summed and allocated correctly."""
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "dividend",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 200.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 30.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 30.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "bond",
                BUCKET_CATEGORY_COL: ORDINARY_INCOME_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 100.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-loss-1",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -50.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
            {
                BUCKET_SOURCE_COL: "freedom",
                BUCKET_LABEL_COL: "trade-loss-2",
                BUCKET_CATEGORY_COL: TRADE_LOSS_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: -30.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    # Total losses = 80, total positive = 300
    # Favorable: bond (0 credit_per_euro, 100) absorbs 80 loss -> all 30 credit preserved
    _, estimate_df = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        estimate_df,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [30.0, 220.0, 30.5],
            }
        ),
    )

    # Proportional: credit * (300-80)/300 = 30 * 220/300 = 22.0
    _, estimate_df_prop = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_PROPORTIONAL,
    )

    assert_frame_equal(
        estimate_df_prop,
        pl.DataFrame(
            {
                ESTIMATE_LABEL_COL: [
                    CREDITABLE_FOREIGN_TAX_METRIC_LABEL,
                    ESTIMATED_BASE_LABEL,
                    ESTIMATED_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [22.0, 220.0, 38.5],
            }
        ),
    )


def test_build_finanzonline_report_trades_zero_amount_classified_as_profit():
    """A 'trades' row with amount=0 should classify as trade_profit (>= 0 path)."""
    buckets_df = _bucket_df(
        [
            {
                BUCKET_SOURCE_COL: "ibkr",
                BUCKET_LABEL_COL: "trade-zero",
                BUCKET_CATEGORY_COL: TRADE_PROFIT_BUCKET_CATEGORY,
                BUCKET_AMOUNT_EUR_COL: 0.0,
                BUCKET_WITHHELD_FOREIGN_TAX_EUR_COL: 0.0,
                BUCKET_CREDITABLE_FOREIGN_TAX_BEFORE_LOSS_EUR_COL: 0.0,
            },
        ]
    )

    inputs_df, _ = build_finanzonline_report(
        buckets_df,
        loss_offset_method=LOSS_OFFSET_METHOD_FAVORABLE,
    )

    assert_frame_equal(
        inputs_df,
        pl.DataFrame(
            {
                INPUT_LABEL_COL: [
                    ORDINARY_CAPITAL_INCOME_LABEL,
                    TRADE_PROFIT_LABEL,
                    TRADE_LOSS_LABEL,
                    ETF_DISTRIBUTIONS_LABEL,
                    REIT_DISTRIBUTIONS_LABEL,
                    WITHHELD_FOREIGN_TAX_LABEL,
                    PRE_LOSS_CREDITABLE_FOREIGN_TAX_LABEL,
                    CREDITABLE_FOREIGN_TAX_LABEL,
                ],
                AMOUNT_EUR_COL: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            }
        ),
    )
