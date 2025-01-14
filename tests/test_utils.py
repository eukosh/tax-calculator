import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.providers.ibkr import calculate_kest

dividends_euro = [100.0, 100.0, 200.0, 300.0]
withholding_tax_euro = [0.0, 15.0, 60.0, 15.0]  # assuming tax rate 0%, 15%, 30%, 5%


@pytest.fixture
def dividends_euro_df():
    return pl.DataFrame(
        {
            Column.profit_euro: dividends_euro,
            Column.withholding_tax_euro: withholding_tax_euro,
        }
    )


@pytest.mark.parametrize(
    "expected_df,tax_withheld_col",
    [
        (
            pl.DataFrame(
                {
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5],
                    Column.kest_net: [27.5, 12.5, 25.0, 67.5],
                    Column.profit_euro_net: [72.5, 72.5, 115.0, 217.5],
                }
            ),
            Column.withholding_tax_euro,
        ),
        (
            pl.DataFrame(
                {
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5],
                    Column.kest_net: [27.5, 27.5, 55.0, 82.5],
                    Column.profit_euro_net: [72.5, 72.5, 145.0, 217.5],
                }
            ),
            None,
        ),
    ],
)
def test_calculate_kest(dividends_euro_df, expected_df, tax_withheld_col):
    # Perform the calculation
    result = calculate_kest(dividends_euro_df, amount_col=Column.profit_euro, tax_withheld_col=tax_withheld_col)

    assert_frame_equal(expected_df, result)
