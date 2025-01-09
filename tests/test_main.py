import polars as pl
from polars.testing.asserts import assert_frame_equal

from src.providers.ibkr import calculate_kest


def test_calculate_kest():
    # Input data
    input_data = pl.DataFrame(
        {
            "dividends_euro": [100.0, 100.0, 200.0, 300.0],
            # assuming tax rate 0%, 15%, 30%, 5%
            "withholding_tax_euro": [0.0, 15.0, 60.0, 15.0],
        }
    )

    # Expected data
    expected_data = pl.DataFrame(
        {
            "dividends_euro": [100.0, 100.0, 200.0, 300.0],
            "withholding_tax_euro": [0.0, 15.0, 60.0, 15.0],
            "kest_gross": [27.5, 27.5, 55.0, 82.5],
            "kest_net": [27.5, 12.5, 25.0, 67.5],
            "dividends_euro_net": [72.5, 72.5, 115.0, 217.5],
        }
    )

    # Perform the calculation
    result = calculate_kest(input_data, amount_col="dividends_euro", tax_withheld_col="withholding_tax_euro")

    assert_frame_equal(expected_data, result)
