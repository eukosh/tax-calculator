import polars as pl
import pytest

from src.currencies import ExchangeRates


@pytest.mark.parametrize(
    "rows,start_date,end_date,currencies,error_match",
    [
        (
            [
                {"TIME_PERIOD": "2025-01-02", "CURRENCY": "USD", "CURRENCY_DENOM": "EUR", "OBS_VALUE": 1.1},
                {"TIME_PERIOD": "2025-01-03", "CURRENCY": "USD", "CURRENCY_DENOM": "EUR", "OBS_VALUE": 1.2},
            ],
            "2025-01-01",
            "2025-12-31",
            ("USD",),
            "does not cover requested period",
        ),
        (
            [
                {"TIME_PERIOD": "2025-01-01", "CURRENCY": "USD", "CURRENCY_DENOM": "EUR", "OBS_VALUE": 1.1},
                {"TIME_PERIOD": "2025-01-10", "CURRENCY": "USD", "CURRENCY_DENOM": "EUR", "OBS_VALUE": 1.2},
            ],
            "2025-01-01",
            "2025-01-10",
            ("USD", "GBP"),
            "missing requested currencies",
        ),
    ],
)
def test_exchange_rates_validation_fail_fast(rows, start_date, end_date, currencies, error_match, tmp_path):
    rates_path = tmp_path / "rates.csv"
    pl.DataFrame(rows).write_csv(rates_path)

    with pytest.raises(ValueError, match=error_match):
        ExchangeRates(
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            overwrite=False,
            raw_file_path=str(rates_path),
        )
