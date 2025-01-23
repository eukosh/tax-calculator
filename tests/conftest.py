import pytest

from src.currencies import ExchangeRates


@pytest.fixture(scope="session")
def rates_df():
    exchange_rates = ExchangeRates(
        start_date="2024-01-01",
        end_date="2024-12-31",
        raw_file_path="tests/test_data/currencies/exchange_rates.csv",
    )
    return exchange_rates.get_rates()
