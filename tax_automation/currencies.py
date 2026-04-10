# https://data.ecb.europa.eu/help/api/data
# https://www.oenb.at/isawebstat/stabfrage/createReport?lang=EN&original=false&report=2.14.9
import logging
import os
from decimal import Decimal
from datetime import date, timedelta
from typing import Sequence

import polars as pl
import requests

from tax_automation.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET
from tax_automation.precision import PL_FX_DTYPE, quantize_fx


class ExchangeRatesCacheError(ValueError):
    pass


class ExchangeRates:
    def __init__(
        self,
        start_date: str | date,
        end_date: str | date,
        currencies: Sequence[str] = ("USD", "GBP"),
        overwrite: bool = False,
        raw_file_path: str = "data/input/currencies/raw_exchange_rates.csv",
    ):
        self.start_date = self._normalize_date(start_date)
        self.end_date = self._normalize_date(end_date)
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date.")

        self.currencies = tuple(currencies)
        self.currency_str = "+".join(self.currencies)
        self.raw_file_path = raw_file_path
        self.overwrite = overwrite
        self.df: pl.DataFrame | None = None

        # Check if raw file exists or overwrite is enabled
        if overwrite or not os.path.exists(raw_file_path):
            self._fetch_and_store_exchange_rates()
        else:
            logging.info("Loading exchange rates from file...")
            self._load_from_file()
            self._validate_coverage()

    def _normalize_date(self, value: str | date) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)

    def _fetch_and_store_exchange_rates(self):
        url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.{self.currency_str}.EUR.SP00.A"
        offset = timedelta(days=EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET)
        params = {
            "startPeriod": (self.start_date - offset).isoformat(),
            "endPeriod": (self.end_date + offset).isoformat(),
            "format": "csvdata",
        }
        logging.info(f"Fetching exchange rates from {url}")
        # Fetch data from API
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            # Write raw data to disk
            os.makedirs(os.path.dirname(self.raw_file_path), exist_ok=True)
            with open(self.raw_file_path, "wb") as file:
                file.write(response.content)

            # Load into memory and filter
            self.df = self._load_and_filter(self.raw_file_path)
            self._validate_coverage()
            logging.info("Exchange rates loaded.")
        else:
            raise Exception(f"Failed to fetch data. HTTP Status Code: {response.status_code}")

    def _load_from_file(self):
        # Load the raw file and filter it into memory
        self.df = self._load_and_filter(self.raw_file_path)

    def _load_and_filter(self, file_path: str) -> pl.DataFrame:
        # Load and filter the CSV data using Polars
        df = pl.read_csv(file_path)
        filtered_df = df.select(
            [
                pl.col("TIME_PERIOD").str.strptime(pl.Date, "%Y-%m-%d").alias("rate_date"),
                pl.col("CURRENCY").alias("currency"),
                pl.col("CURRENCY_DENOM").alias("currency_denom"),
                pl.col("OBS_VALUE").cast(pl.String).map_elements(lambda value: quantize_fx(Decimal(value)), return_dtype=PL_FX_DTYPE).alias("exchange_rate"),
                # pl.col("TITLE").alias("description"),
            ]
        )
        return filtered_df

    def _validate_coverage(self):
        if self.df is None or self.df.is_empty():
            raise ExchangeRatesCacheError("Exchange rates dataset is empty.")

        available_currencies = set(self.df["currency"].unique().to_list())
        missing_currencies = set(self.currencies) - available_currencies
        if missing_currencies:
            raise ExchangeRatesCacheError(
                f"Exchange rates file is missing requested currencies: {sorted(missing_currencies)}"
            )

        min_rate_date_raw = self.df["rate_date"].min()
        max_rate_date_raw = self.df["rate_date"].max()
        if not isinstance(min_rate_date_raw, date) or not isinstance(max_rate_date_raw, date):
            raise ExchangeRatesCacheError("Exchange rates dataset does not contain valid dates.")
        min_rate_date = min_rate_date_raw
        max_rate_date = max_rate_date_raw

        offset = timedelta(days=EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET)
        if min_rate_date > self.start_date + offset or max_rate_date < self.end_date - offset:
            raise ExchangeRatesCacheError(
                f"Exchange rates file does not cover requested period "
                f"{self.start_date}..{self.end_date}. Available dates: {min_rate_date}..{max_rate_date}. "
                "Refresh the cache with overwrite=True."
            )

    def get_rates(self) -> pl.DataFrame:
        if self.df is None:
            raise ValueError("Exchange rates are not loaded.")
        return self.df


# Example usage
if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    start_date = "2024-12-12"
    end_date = "2024-12-31"

    exchange_rates = ExchangeRates(start_date, end_date, overwrite=True)
    rates_df = exchange_rates.get_rates()
    print(rates_df)
