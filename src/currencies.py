# https://data.ecb.europa.eu/help/api/data
# https://www.oenb.at/isawebstat/stabfrage/createReport?lang=EN&original=false&report=2.14.9
import logging
import os

import polars as pl
import requests


class ExchangeRates:
    def __init__(
        self,
        start_date,
        end_date,
        currencies=["USD", "GBP"],
        overwrite=False,
        raw_file_path="data/input/currencies/raw_exchange_rates.csv",
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.currency_str = "+".join(currencies)
        self.raw_file_path = raw_file_path
        self.overwrite = overwrite
        self.df = None  # To hold the Polars DataFrame

        # Check if raw file exists or overwrite is enabled
        if overwrite or not os.path.exists(raw_file_path):
            self._fetch_and_store_exchange_rates()
        else:
            logging.info("Loading exchange rates from file...")
            self._load_from_file()

    def _fetch_and_store_exchange_rates(self):
        url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.{self.currency_str}.EUR.SP00.A"
        params = {
            "startPeriod": self.start_date,
            "endPeriod": self.end_date,
            "format": "csvdata",
        }
        logging.info(f"Fetching exchange rates from {url}")
        # Fetch data from API
        response = requests.get(url, params=params)

        if response.status_code == 200:
            # Write raw data to disk
            os.makedirs(os.path.dirname(self.raw_file_path), exist_ok=True)
            with open(self.raw_file_path, "wb") as file:
                file.write(response.content)

            # Load into memory and filter
            self.df = self._load_and_filter(self.raw_file_path)
            logging.info("Exchange rates loaded.")
        else:
            raise Exception(f"Failed to fetch data. HTTP Status Code: {response.status_code}")

    def _load_from_file(self):
        # Load the raw file and filter it into memory
        self.df = self._load_and_filter(self.raw_file_path)

    def _load_and_filter(self, file_path):
        # Load and filter the CSV data using Polars
        df = pl.read_csv(file_path)
        filtered_df = df.select(
            [
                pl.col("TIME_PERIOD").str.strptime(pl.Date, "%Y-%m-%d").alias("rate_date"),
                pl.col("CURRENCY").alias("currency"),
                pl.col("CURRENCY_DENOM").alias("currency_denom"),
                pl.col("OBS_VALUE").alias("exchange_rate"),
                # pl.col("TITLE").alias("description"),
            ]
        )
        return filtered_df

    def get_rates(self):
        # Return the in-memory Polars DataFrame
        return self.df


# Example usage
if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    start_date = "2024-12-12"
    end_date = "2024-12-31"

    exchange_rates = ExchangeRates(start_date, end_date, overwrite=True)
    rates_df = exchange_rates.get_rates()
    print(rates_df)
