import logging
from datetime import date

import polars as pl

from src.currencies import ExchangeRates
from src.providers.revolut import process_revolut_savings_statement

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s\n",
)

# Path to your XML file
xml_file = "data/input/For_tax_automation*"


if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(100)
    # reporting_start_date = date(2024, 5, 1)
    # reporting_end_date = date(2024, 12, 31)
    reporting_start_date = date(2024, 1, 1)
    reporting_end_date = date(2024, 12, 31)

    exchange_rates = ExchangeRates(start_date="2024-01-01", end_date="2024-12-31")
    rates_df = exchange_rates.get_rates()

    # dividends_country_agg_df = process_cash_transactions_ibkr(
    #     xml_file_path=xml_file, exchange_rates_df=rates_df, start_date=reporting_start_date, end_date=reporting_end_date
    # )

    # bonds_tax_df, bonds_tax_country_agg_df = process_bonds_ibkr(
    #     xml_file_path=xml_file, exchange_rates_df=rates_df, start_date=reporting_start_date, end_date=reporting_end_date
    # )

    # ibkr_writer = PolarsWriter(
    #     output_dir="data/output/ibkr", report_start_date=reporting_start_date, report_end_date=reporting_end_date
    # )

    # ibkr_writer.write_csv(dividends_country_agg_df, "dividends_country_agg.csv")
    # ibkr_writer.write_csv(bonds_tax_df, "bonds_tax_df.csv")
    # ibkr_writer.write_csv(bonds_tax_country_agg_df, "bonds_tax_country_agg_df.csv")

    # summary_ibkr_df = calculate_summary_ibkr(dividends_country_agg_df, bonds_tax_country_agg_df)

    # ibkr_writer.write_csv(summary_ibkr_df, "ibkr_summary.csv")

    process_revolut_savings_statement("data/input/revolut-savings-statement_2024-10-07_2024-12-31.csv", rates_df)
