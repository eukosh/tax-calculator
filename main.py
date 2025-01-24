import logging
from datetime import date

import polars as pl

from src.currencies import ExchangeRates
from src.providers.freedom import process_freedom_statement
from src.providers.ibkr import calculate_summary_ibkr, process_bonds_ibkr, process_cash_transactions_ibkr
from src.providers.revolut import process_revolut_savings_statement
from src.providers.wise import process_wise_statement
from src.writer import PolarsWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s\n",
)

# Path to your XML file
xml_file = "data/input/For_tax_automation*"

person = "oryna"
# person = "eugene"

if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(100)
    reporting_start_date = date(2024, 5, 1)
    reporting_end_date = date(2024, 12, 31)
    # reporting_start_date = date(2024, 1, 1)
    # reporting_end_date = date(2024, 12, 31)

    exchange_rates = ExchangeRates(start_date="2024-01-01", end_date="2024-12-31")
    rates_df = exchange_rates.get_rates()

    if person == "eugene":
        # ------- IBKR
        dividends_country_agg_df = process_cash_transactions_ibkr(
            xml_file_path=xml_file,
            exchange_rates_df=rates_df,
            start_date=reporting_start_date,
            end_date=reporting_end_date,
        )

        bonds_tax_df, bonds_tax_country_agg_df = process_bonds_ibkr(
            xml_file_path=xml_file,
            exchange_rates_df=rates_df,
            start_date=reporting_start_date,
            end_date=reporting_end_date,
        )

        ibkr_writer = PolarsWriter(
            output_dir=f"data/output/{person}/ibkr",
            report_start_date=reporting_start_date,
            report_end_date=reporting_end_date,
        )

        ibkr_writer.write_csv(dividends_country_agg_df, "dividends_country_agg.csv")
        ibkr_writer.write_csv(bonds_tax_df, "bonds_tax_df.csv")
        ibkr_writer.write_csv(bonds_tax_country_agg_df, "bonds_tax_country_agg_df.csv")

        summary_ibkr_df = calculate_summary_ibkr(dividends_country_agg_df, bonds_tax_country_agg_df)
        ibkr_writer.write_csv(summary_ibkr_df, "ibkr_summary.csv")

    # ------- Revolut
    revolut_summary_df = process_revolut_savings_statement(
        "data/input/oryna/revolut/savings_statement_2024_12_08_2024_12_31.csv",
        rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )
    revolut_writer = PolarsWriter(
        output_dir=f"data/output/{person}/revolut",
        report_start_date=reporting_start_date,
        report_end_date=reporting_end_date,
    )
    revolut_writer.write_csv(revolut_summary_df, "revolut_tax_summary.csv")

    # ------- Wise
    wise_summary_df = process_wise_statement(
        f"data/input/{person}/wise/wise*",
        rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )
    wise_writer = PolarsWriter(
        output_dir=f"data/output/{person}/wise",
        report_start_date=reporting_start_date,
        report_end_date=reporting_end_date,
    )
    wise_writer.write_csv(wise_summary_df, "wise_tax_summary.csv")

    # ------- Freedom Finance
    freedom_summary_df = process_freedom_statement(
        "data/input/oryna/freedom/ff_oryna_2024-10-05 23_59_59_2024-12-31 23_59_59_all.json", rates_df
    )
    ff_writer = PolarsWriter(
        output_dir=f"data/output/{person}/freedom",
        report_start_date=reporting_start_date,
        report_end_date=reporting_end_date,
    )
    ff_writer.write_csv(freedom_summary_df, "freedom_tax_summary.csv")
