import logging
from datetime import UTC, date, datetime

import polars as pl
from dateutil.relativedelta import relativedelta

from src.currencies import ExchangeRates
from src.pdf.tax_report import ReportSection, create_tax_report
from src.providers.freedom import process_freedom_statement
from src.providers.ibkr import (
    calculate_summary_ibkr,
    process_bonds_ibkr,
    process_cash_transactions_ibkr,
    process_trades_ibkr,
)
from src.providers.revolut import process_revolut_savings_statement
from src.providers.wise import process_wise_statement
from src.writer import PolarsWriter

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s: %(message)s\n",
)


# person = "oryna"
person = "eugene"
ibkr_input_path = "data/input/eugene/ib/full/For_tax_automation_2025.xml"

if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    pl.Config.set_tbl_cols(100)
    reporting_start_date = date(2025, 1, 1)
    reporting_end_date = date(2025, 12, 31)
    # reporting_start_date = date(2024, 1, 1)
    # reporting_end_date = date(2024, 12, 31)

    logging.info(f"Reporting dates: {reporting_start_date} - {reporting_end_date}")

    now = datetime.now(tz=UTC)
    rates_start_date = date(year=now.year - 3, month=1, day=1)
    rates_end_date = reporting_end_date + relativedelta(weeks=1)
    if reporting_start_date < rates_start_date:
        rates_start_date = reporting_start_date

    logging.info(f"Exchange rate dates: {rates_start_date} - {rates_end_date}")
    # seems like no reason to persist rates since in prod use cases every day we would have to fetch new ones anyway
    # actually the whole process has to be smarter. Reporting period could be last fiscal year,
    # but the stock that was sold last year could be bought 10 years ago, so we need rates for the buy date as well
    # I like the idea of decoupling these 2 processes: rate fetching from ecb and tax calculation
    # In prod there could be a separate db of rates and a cron that would fetch new rates daily.
    # Or a simpler way -> infer start and end dates from brokerage statements and fetch from ecb on the fly
    exchange_rates = ExchangeRates(start_date=rates_start_date, end_date=rates_end_date, overwrite=True)
    rates_df = exchange_rates.get_rates()

    report_sections: list[ReportSection] = []

    # ------- IBKR
    trades_pnl_df = process_trades_ibkr(
        xml_file_path=ibkr_input_path,
        exchange_rates_df=rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )

    print(trades_pnl_df)
    dividends_country_agg_df, reit_divs_agg_df = process_cash_transactions_ibkr(
        xml_file_path=ibkr_input_path,
        exchange_rates_df=rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
        extract_etf_and_reit=True,
    )

    bonds_tax_df, bonds_tax_country_agg_df = process_bonds_ibkr(
        xml_file_path=ibkr_input_path,
        exchange_rates_df=rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )

    ibkr_writer = PolarsWriter(
        output_dir=f"data/output/{person}/ibkr",
        report_start_date=reporting_start_date,
        report_end_date=reporting_end_date,
    )

    if dividends_country_agg_df is not None:
        ibkr_writer.write_csv(dividends_country_agg_df, "dividends_country_agg.csv")
    if bonds_tax_df is not None:
        ibkr_writer.write_csv(bonds_tax_df, "bonds_tax_df.csv")
        ibkr_writer.write_csv(bonds_tax_country_agg_df, "bonds_tax_country_agg_df.csv")

    summary_ibkr_df = calculate_summary_ibkr(dividends_country_agg_df, bonds_tax_country_agg_df, reit_divs_agg_df)
    ibkr_writer.write_csv(summary_ibkr_df, "ibkr_summary.csv")
    report_sections.append(ReportSection("IBKR", summary_ibkr_df))

    # ------- Revolut
    revolut_summary_df = process_revolut_savings_statement(
        "data/input/oryna/revolut/savings_statement_2025_01_01_2025_06_30.csv"
        if person == "oryna"
        else "data/input/eugene/revolut/savings-statement_2024-10-07_2024-12-31.csv",
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
    report_sections.append(ReportSection("Revolut", revolut_summary_df))

    if person == "oryna":
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
        report_sections.append(ReportSection("Wise", wise_summary_df))

    # ------- Freedom Finance
    freedom_summary_df = process_freedom_statement(
        "data/input/oryna/freedom/ff_oryna_2024-12-31 23_59_59_2025-07-06 23_59_59_all.json"
        if person == "oryna"
        else "data/input/eugene/freedom/_freedom_2024-04-30 23_59_59_2024-12-31 23_59_59_all.json",
        rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )
    ff_writer = PolarsWriter(
        output_dir=f"data/output/{person}/freedom",
        report_start_date=reporting_start_date,
        report_end_date=reporting_end_date,
    )
    ff_writer.write_csv(freedom_summary_df, "freedom_tax_summary.csv")
    report_sections.append(ReportSection("Freedom Finance", freedom_summary_df))

    create_tax_report(
        report_sections,
        output_path=f"data/output/{person}/tax_report_{person}_{reporting_start_date}_{reporting_end_date}.pdf",
        title=f"Tax Report - {person.capitalize()}",
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )
