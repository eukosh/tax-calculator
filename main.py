import logging
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta

from src.currencies import ExchangeRates, ExchangeRatesCacheError
from src.pdf.tax_report import ReportSection, create_tax_report
from src.providers.freedom import process_freedom_statement
from src.providers.ibkr import (
    IbkrSummarySection,
    calculate_summary_ibkr,
    process_bonds_ibkr,
    process_cash_transactions_ibkr,
    process_trades_ibkr,
)
from src.providers.revolut import process_revolut_savings_statement
from src.providers.wise import process_wise_statement
from src.utils import has_rows
from src.writer import PolarsWriter

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s: %(message)s\n",
)


# person = "oryna"
person = "eugene"
ibkr_input_path = "data/input/eugene/2025/ibkr_20250101_20251231.xml"
freedom_input_path = (
    "data/input/oryna/freedom/ff_oryna_2024-12-31 23_59_59_2025-07-06 23_59_59_all.json"
    if person == "oryna"
    else "data/input/eugene/2025/freedom_2024-12-31 23_59_59_2025-12-31 23_59_59_all.json"
)

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
    try:
        exchange_rates = ExchangeRates(start_date=rates_start_date, end_date=rates_end_date, overwrite=False)
    except ExchangeRatesCacheError as cache_error:
        logging.warning("Cached exchange rates are insufficient (%s). Refreshing from ECB...", cache_error)
        exchange_rates = ExchangeRates(start_date=rates_start_date, end_date=rates_end_date, overwrite=True)
    rates_df = exchange_rates.get_rates()

    report_sections: list[ReportSection] = []

    # ------- IBKR
    trades_tax_df, trades_summary_df = process_trades_ibkr(
        xml_file_path=ibkr_input_path,
        exchange_rates_df=rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
    )
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

    if has_rows(dividends_country_agg_df):
        ibkr_writer.write_csv(dividends_country_agg_df, "dividends_country_agg.csv")
    if has_rows(bonds_tax_df):
        ibkr_writer.write_csv(bonds_tax_df, "bonds_tax_df.csv")
    if has_rows(bonds_tax_country_agg_df):
        ibkr_writer.write_csv(bonds_tax_country_agg_df, "bonds_tax_country_agg_df.csv")
    if has_rows(trades_tax_df):
        ibkr_writer.write_csv(trades_tax_df, "trades_tax_df.csv")

    summary_sections: list[IbkrSummarySection] = []
    if has_rows(dividends_country_agg_df):
        summary_sections.append(IbkrSummarySection("dividends", dividends_country_agg_df))
    if has_rows(bonds_tax_country_agg_df):
        summary_sections.append(IbkrSummarySection("bonds", bonds_tax_country_agg_df))
    if has_rows(reit_divs_agg_df):
        summary_sections.append(IbkrSummarySection("reit_dividends", reit_divs_agg_df))
    if has_rows(trades_summary_df):
        summary_sections.append(IbkrSummarySection("trades", trades_summary_df))

    summary_ibkr_df = calculate_summary_ibkr(sections=summary_sections)
    ibkr_writer.write_csv(summary_ibkr_df, "ibkr_summary.csv")
    report_sections.append(ReportSection("IBKR", summary_ibkr_df))

    # ------- Revolut
    revolut_statement_paths = (
        ["data/input/oryna/revolut/savings_statement_2025_01_01_2025_06_30.csv"]
        if person == "oryna"
        else [
            "data/input/eugene/2025/revolut_2025-01-01_2025-12-31_en_eur.csv",
            "data/input/eugene/2025/revolut_2025-01-01_2025-12-31_en_usd.csv",
        ]
    )

    revolut_summary_df = pl.concat(
        [
            process_revolut_savings_statement(
                statement_path,
                rates_df,
                start_date=reporting_start_date,
                end_date=reporting_end_date,
            )
            for statement_path in revolut_statement_paths
        ],
        how="vertical",
    ).sort("profit_euro_total", descending=True)

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
    exclusion_file_path = f"data/input/{person}/freedom/dividend_entries_to_be_excluded_from_future_tax.csv"
    incorrect_withholding_tax_output_file = (
        f"data/output/{person}/freedom/dividends_with_incorrect_non_0_withholding_tax.csv"
    )
    freedom_summary_df = process_freedom_statement(
        freedom_input_path,
        rates_df,
        start_date=reporting_start_date,
        end_date=reporting_end_date,
        exclude_corporate_action_ids_file=exclusion_file_path if Path(exclusion_file_path).exists() else None,
        incorrect_withholding_tax_output_file=incorrect_withholding_tax_output_file,
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
