from dataclasses import dataclass
from datetime import date
from typing import List

import polars as pl
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Table

from src.const import Column, get_column_repr

styles = getSampleStyleSheet()
FINANZONLINE_NOTES = [
    "The FinanzOnline helper and tax estimate are capital-income-only views. "
    "They do not include other tax-return items such as donations, so the overall FinanzOnline "
    "pre-calculation can differ.",
    "Foreign tax withheld = sum of actual foreign tax withheld by brokers.",
    "Preliminary creditable foreign tax before loss offset = "
    "sum(min(withheld tax per payment, treaty cap per payment)). "
    "For the currently supported dividend/distribution rows, the treaty cap is generally 15% of gross income.",
    "Final creditable foreign tax uses favorable loss allocation: losses are applied first to "
    "positive income buckets with the lowest foreign-tax-credit ratio.",
    "Total tax base 27.5% = max(capital income + ETF distributions + trade profits + trade losses, 0).",
    "Estimated Austrian tax = max(total tax base * 27.5% - final creditable foreign tax, 0).",
    "Detailed formulas and decision rules are documented in docs/.",
]


@dataclass
class ReportSection:
    title: str
    df: pl.DataFrame


def create_table_from_df(df: pl.DataFrame) -> Table:
    columns = [col_repr.name if (col_repr := get_column_repr(col)) is not None else col for col in df.columns]
    table_data = [columns] + df.to_numpy().tolist()

    table = Table(table_data)
    table.hAlign = "LEFT"

    color = colors.toColor("rgba(0,115,153,0.9)")
    table.setStyle(
        [
            ("INNERGRID", (0, 0), (-1, -1), 0.5, "grey"),
            ("BACKGROUND", (0, 0), (-1, 0), color),
            ("TEXTCOLOR", (0, 0), (-1, 0), "white"),
            # ("FONTSIZE", (0, 0), (-1, 0), 12),
            # ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            # ("ALIGN", (1, 0), (-1, 0), "CENTER"),
            # ("ALIGN", (1, 1), (2, -1), "CENTER"),
            # ("ALIGN", (5, 1), (5, -1), "RIGHT"),
            # ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.antiquewhite, colors.beige]),
        ]
    )
    return table


def create_tax_report(
    sections: List[ReportSection], output_path: str, start_date: date, end_date: date, title: str = "Tax Report"
) -> None:
    pdf = SimpleDocTemplate(
        output_path, pagesize=A4, leftMargin=1 * cm, rightMargin=0.5 * cm, topMargin=1 * cm, bottomMargin=0.5 * cm
    )
    space = Spacer(1, 12)
    elements = [
        Paragraph(title, styles["Title"]),
        space,
        Paragraph(f"Reporting period: <b>{start_date}</b> to <b>{end_date}</b>", styles["Normal"]),
        space,
    ]
    legend_items: dict[str, ListItem] = {}
    for section in sections:
        elements.append(Paragraph(section.title, styles["Heading1"]))
        elements.append(create_table_from_df(section.df))
        elements.append(space)

        for col in section.df.columns:
            if col in legend_items:
                continue
            col_repr = get_column_repr(col)
            if col_repr:
                legend_items[col] = ListItem(
                    Paragraph(f"<b>{col_repr.name}</b>: {col_repr.description}", styles["Normal"]),
                    bulletText="•",
                )

    has_trades_row = any(
        (Column.type.value in section.df.columns)
        and section.df.filter(pl.col(Column.type.value).cast(pl.String).str.starts_with("trades")).height > 0
        for section in sections
    )
    if has_trades_row:
        elements.append(Spacer(1, 8))
        trade_note = (
            "Trade rows are converted to EUR at processing time "
            "(buy-date FX for cost, sell-date FX for proceeds) and then aggregated in EUR. "
            "So trade profit/loss rows are shown in EUR."
        )
        elements.append(Paragraph(f"* Note: {trade_note}", styles["Normal"]))

    has_finanzonline_section = any(section.title == "FinanzOnline Helper" for section in sections)
    if has_finanzonline_section:
        elements.append(Spacer(1, 8))
        for note in FINANZONLINE_NOTES:
            elements.append(Paragraph(f"* Note: {note}", styles["Normal"]))

    if legend_items:
        elements.append(Spacer(1, 12))
        elements.append(Paragraph("Glossary", styles["Heading1"]))
        elements.append(ListFlowable(list(legend_items.values()), bulletType="bullet"))
    pdf.build(elements)
