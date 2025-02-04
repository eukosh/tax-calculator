from dataclasses import dataclass
from typing import List, TypedDict

import polars as pl
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Table

from src.const import COL_REPR_MAP, FLOAT_PRECISION, Column
from src.exceptions import MissingColumnException

styles = getSampleStyleSheet()


class SummarySection(TypedDict):
    Column.profit_euro_total: float
    Column.profit_euro_net_total: float
    Column.withholding_tax_euro_total: float
    Column.kest_gross_total: float
    Column.kest_net_total: float


@dataclass
class ReportSection:
    title: str
    df: pl.DataFrame


def create_table_from_df(df: pl.DataFrame):
    columns = [COL_REPR_MAP.get(col).name for col in df.columns]
    table_data = [columns] + df.to_numpy().tolist()
    print(table_data)
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


def add_summary_stats_from_df(current_stats: SummarySection, df: pl.DataFrame) -> SummarySection:
    missing_columns = set(current_stats.keys()) - set(df.columns)
    if missing_columns:
        raise MissingColumnException(f"Missing required columns: {', '.join(missing_columns)}")
    for col in current_stats.keys():
        current_stats[col] += df[col].sum()
    return current_stats


def create_tax_report(sections: List[ReportSection], output_path: str, title: str = "Tax Report"):
    pdf = SimpleDocTemplate(
        output_path, pagesize=A4, leftMargin=1 * cm, rightMargin=0.5 * cm, topMargin=1 * cm, bottomMargin=0.5 * cm
    )

    elements = [Paragraph(title, styles["Title"])]
    total_stats: SummarySection = {
        Column.profit_euro_total: 0,
        Column.profit_euro_net_total: 0,
        Column.withholding_tax_euro_total: 0,
        Column.kest_gross_total: 0,
        Column.kest_net_total: 0,
    }
    legend_items = {}
    for section in sections:
        elements.append(Paragraph(section.title, styles["Heading1"]))
        elements.append(create_table_from_df(section.df))
        elements.append(Spacer(1, 12))

        total_stats = add_summary_stats_from_df(total_stats, section.df)
        for col in section.df.columns:
            if col in legend_items:
                continue
            col_repr = COL_REPR_MAP.get(col)
            if col_repr:
                legend_items[col] = ListItem(
                    Paragraph(f"<b>{COL_REPR_MAP[col].name}</b>: {col_repr.description}", styles["Normal"]),
                    bulletText="•",
                )

    elements.append(Paragraph("Summary", styles["Heading1"]))
    total_stats = {k: round(v, FLOAT_PRECISION) for k, v in total_stats.items()}
    elements.append(
        Paragraph(
            "<br/>".join(f"<b>{COL_REPR_MAP[k].name}</b>: {v} €" for k, v in total_stats.items()), styles["Normal"]
        )
    )
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("Legend", styles["Heading1"]))
    elements.append(ListFlowable(legend_items.values(), bulletType="bullet"))
    pdf.build(elements)
