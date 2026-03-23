from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


SIX_DP = Decimal("0.000001")
TWO_DP = Decimal("0.01")


def _to_decimal(raw: str) -> Decimal:
    normalized = raw.strip().replace(" ", "").replace(",", ".")
    if not normalized:
        return Decimal("0")
    return Decimal(normalized)


def parse_money_input(raw: str) -> Decimal:
    return _to_decimal(raw)


def format_formula_amount(value: Decimal) -> str:
    rendered = f"{value.quantize(SIX_DP, rounding=ROUND_HALF_UP):f}"
    rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def format_display_amount(value: Decimal) -> str:
    return f"{value.quantize(TWO_DP, rounding=ROUND_HALF_UP):f}"


@dataclass(frozen=True)
class CoreInputs:
    ordinary_capital_income: Decimal = Decimal("0")
    trade_profit: Decimal = Decimal("0")
    trade_loss: Decimal = Decimal("0")
    fund_distributions: Decimal = Decimal("0")
    creditable_foreign_tax: Decimal = Decimal("0")


@dataclass(frozen=True)
class ReportingFundsInputs:
    fund_distributions: Decimal = Decimal("0")
    deemed_distributed_income: Decimal = Decimal("0")
    domestic_dividends_kz189: Decimal = Decimal("0")
    domestic_dividend_kest_kz899: Decimal = Decimal("0")
    creditable_foreign_tax: Decimal = Decimal("0")


@dataclass(frozen=True)
class NonReportingFundsInputs:
    fund_distributions: Decimal = Decimal("0")
    deemed_distributed_income: Decimal = Decimal("0")
    domestic_dividends_kz189: Decimal = Decimal("0")
    domestic_dividend_kest_kz899: Decimal = Decimal("0")
    creditable_foreign_tax: Decimal = Decimal("0")


@dataclass(frozen=True)
class E1kvComputation:
    ordinary_capital_income_total: Decimal
    trade_profit_total: Decimal
    trade_loss_total: Decimal
    fund_distributions_total: Decimal
    deemed_distributed_income_total: Decimal
    domestic_dividends_kz189_total: Decimal
    domestic_dividend_kest_kz899_total: Decimal
    creditable_foreign_tax_source_sum: Decimal
    positive_total: Decimal
    post_loss_base: Decimal
    foreign_tax_ceiling: Decimal
    final_creditable_foreign_tax: Decimal


def build_e1kv_computation(
    *,
    core: CoreInputs,
    reporting_funds: ReportingFundsInputs,
    non_reporting_funds: NonReportingFundsInputs,
) -> E1kvComputation:
    normalized_trade_loss = -abs(core.trade_loss)

    ordinary_capital_income_total = core.ordinary_capital_income
    trade_profit_total = core.trade_profit
    trade_loss_total = normalized_trade_loss
    fund_distributions_total = (
        core.fund_distributions + reporting_funds.fund_distributions + non_reporting_funds.fund_distributions
    )
    deemed_distributed_income_total = (
        reporting_funds.deemed_distributed_income + non_reporting_funds.deemed_distributed_income
    )
    domestic_dividends_kz189_total = (
        reporting_funds.domestic_dividends_kz189 + non_reporting_funds.domestic_dividends_kz189
    )
    domestic_dividend_kest_kz899_total = (
        reporting_funds.domestic_dividend_kest_kz899 + non_reporting_funds.domestic_dividend_kest_kz899
    )
    creditable_foreign_tax_source_sum = (
        core.creditable_foreign_tax
        + reporting_funds.creditable_foreign_tax
        + non_reporting_funds.creditable_foreign_tax
    )

    positive_total = (
        ordinary_capital_income_total
        + trade_profit_total
        + fund_distributions_total
        + deemed_distributed_income_total
        + domestic_dividends_kz189_total
    )
    post_loss_base = max(positive_total - abs(trade_loss_total), Decimal("0"))
    foreign_tax_ceiling = (post_loss_base * Decimal("0.275")).quantize(SIX_DP, rounding=ROUND_HALF_UP)
    final_creditable_foreign_tax = min(creditable_foreign_tax_source_sum, foreign_tax_ceiling)

    return E1kvComputation(
        ordinary_capital_income_total=ordinary_capital_income_total,
        trade_profit_total=trade_profit_total,
        trade_loss_total=trade_loss_total,
        fund_distributions_total=fund_distributions_total,
        deemed_distributed_income_total=deemed_distributed_income_total,
        domestic_dividends_kz189_total=domestic_dividends_kz189_total,
        domestic_dividend_kest_kz899_total=domestic_dividend_kest_kz899_total,
        creditable_foreign_tax_source_sum=creditable_foreign_tax_source_sum,
        positive_total=positive_total,
        post_loss_base=post_loss_base,
        foreign_tax_ceiling=foreign_tax_ceiling,
        final_creditable_foreign_tax=final_creditable_foreign_tax,
    )


def render_output_markdown(
    *,
    tax_year: int,
    core: CoreInputs,
    reporting_funds: ReportingFundsInputs,
    non_reporting_funds: NonReportingFundsInputs,
    result: E1kvComputation,
) -> str:
    return (
        f"# Manual E1kv Input {tax_year}\n\n"
        f"##### Einkünfte aus der Überlassung von Kapital (§ 27 Abs. 2; insbesondere Dividenden, Zinserträge aus Wertpapieren 27,5%) = "
        f"{format_display_amount(result.ordinary_capital_income_total)}\n\n"
        "##### Einkünfte aus realisierten Wertsteigerungen von Kapitalvermögen (§ 27 Abs. 3; insbesondere Veräußerungsgewinne aus Aktien,\n"
        "Forderungswertpapieren und Fondsanteilen)\n"
        f"Überschüsse 27,5% = {format_display_amount(result.trade_profit_total)}\n\n"
        f"Verluste = {format_display_amount(result.trade_loss_total)}\n\n\n"
        "##### Einkünfte aus Investmentfonds und Immobilieninvestmentfonds\n"
        f"Ausschüttungen 27,5% = {format_formula_amount(core.fund_distributions)} + "
        f"{format_formula_amount(reporting_funds.fund_distributions)} + "
        f"{format_formula_amount(non_reporting_funds.fund_distributions)} = "
        f"{format_display_amount(result.fund_distributions_total)}\n"
        f"Ausschüttungsgleiche Erträge 27,5% = {format_formula_amount(reporting_funds.deemed_distributed_income)} + "
        f"{format_formula_amount(non_reporting_funds.deemed_distributed_income)} = "
        f"{format_display_amount(result.deemed_distributed_income_total)}\n"
        f"Inländische Dividenden im Verlustausgleich (KZ 189) = "
        f"{format_formula_amount(reporting_funds.domestic_dividends_kz189)} + "
        f"{format_formula_amount(non_reporting_funds.domestic_dividends_kz189)} = "
        f"{format_display_amount(result.domestic_dividends_kz189_total)}\n"
        f"KESt auf inländische Dividenden (KZ 899) = "
        f"{format_formula_amount(reporting_funds.domestic_dividend_kest_kz899)} + "
        f"{format_formula_amount(non_reporting_funds.domestic_dividend_kest_kz899)} = "
        f"{format_display_amount(result.domestic_dividend_kest_kz899_total)}\n\n"
        "#### Anzurechnende ausländische\n"
        "(Quellen) Steuer auf Einkünfte, die dem besonderen Steuersatz von 27,5% unterliegen = "
        f"min({format_formula_amount(core.creditable_foreign_tax)} + "
        f"{format_formula_amount(reporting_funds.creditable_foreign_tax)} + "
        f"{format_formula_amount(non_reporting_funds.creditable_foreign_tax)}, "
        f"{format_formula_amount(result.foreign_tax_ceiling)}) = "
        f"{format_display_amount(result.final_creditable_foreign_tax)}\n\n"
        "## Formula Notes\n"
        f"- positive total for 27.5% base = {format_formula_amount(result.positive_total)}\n"
        f"- loss total applied = {format_formula_amount(abs(result.trade_loss_total))}\n"
        f"- final post-loss base = max({format_formula_amount(result.positive_total)} - {format_formula_amount(abs(result.trade_loss_total))}, 0) = {format_formula_amount(result.post_loss_base)}\n"
        f"- foreign-tax ceiling = {format_formula_amount(result.post_loss_base)} * 0.275 = {format_formula_amount(result.foreign_tax_ceiling)}\n"
        f"- source-level creditable foreign tax sum = {format_formula_amount(result.creditable_foreign_tax_source_sum)}\n"
        "- KZ 899 stays separate from foreign-tax credit and is not included in the foreign-tax ceiling\n"
    )
