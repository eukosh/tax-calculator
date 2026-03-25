from __future__ import annotations

from decimal import Decimal

from scripts.manual_e1kv_input.workflow import (
    CoreInputs,
    ReportingFundsInputs,
    build_e1kv_computation,
    parse_money_input,
    render_output_markdown,
)


def test_build_e1kv_computation_applies_foreign_tax_ceiling_and_keeps_kz899_separate() -> None:
    result = build_e1kv_computation(
        core=CoreInputs(
            ordinary_capital_income=Decimal("840.1220"),
            trade_profit=Decimal("15.4604"),
            trade_loss=Decimal("-1350.8942"),
            fund_distributions=Decimal("291.7672"),
            creditable_foreign_tax=Decimal("121.9311"),
        ),
        reporting_funds=ReportingFundsInputs(
            fund_distributions=Decimal("21.453814"),
            deemed_distributed_income=Decimal("62.424798"),
            domestic_dividends_kz189=Decimal("1.2345"),
            domestic_dividend_kest_kz899=Decimal("0.3086"),
            creditable_foreign_tax=Decimal("6.0168"),
        ),
        non_reporting_etf_age=Decimal("1089.5114"),
    )

    assert result.fund_distributions_total == Decimal("313.221014")
    assert result.deemed_distributed_income_total == Decimal("1151.936198")
    assert result.domestic_dividends_kz189_total == Decimal("1.2345")
    assert result.domestic_dividend_kest_kz899_total == Decimal("0.3086")
    assert result.creditable_foreign_tax_source_sum == Decimal("127.9479")
    assert result.post_loss_base == Decimal("971.079912")
    assert result.foreign_tax_ceiling == Decimal("267.046976")
    assert result.final_creditable_foreign_tax == Decimal("127.9479")


def test_build_e1kv_computation_with_non_reporting_reits() -> None:
    result = build_e1kv_computation(
        core=CoreInputs(
            ordinary_capital_income=Decimal("840.1220"),
            trade_profit=Decimal("15.4604"),
            trade_loss=Decimal("-1350.8942"),
            fund_distributions=Decimal("291.7672"),
            reit_distributions=Decimal("133.0915"),
            creditable_foreign_tax=Decimal("121.9311"),
        ),
        reporting_funds=ReportingFundsInputs(
            fund_distributions=Decimal("21.453814"),
            deemed_distributed_income=Decimal("62.424798"),
            creditable_foreign_tax=Decimal("6.0168"),
        ),
        non_reporting_etf_age=Decimal("1089.5114"),
        non_reporting_reit_age=Decimal("300.4264"),
    )

    assert result.fund_distributions_total == Decimal("446.312514")
    assert result.deemed_distributed_income_total == Decimal("1452.362598")
    assert result.creditable_foreign_tax_source_sum == Decimal("127.9479")
    expected_positive = Decimal("840.1220") + Decimal("15.4604") + Decimal("446.312514") + Decimal("1452.362598")
    assert result.positive_total == expected_positive
    expected_post_loss = expected_positive - Decimal("1350.8942")
    assert result.post_loss_base == expected_post_loss


def test_parse_money_input_supports_comma_and_blank() -> None:
    assert parse_money_input("1,2345") == Decimal("1.2345")
    assert parse_money_input("") == Decimal("0")


def test_render_output_markdown_contains_kz189_kz899_and_formula() -> None:
    core = CoreInputs(
        ordinary_capital_income=Decimal("10"),
        trade_profit=Decimal("2"),
        trade_loss=Decimal("-3"),
        fund_distributions=Decimal("4"),
        creditable_foreign_tax=Decimal("1"),
    )
    reporting = ReportingFundsInputs(
        fund_distributions=Decimal("5"),
        deemed_distributed_income=Decimal("6"),
        domestic_dividends_kz189=Decimal("0.5"),
        domestic_dividend_kest_kz899=Decimal("0.2"),
        creditable_foreign_tax=Decimal("0.3"),
    )
    non_reporting_etf_age = Decimal("7")
    result = build_e1kv_computation(
        core=core, reporting_funds=reporting, non_reporting_etf_age=non_reporting_etf_age,
    )

    rendered = render_output_markdown(
        tax_year=2025,
        core=core,
        reporting_funds=reporting,
        non_reporting_etf_age=non_reporting_etf_age,
        result=result,
    )

    assert "Inländische Dividenden im Verlustausgleich (KZ 189)" in rendered
    assert "KESt auf inländische Dividenden (KZ 899)" in rendered
    assert "min(1 + 0.3, 8.6625)" in rendered
