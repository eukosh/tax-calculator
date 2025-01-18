from datetime import date

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.utils import calculate_kest, convert_to_euro, extract_elements, join_exchange_rates, read_xml_to_df

dates = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 6), date(2024, 1, 10), date(2024, 1, 10)]
dividend_currencies = ["USD", "USD", "USD", "GBP", "EUR"]
dividends_euro = [100.0, 100.0, 200.0, 300.0, 100.0]
withholding_tax_euro = [0.0, 15.0, 60.0, 15.0, 9.0]  # assuming tax rate 0%, 15%, 30%, 5%, 9%


@pytest.fixture
def dividends_euro_df():
    return pl.DataFrame(
        {
            Column.date: dates,
            Column.currency: dividend_currencies,
            Column.profit_euro: dividends_euro,
            Column.withholding_tax_euro: withholding_tax_euro,
        }
    )


@pytest.fixture
def exhange_rates_df():
    return pl.DataFrame(
        {
            Column.rate_date: [
                date(2024, 1, 1),
                date(2024, 1, 3),
                date(2024, 1, 5),
                date(2024, 1, 10),
                date(2024, 1, 25),
            ],
            Column.currency: ["USD", "USD", "USD", "GBP", "USD"],
            Column.exchange_rate: [1.03, 1.02, 1.1, 1.4, 1.1],
        }
    )


@pytest.mark.parametrize(
    "expected_df,tax_withheld_col",
    [
        (
            pl.DataFrame(
                {
                    Column.date: dates,
                    Column.currency: dividend_currencies,
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5, 27.5],
                    Column.kest_net: [27.5, 12.5, 25.0, 67.5, 18.5],
                    Column.profit_euro_net: [72.5, 72.5, 115.0, 217.5, 72.5],
                }
            ),
            Column.withholding_tax_euro,
        ),
        (
            pl.DataFrame(
                {
                    Column.date: dates,
                    Column.currency: dividend_currencies,
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5, 27.5],
                    Column.kest_net: [27.5, 27.5, 55.0, 82.5, 27.5],
                    Column.profit_euro_net: [72.5, 72.5, 145.0, 217.5, 72.5],
                }
            ),
            None,
        ),
    ],
)
def test_calculate_kest(dividends_euro_df, expected_df, tax_withheld_col):
    # Perform the calculation
    result = calculate_kest(dividends_euro_df, amount_col=Column.profit_euro, tax_withheld_col=tax_withheld_col)

    assert_frame_equal(expected_df, result)


XML_CONTENT_1 = """\
<root>
    <record id="1" name="test1" value="10"/>
</root>
"""

XML_CONTENT_2 = """\
<root>
    <record id="2" name="test2" value="20"/>
</root>
"""


@pytest.mark.parametrize(
    "files_info, wildcard_pattern, expected_data",
    [
        # Scenario 1: Single file
        ([("single.xml", XML_CONTENT_1)], "single.xml", [{"id": "1", "name": "test1", "value": "10"}]),
        # Scenario 2: Multiple files
        (
            [
                ("multi1.xml", XML_CONTENT_1),
                ("multi2.xml", XML_CONTENT_2),
            ],
            "multi*.xml",
            [
                {"id": "1", "name": "test1", "value": "10"},
                {"id": "2", "name": "test2", "value": "20"},
            ],
        ),
    ],
)
def test_read_xml_to_df_param(tmp_path, files_info, wildcard_pattern, expected_data):
    """
    A single, parametrized test function that covers both single-file and multi-file scenarios.
    """
    # Arrange: Create the temporary XML files
    for filename, content in files_info:
        (tmp_path / filename).write_text(content)

    df = read_xml_to_df(str(tmp_path / wildcard_pattern), lambda root: extract_elements(root, "record"))

    expected_df = pl.DataFrame(expected_data)
    assert_frame_equal(df, expected_df)


@pytest.mark.parametrize(
    "dividend_date",
    [
        # Close to the rate date in the future, but far too from the exchange rate date in the past
        date(2024, 1, 20),
        # Far from any date
        date(2024, 2, 22),
    ],
)
def test_join_exchange_rates_unmatched_record_error(dividend_date, dividends_euro_df, exhange_rates_df):
    """
    Test that join_exchange_rates raises a ValueError when some dates do not match.
    """
    print(f"Dividend date: {dividend_date}")
    dividends_euro_df = dividends_euro_df.vstack(
        pl.DataFrame(
            {
                Column.date: [dividend_date],
                Column.currency: ["USD"],
                Column.profit_euro: [100.0],
                Column.withholding_tax_euro: [18.0],
            }
        )
    )

    with pytest.raises(ValueError, match="Some dates did not match. See the logs above."):
        join_exchange_rates(dividends_euro_df, exhange_rates_df, df_date_col=Column.date)


def test_join_exchange_rates_unsupported_currency_error(dividends_euro_df, exhange_rates_df):
    dividends_euro_df = dividends_euro_df.vstack(
        pl.DataFrame(
            {
                Column.date: [date(2024, 1, 1)],
                Column.currency: ["UAH"],
                Column.profit_euro: [100.0],
                Column.withholding_tax_euro: [20.0],
            }
        )
    )

    with pytest.raises(ValueError, match="rates_df is missing the following currencies: {'UAH'}"):
        join_exchange_rates(dividends_euro_df, exhange_rates_df, df_date_col=Column.date)


def test_join_exchange_rates(dividends_euro_df, exhange_rates_df):
    res_df = join_exchange_rates(dividends_euro_df, exhange_rates_df, df_date_col=Column.date).sort(
        Column.date, Column.currency
    )

    expected_df = pl.DataFrame(
        {
            Column.date: dates,
            Column.currency: dividend_currencies,
            Column.profit_euro: dividends_euro,
            Column.withholding_tax_euro: withholding_tax_euro,
            Column.rate_date: [date(2024, 1, 1), date(2024, 1, 3), date(2024, 1, 5), date(2024, 1, 10), None],
            Column.exchange_rate: [1.03, 1.02, 1.1, 1.4, None],
        }
    ).sort(Column.date, Column.currency)

    assert_frame_equal(expected_df, res_df)


@pytest.mark.parametrize(
    "col_to_convert,converted_cols",
    [
        (
            Column.amount,
            {Column.amount_euro: [97.0874, 117.6471, 400.0, 392.1569]},
        ),
        (
            [Column.amount, Column.withholding_tax],
            {
                Column.amount_euro: [97.0874, 117.6471, 400.0, 392.1569],
                Column.withholding_tax_euro: [1.9417, 1.1765, 10.0, 19.6078],
            },
        ),
    ],
)
def test_convert_to_euro(col_to_convert, converted_cols: dict):
    currencies = ["USD", "GBP", "EUR", "USD"]
    amounts = [100.0, 100.0, 400.0, 400.0]
    tax = [2.0, 1.0, 10.0, 20.0]
    rates = [1.03, 0.85, None, 1.02]
    df = pl.DataFrame(
        {
            Column.currency: currencies,
            Column.amount: amounts,
            Column.withholding_tax: tax,
            Column.exchange_rate: rates,
        }
    )

    expected_df = pl.DataFrame(
        {
            Column.currency: currencies,
            Column.amount: amounts,
            Column.withholding_tax: tax,
            Column.exchange_rate: rates,
            **converted_cols,
        }
    )
    res_df = convert_to_euro(df, col_to_convert)

    assert_frame_equal(expected_df, res_df)
