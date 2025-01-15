import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.utils import calculate_kest, extract_elements, read_xml_to_df

dividends_euro = [100.0, 100.0, 200.0, 300.0]
withholding_tax_euro = [0.0, 15.0, 60.0, 15.0]  # assuming tax rate 0%, 15%, 30%, 5%


@pytest.fixture
def dividends_euro_df():
    return pl.DataFrame(
        {
            Column.profit_euro: dividends_euro,
            Column.withholding_tax_euro: withholding_tax_euro,
        }
    )


@pytest.mark.parametrize(
    "expected_df,tax_withheld_col",
    [
        (
            pl.DataFrame(
                {
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5],
                    Column.kest_net: [27.5, 12.5, 25.0, 67.5],
                    Column.profit_euro_net: [72.5, 72.5, 115.0, 217.5],
                }
            ),
            Column.withholding_tax_euro,
        ),
        (
            pl.DataFrame(
                {
                    Column.profit_euro: dividends_euro,
                    Column.withholding_tax_euro: withholding_tax_euro,
                    Column.kest_gross: [27.5, 27.5, 55.0, 82.5],
                    Column.kest_net: [27.5, 27.5, 55.0, 82.5],
                    Column.profit_euro_net: [72.5, 72.5, 145.0, 217.5],
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
