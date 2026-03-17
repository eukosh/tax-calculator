import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.testing.asserts import assert_frame_equal

from src.const import Column
from src.providers.freedom import process_freedom_statement

REPORTING_PERIOD_START_DATE = date(2024, 1, 1)
REPORTING_PERIOD_END_DATE = date(2024, 12, 31)


def _rates_df(*rows: tuple[date, str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            Column.rate_date: [row[0] for row in rows],
            Column.currency: [row[1] for row in rows],
            Column.exchange_rate: [row[2] for row in rows],
        }
    )


def _corporate_action(
    *,
    event_date: str,
    ex_date: str,
    type_id: str,
    corporate_action_id: str,
    ticker: str,
    amount: float,
    tax_amount: str,
    currency: str = "USD",
) -> dict:
    return {
        "date": event_date,
        "type": "Dividends",
        "type_id": type_id,
        "corporate_action_id": corporate_action_id,
        "amount": amount,
        "amount_per_one": amount,
        "asset_type": " Cash ",
        "ticker": ticker,
        "isin": "US0000000001",
        "currency": currency,
        "ex_date": ex_date,
        "external_tax": 0,
        "external_tax_currency": currency,
        "tax_amount": tax_amount,
        "tax_currency": currency,
        "comment": "",
        "q_on_ex_date": "1.00000000",
    }


def _trade(
    *,
    short_date: str,
    operation: str,
    instr_nm: str,
    curr_c: str,
    fifo_profit: str,
) -> dict:
    return {
        "trade_id": 1,
        "date": f"{short_date} 12:00:00",
        "short_date": short_date,
        "pay_d": short_date,
        "instr_nm": instr_nm,
        "instr_type": 1,
        "instr_kind": "stock",
        "issue_nb": "US0000000001",
        "operation": operation,
        "p": 10.0,
        "curr_c": curr_c,
        "q": 1,
        "summ": 10.0,
        "turnover": "0.00000000",
        "profit": 0,
        "fifo_profit": fifo_profit,
        "repo_operation": None,
        "mkt_id": 1,
        "order_id": "1",
        "office": 35,
        "yield": None,
        "commission": 0,
        "commission_currency": curr_c,
        "comment": "",
        "transaction_id": 1,
        "isin": "US0000000001",
        "offbalance": 0,
        "otc": 0,
        "is_dvp": 0,
        "stamp_tax": None,
        "smat": 0,
        "forts_exchange_fee": None,
        "trade_nb": "1",
        "broker": "DAS.FFEU",
        "das_exe_id": "1",
        "market": None,
        "mkt_name": "NYSE/NASDAQ",
        "id": "1/1",
    }


def _statement_path(tmp_path: Path, corporate_actions: list[dict], trades: list[dict] | None = None) -> str:
    statement = {"corporate_actions": {"detailed": corporate_actions}}
    if trades is not None:
        statement["trades"] = {"detailed": trades}

    path = tmp_path / "freedom_statement.json"
    path.write_text(json.dumps(statement))
    return str(path)


def _stock_award(*, ticker: str, quantity: float, date_time: str, transaction_id: int = 10) -> dict:
    return {
        "type": "stock_award",
        "ticker": ticker,
        "quantity": f"{quantity:.8f}",
        "date_created": f"{date_time}.000000",
        "datetime": date_time,
        "transaction_id": transaction_id,
        "cost": "0.00000000",
        "fifo_profit": "0.00000000",
        "commission": "0.00000000",
        "commission_currency": None,
        "comment": "Promo stock",
    }


def _statement_path_with_awards(
    tmp_path: Path,
    corporate_actions: list[dict],
    trades: list[dict] | None = None,
    awards: list[dict] | None = None,
) -> str:
    statement = {"corporate_actions": {"detailed": corporate_actions}}
    if trades is not None:
        statement["trades"] = {"detailed": trades}
    if awards is not None:
        statement["securities_in_outs"] = awards

    path = tmp_path / "freedom_statement.json"
    path.write_text(json.dumps(statement))
    return str(path)


def test_process_freedom_statement_uses_ex_date_for_fx_matching(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2023-12-27",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="2023-06-03_35_TLT.US_7.6923",
                ticker="TLT.US",
                amount=100.0,
                tax_amount="-",
            )
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends"],
            Column.currency: ["USD"],
            Column.profit_total: [100.0],
            Column.profit_euro_total: [100.0],
            Column.profit_euro_net_total: [72.5],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [27.5],
            Column.kest_net_total: [27.5],
        }
    )

    assert_frame_equal(res_df, expected_df)


def test_process_freedom_statement_splits_dividends_using_type_mapping(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0))
    mapping_path = tmp_path / "dividend_type_mapping.csv"
    mapping_path.write_text("ticker,type\nTLT.US,etf_dividends\nAAPL.US,dividends\n")
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="div_1",
                ticker="TLT.US",
                amount=10.0,
                tax_amount="-",
            ),
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="div_2",
                ticker="AAPL.US",
                amount=20.0,
                tax_amount="-3.0",
            ),
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
        dividend_type_mapping_file=str(mapping_path),
    ).sort(Column.type)

    expected_df = pl.DataFrame(
        {
            Column.type: ["ETF div", "dividends"],
            Column.currency: ["USD", "USD"],
            Column.profit_total: [10.0, 23.0],
            Column.profit_euro_total: [10.0, 23.0],
            Column.profit_euro_net_total: [7.25, 16.675],
            Column.withholding_tax_euro_total: [0.0, 3.0],
            Column.kest_gross_total: [2.75, 6.325],
            Column.kest_net_total: [2.75, 3.325],
        }
    ).sort(Column.type)

    assert_frame_equal(res_df, expected_df)


def test_process_freedom_statement_requires_mapping_for_all_dividend_tickers(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0))
    mapping_path = tmp_path / "dividend_type_mapping.csv"
    mapping_path.write_text("ticker,type\nTLT.US,etf_dividends\n")
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="div_1",
                ticker="TLT.US",
                amount=10.0,
                tax_amount="-",
            ),
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="div_2",
                ticker="AAPL.US",
                amount=20.0,
                tax_amount="-",
            ),
        ],
    )

    with pytest.raises(ValueError, match="Unmapped Freedom dividend tickers: \\['AAPL.US'\\]"):
        process_freedom_statement(
            statement_path,
            rates_df,
            start_date=REPORTING_PERIOD_START_DATE,
            end_date=REPORTING_PERIOD_END_DATE,
            dividend_type_mapping_file=str(mapping_path),
        )


def test_process_freedom_statement_handles_tlt_reversal_with_corrected_dividend(tmp_path):
    rates_df = _rates_df((date(2024, 12, 2), "USD", 1.0))
    corporate_action_id = "2024-12-02_35_TLT.US_0.325021"
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2024-12-20",
                ex_date="2024-12-02",
                type_id="dividend",
                corporate_action_id=corporate_action_id,
                ticker="TLT.US",
                amount=10.0,
                tax_amount="-1.5",
            ),
            _corporate_action(
                event_date="2025-02-06",
                ex_date="2024-12-02",
                type_id="dividend_reverted",
                corporate_action_id=corporate_action_id,
                ticker="TLT.US",
                amount=-10.0,
                tax_amount="1.5",
            ),
            _corporate_action(
                event_date="2025-02-06",
                ex_date="2024-12-02",
                type_id="dividend",
                corporate_action_id=corporate_action_id,
                ticker="TLT.US",
                amount=11.5,
                tax_amount="-",
            ),
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends"],
            Column.currency: ["USD"],
            Column.profit_total: [11.5],
            Column.profit_euro_total: [11.5],
            Column.profit_euro_net_total: [8.3375],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [3.1625],
            Column.kest_net_total: [3.1625],
        }
    )
    assert_frame_equal(res_df, expected_df)


def test_process_freedom_statement_applies_exclusion_file(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="exclude_me",
                ticker="SCHD.US",
                amount=100.0,
                tax_amount="-",
            ),
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="keep_me",
                ticker="SCHD.US",
                amount=50.0,
                tax_amount="-",
            ),
        ],
    )
    exclusions_path = tmp_path / "exclude.csv"
    exclusions_path.write_text("corporate_action_id\nexclude_me\n")

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
        exclude_corporate_action_ids_file=str(exclusions_path),
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["dividends"],
            Column.currency: ["USD"],
            Column.profit_total: [50.0],
            Column.profit_euro_total: [50.0],
            Column.profit_euro_net_total: [36.25],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [13.75],
            Column.kest_net_total: [13.75],
        }
    )

    assert_frame_equal(res_df, expected_df)


@pytest.mark.parametrize(
    ("fifo_profit", "expected_profit_euro_total"),
    [
        ("110.0", 100.0),
        ("-55.0", -50.0),
    ],
)
def test_process_freedom_statement_includes_trade_summary(
    tmp_path,
    fifo_profit,
    expected_profit_euro_total,
):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0), (date(2024, 6, 10), "USD", 1.1))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[
            _corporate_action(
                event_date="2024-06-10",
                ex_date="2024-06-03",
                type_id="dividend",
                corporate_action_id="div_1",
                ticker="SCHD.US",
                amount=10.0,
                tax_amount="-",
            ),
        ],
        trades=[
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="AAPL.US",
                curr_c="USD",
                fifo_profit=fifo_profit,
            ),
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="EUR/USD",
                curr_c="USD",
                fifo_profit="1100.0",
            ),
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )
    trades_df = res_df.filter(pl.col(Column.type).str.starts_with("trades"))

    expected_df = pl.DataFrame(
        {
            Column.type: ["trades profit" if expected_profit_euro_total >= 0 else "trades loss"],
            Column.currency: ["EUR"],
            Column.profit_total: [expected_profit_euro_total],
            Column.profit_euro_total: [expected_profit_euro_total],
            Column.profit_euro_net_total: [expected_profit_euro_total],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.0],
            Column.kest_net_total: [0.0],
        }
    )

    assert trades_df.height == 1
    assert_frame_equal(trades_df, expected_df)


def test_process_freedom_statement_uses_profit_when_fifo_profit_is_zero_for_award_shares(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0), (date(2024, 6, 10), "USD", 1.1))
    statement_path = _statement_path_with_awards(
        tmp_path=tmp_path,
        corporate_actions=[],
        trades=[
            {
                **_trade(
                    short_date="2024-06-10",
                    operation="sell",
                    instr_nm="AAPL.US",
                    curr_c="USD",
                    fifo_profit="0.00000000",
                ),
                "profit": 110.0,
            },
        ],
        awards=[
            _stock_award(
                ticker="AAPL.US",
                quantity=1,
                date_time="2024-06-09 10:00:00",
            )
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["trades profit"],
            Column.currency: ["EUR"],
            Column.profit_total: [100.0],
            Column.profit_euro_total: [100.0],
            Column.profit_euro_net_total: [100.0],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [0.0],
            Column.kest_net_total: [0.0],
        }
    )

    assert_frame_equal(res_df, expected_df)


def test_process_freedom_statement_ignores_profit_when_fifo_profit_is_zero_without_award_match(tmp_path):
    rates_df = _rates_df((date(2024, 6, 3), "USD", 1.0), (date(2024, 6, 10), "USD", 1.1))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[],
        trades=[
            {
                **_trade(
                    short_date="2024-06-10",
                    operation="sell",
                    instr_nm="AAPL.US",
                    curr_c="USD",
                    fifo_profit="0.00000000",
                ),
                "profit": 110.0,
            },
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )

    assert res_df.is_empty()


def test_process_freedom_statement_separates_trade_profit_and_loss_by_default(tmp_path):
    rates_df = _rates_df((date(2024, 6, 10), "USD", 1.1))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[],
        trades=[
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="AAPL.US",
                curr_c="USD",
                fifo_profit="110.0",
            ),
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="MSFT.US",
                curr_c="USD",
                fifo_profit="-55.0",
            ),
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["trades loss", "trades profit"],
            Column.currency: ["EUR", "EUR"],
            Column.profit_total: [-50.0, 100.0],
            Column.profit_euro_total: [-50.0, 100.0],
            Column.profit_euro_net_total: [-50.0, 100.0],
            Column.withholding_tax_euro_total: [0.0, 0.0],
            Column.kest_gross_total: [0.0, 0.0],
            Column.kest_net_total: [0.0, 0.0],
        }
    ).sort(Column.type)

    assert_frame_equal(res_df.sort(Column.type), expected_df)


def test_process_freedom_statement_can_disable_separate_trade_profit_loss_reporting(tmp_path):
    rates_df = _rates_df((date(2024, 6, 10), "USD", 1.1))
    statement_path = _statement_path(
        tmp_path=tmp_path,
        corporate_actions=[],
        trades=[
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="AAPL.US",
                curr_c="USD",
                fifo_profit="110.0",
            ),
            _trade(
                short_date="2024-06-10",
                operation="sell",
                instr_nm="MSFT.US",
                curr_c="USD",
                fifo_profit="-55.0",
            ),
        ],
    )

    res_df = process_freedom_statement(
        statement_path,
        rates_df,
        start_date=REPORTING_PERIOD_START_DATE,
        end_date=REPORTING_PERIOD_END_DATE,
        separate_trade_profit_loss=False,
    )

    expected_df = pl.DataFrame(
        {
            Column.type: ["trades"],
            Column.currency: ["EUR"],
            Column.profit_total: [50.0],
            Column.profit_euro_total: [50.0],
            Column.profit_euro_net_total: [36.25],
            Column.withholding_tax_euro_total: [0.0],
            Column.kest_gross_total: [13.75],
            Column.kest_net_total: [13.75],
        }
    )

    assert_frame_equal(res_df, expected_df)
