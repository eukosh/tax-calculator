from __future__ import annotations

import glob
from collections import defaultdict
from decimal import Decimal
from datetime import date, datetime
from pathlib import Path

import lxml.etree as etree

from scripts.reporting_funds.models import (
    BrokerDividendEvent,
    IbkrCashDividendRow,
    IbkrDividendAccrualRow,
    IbkrTrade,
    round_money,
    round_qty,
)

RAW_IBKR_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_trade_datetime(row: etree._Element) -> datetime:
    raw_value = row.get("dateTime") or row.get("tradeDate")
    if raw_value is None:
        raise ValueError("IBKR trade row is missing dateTime/tradeDate")
    if len(raw_value) == 10:
        raw_value = f"{raw_value} 00:00:00"
    return datetime.strptime(raw_value, RAW_IBKR_DATETIME_FORMAT)


def _parse_optional_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    normalized = raw_value.strip()
    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported IBKR date value: {raw_value!r}")


def _parse_optional_float(raw_value: str | None) -> Decimal | None:
    if raw_value is None or raw_value == "":
        return None
    return round_money(Decimal(raw_value))


def _resolve_file_paths(xml_file_path: str) -> list[str]:
    path = Path(xml_file_path)
    if path.exists():
        if path.is_dir():
            return sorted(str(candidate) for candidate in path.glob("*.xml"))
        return [str(path)]
    return sorted(glob.glob(xml_file_path))


def _iter_unique_rows(xml_file_path: str, parent_tags: tuple[str, ...], row_tags: set[str]) -> list[tuple[str, etree._Element]]:
    file_paths = _resolve_file_paths(xml_file_path)
    if not file_paths:
        raise FileNotFoundError(f"No files matched the pattern: {xml_file_path}")

    seen_rows: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    unique_rows: list[tuple[str, etree._Element]] = []
    for path in file_paths:
        root = etree.parse(path).getroot()
        for parent_tag in parent_tags:
            for parent in root.findall(parent_tag):
                for row in parent:
                    if row.tag not in row_tags:
                        continue
                    row_key = (row.tag, tuple(sorted(row.items())))
                    if row_key in seen_rows:
                        continue
                    seen_rows.add(row_key)
                    unique_rows.append((path, row))
    return unique_rows


def load_ibkr_etf_trades(xml_file_path: str, require_raw_trades: bool) -> list[IbkrTrade]:
    raw_trades: list[IbkrTrade] = []
    closed_lot_rows_detected = False

    for path, row in _iter_unique_rows(
        xml_file_path,
        parent_tags=(".//TradeConfirms", ".//Trades"),
        row_tags={"TradeConfirm", "Trade", "Lot"},
    ):
        asset_category = (row.get("assetCategory") or "").strip()
        if asset_category == "CASH":
            continue

        sub_category = (row.get("subCategory") or "").strip()
        if sub_category != "ETF":
            if row.tag == "Lot" and (row.get("levelOfDetail") or "") == "CLOSED_LOT":
                closed_lot_rows_detected = True
            continue

        level_of_detail = (row.get("levelOfDetail") or "").strip()
        if row.tag == "Lot" and level_of_detail == "CLOSED_LOT":
            closed_lot_rows_detected = True
            continue

        buy_sell = (row.get("buySell") or "").strip().upper()
        if buy_sell not in {"BUY", "SELL"}:
            continue

        trade_datetime = _parse_trade_datetime(row)
        trade_price = row.get("tradePrice") or row.get("price")
        quantity = row.get("quantity")
        currency = row.get("currency")
        symbol = row.get("symbol")
        isin = row.get("isin") or row.get("securityID")
        if not all([trade_price, quantity, currency, symbol, isin]):
            raise ValueError("IBKR raw ETF trade row is missing required fields")

        trade_id = (
            row.get("tradeID")
            or row.get("transactionID")
            or row.get("ibOrderID")
            or f"{symbol}:{trade_datetime.isoformat()}:{buy_sell}"
        )

        raw_trades.append(
            IbkrTrade(
                ticker=symbol.strip(),
                isin=isin.strip(),
                trade_date=trade_datetime.date(),
                trade_datetime=trade_datetime,
                operation=buy_sell.lower(),
                quantity=round_qty(abs(Decimal(quantity))),
                price_ccy=round_money(Decimal(trade_price)),
                currency=currency.strip(),
                trade_id=trade_id.strip(),
                account_id=(row.get("accountId") or "").strip(),
                source_statement_file=path,
            )
        )

    raw_trades.sort(key=lambda trade: (trade.trade_datetime, trade.trade_id, trade.operation))

    if require_raw_trades and not raw_trades:
        if closed_lot_rows_detected:
            raise ValueError(
                "IBKR file contains only closed lots for ETFs. Initial reporting-fund bootstrap requires raw BUY/SELL trade history."
            )
        raise ValueError("No raw ETF BUY/SELL trades found in the IBKR file.")

    return raw_trades


def load_ibkr_etf_dividend_accrual_rows(xml_file_path: str) -> list[IbkrDividendAccrualRow]:
    rows: list[IbkrDividendAccrualRow] = []
    for path, row in _iter_unique_rows(
        xml_file_path,
        parent_tags=(".//ChangeInDividendAccruals",),
        row_tags={"ChangeInDividendAccrual"},
    ):
        if (row.get("subCategory") or "").strip() != "ETF":
            continue

        ticker = (row.get("symbol") or "").strip()
        isin = (row.get("isin") or row.get("securityID") or "").strip()
        currency = (row.get("currency") or "").strip()
        report_date = _parse_optional_date(row.get("reportDate"))
        effective_date = _parse_optional_date(row.get("date"))
        pay_date = _parse_optional_date(row.get("payDate"))
        quantity = row.get("quantity")
        if not all([ticker, isin, currency, report_date, effective_date, pay_date, quantity]):
            raise ValueError("IBKR ETF dividend accrual row is missing required fields")

        rows.append(
            IbkrDividendAccrualRow(
                ticker=ticker,
                isin=isin,
                currency=currency,
                report_date=report_date,
                date=effective_date,
                ex_date=_parse_optional_date(row.get("exDate")),
                pay_date=pay_date,
                quantity=round_qty(Decimal(quantity)),
                tax=_parse_optional_float(row.get("tax")),
                gross_rate=_parse_optional_float(row.get("grossRate")),
                gross_amount=_parse_optional_float(row.get("grossAmount")),
                net_amount=_parse_optional_float(row.get("netAmount")),
                code=(row.get("code") or "").strip(),
                action_id=(row.get("actionID") or "").strip(),
                account_id=(row.get("accountId") or "").strip(),
                source_statement_file=path,
            )
        )

    rows.sort(key=lambda item: (item.pay_date or item.report_date, item.report_date, item.action_id, item.code))
    return rows


def load_ibkr_etf_cash_dividend_rows(xml_file_path: str) -> list[IbkrCashDividendRow]:
    rows: list[IbkrCashDividendRow] = []
    for path, row in _iter_unique_rows(
        xml_file_path,
        parent_tags=(".//CashTransactions",),
        row_tags={"CashTransaction"},
    ):
        if (row.get("subCategory") or "").strip() != "ETF":
            continue
        if (row.get("type") or "").strip() != "Dividends":
            continue

        ticker = (row.get("symbol") or "").strip()
        isin = (row.get("isin") or row.get("securityID") or "").strip()
        currency = (row.get("currency") or "").strip()
        settle_date = _parse_optional_date(row.get("settleDate"))
        amount = row.get("amount")
        if not all([ticker, isin, currency, settle_date, amount]):
            raise ValueError("IBKR ETF cash dividend row is missing required fields")

        rows.append(
            IbkrCashDividendRow(
                ticker=ticker,
                isin=isin,
                currency=currency,
                settle_date=settle_date,
                ex_date=_parse_optional_date(row.get("exDate")),
                amount=round_money(Decimal(amount)),
                action_id=(row.get("actionID") or "").strip(),
                account_id=(row.get("accountId") or "").strip(),
                report_date=_parse_optional_date(row.get("reportDate")),
                source_statement_file=path,
            )
        )

    rows = _merge_duplicate_cash_dividend_rows(rows)
    rows.sort(key=lambda item: (item.settle_date, item.action_id, item.isin, item.ticker))
    return rows


def _merge_duplicate_cash_dividend_rows(rows: list[IbkrCashDividendRow]) -> list[IbkrCashDividendRow]:
    grouped: dict[tuple[object, ...], list[IbkrCashDividendRow]] = defaultdict(list)
    for row in rows:
        key = (
            ("action", row.action_id)
            if row.action_id
            else ("fallback", row.isin, row.currency, row.settle_date, row.amount)
        )
        grouped[key].append(row)

    merged_rows: list[IbkrCashDividendRow] = []
    for group_rows in grouped.values():
        if len(group_rows) == 1:
            merged_rows.append(group_rows[0])
            continue

        canonical_row = group_rows[0]
        for other_row in group_rows[1:]:
            if (
                other_row.isin != canonical_row.isin
                or other_row.currency != canonical_row.currency
                or other_row.settle_date != canonical_row.settle_date
                or other_row.amount != canonical_row.amount
            ):
                raise ValueError(
                    "Semantically duplicated ETF cash dividend rows disagree on "
                    "ISIN/currency/settle date/amount and cannot be merged."
                )
            if (
                canonical_row.ex_date is not None
                and other_row.ex_date is not None
                and canonical_row.ex_date != other_row.ex_date
            ):
                raise ValueError(
                    "Semantically duplicated ETF cash dividend rows disagree on ex-date and cannot be merged."
                )

        richest_row = max(group_rows, key=_cash_dividend_row_specificity)
        ex_date = next((row.ex_date for row in group_rows if row.ex_date is not None), None)
        report_date = next((row.report_date for row in group_rows if row.report_date is not None), None)
        account_id = next((row.account_id for row in group_rows if row.account_id and row.account_id != "-"), "")
        source_statement_file = "|".join(sorted({row.source_statement_file for row in group_rows if row.source_statement_file}))

        merged_rows.append(
            IbkrCashDividendRow(
                ticker=richest_row.ticker,
                isin=canonical_row.isin,
                currency=canonical_row.currency,
                settle_date=canonical_row.settle_date,
                ex_date=ex_date,
                amount=canonical_row.amount,
                action_id=canonical_row.action_id,
                account_id=account_id,
                report_date=report_date,
                source_statement_file=source_statement_file,
            )
        )

    return merged_rows


def _cash_dividend_row_specificity(row: IbkrCashDividendRow) -> tuple[int, int, int, int]:
    return (
        1 if row.ex_date is not None else 0,
        1 if row.account_id and row.account_id != "-" else 0,
        1 if row.report_date is not None else 0,
        len(row.ticker),
    )


def collapse_dividend_accrual_rows(rows: list[IbkrDividendAccrualRow], tax_year: int | None = None) -> list[BrokerDividendEvent]:
    grouped: dict[tuple[object, ...], list[IbkrDividendAccrualRow]] = defaultdict(list)
    for row in rows:
        key = (
            ("action", row.action_id)
            if row.action_id
            else ("fallback", row.isin, row.ex_date, row.pay_date, round_qty(row.quantity), row.gross_rate)
        )
        grouped[key].append(row)

    events: list[BrokerDividendEvent] = []
    for group_rows in grouped.values():
        po_row = next((row for row in group_rows if row.code == "Po"), None)
        re_row = next((row for row in group_rows if row.code == "Re"), None)
        base_row = re_row or po_row or group_rows[0]
        if base_row.pay_date is None:
            raise ValueError("Collapsed ETF dividend accrual event is missing pay date")
        if tax_year is not None and base_row.pay_date.year != tax_year:
            continue
        group_notes: list[str] = []
        if len({row.ticker for row in group_rows}) > 1:
            group_notes.append("accrual symbol drift present")

        def _abs_or_none(value: float | None) -> float | None:
            return round_money(abs(value)) if value is not None else None

        events.append(
            BrokerDividendEvent(
                ticker=base_row.ticker,
                isin=base_row.isin,
                currency=base_row.currency,
                ex_date=base_row.ex_date,
                pay_date=base_row.pay_date,
                quantity=base_row.quantity,
                gross_rate=_abs_or_none(base_row.gross_rate),
                gross_amount=_abs_or_none(re_row.gross_amount if re_row and re_row.gross_amount is not None else base_row.gross_amount),
                net_amount=_abs_or_none(re_row.net_amount if re_row and re_row.net_amount is not None else base_row.net_amount),
                tax=_abs_or_none(re_row.tax if re_row and re_row.tax is not None else base_row.tax),
                has_po=po_row is not None,
                has_re=re_row is not None,
                action_id=base_row.action_id,
                matching_notes="; ".join(group_notes),
                source_statement_file=base_row.source_statement_file,
            )
        )

    events.sort(key=lambda event: (event.pay_date, event.ticker, event.event_id))
    return events


def build_broker_dividend_events(
    accrual_rows: list[IbkrDividendAccrualRow],
    cash_rows: list[IbkrCashDividendRow],
    *,
    tax_year: int | None = None,
    amount_tolerance: float = 0.02,
) -> list[BrokerDividendEvent]:
    accrual_events = collapse_dividend_accrual_rows(accrual_rows, tax_year=None)
    cash_by_key: dict[tuple[object, ...], list[IbkrCashDividendRow]] = defaultdict(list)
    for row in cash_rows:
        key = ("action", row.action_id) if row.action_id else ("fallback", row.isin, row.ex_date, row.settle_date)
        cash_by_key[key].append(row)

    events: list[BrokerDividendEvent] = []
    matched_cash_keys: set[tuple[object, ...]] = set()
    for event in accrual_events:
        accrual_key = (
            ("action", event.action_id)
            if event.action_id
            else ("fallback", event.isin, event.ex_date, event.pay_date)
        )
        candidate_cash_rows = cash_by_key.get(accrual_key, [])
        if not candidate_cash_rows and not event.action_id:
            candidate_cash_rows = [
                row
                for row in cash_rows
                if row.isin == event.isin
                and row.settle_date == event.pay_date
                and (event.ex_date is None or row.ex_date is None or row.ex_date == event.ex_date)
            ]
        if len(candidate_cash_rows) > 1:
            raise ValueError(
                f"Multiple ETF cash dividend rows matched broker payout {event.event_id} ({event.ticker} {event.pay_date})."
            )

        cash_row = candidate_cash_rows[0] if candidate_cash_rows else None
        if cash_row is not None:
            matched_cash_keys.add(
                ("action", cash_row.action_id) if cash_row.action_id else ("fallback", cash_row.isin, cash_row.ex_date, cash_row.settle_date)
            )
            if cash_row.isin != event.isin:
                raise ValueError(f"Cash/accrual ISIN mismatch for broker payout {event.event_id}.")
            if cash_row.settle_date != event.pay_date:
                raise ValueError(f"Cash/accrual pay-date mismatch for broker payout {event.event_id}.")
            if event.ex_date and cash_row.ex_date and event.ex_date != cash_row.ex_date:
                raise ValueError(f"Cash/accrual ex-date mismatch for broker payout {event.event_id}.")

            accrual_amount = round_money(abs(event.gross_amount or 0.0))
            cash_amount = round_money(abs(cash_row.amount))
            if accrual_amount and abs(accrual_amount - cash_amount) > amount_tolerance:
                raise ValueError(
                    f"Cash/accrual gross amount mismatch for broker payout {event.event_id}: "
                    f"accrual {accrual_amount} vs cash {cash_amount}"
                )
            notes = "matched by actionID" if event.action_id else "matched by fallback tuple"
            if event.matching_notes:
                notes = f"{notes}; {event.matching_notes}"
            if event.ticker != cash_row.ticker or "symbol drift" in event.matching_notes:
                notes = f"{notes}; symbol drift ignored because ISIN/actionID matched"
            source_file = "|".join(sorted({event.source_statement_file, cash_row.source_statement_file}))
            events.append(
                BrokerDividendEvent(
                    ticker=cash_row.ticker or event.ticker,
                    isin=event.isin,
                    currency=event.currency,
                    ex_date=event.ex_date or cash_row.ex_date,
                    pay_date=event.pay_date,
                    quantity=event.quantity,
                    gross_rate=event.gross_rate,
                    gross_amount=cash_amount,
                    net_amount=round_money(event.net_amount or cash_amount),
                    tax=round_money(event.tax or 0.0),
                    has_po=event.has_po,
                    has_re=event.has_re,
                    action_id=event.action_id or cash_row.action_id,
                    source_statement_file=source_file,
                    cash_amount=cash_amount,
                    accrual_amount=accrual_amount,
                    matching_notes=notes,
                    evidence_state="confirmed_cash",
                )
            )
            continue

        evidence_state = "accrual_realized_cash_missing" if event.has_re else "accrual_pre_payout_only"
        events.append(
            BrokerDividendEvent(
                ticker=event.ticker,
                isin=event.isin,
                currency=event.currency,
                ex_date=event.ex_date,
                pay_date=event.pay_date,
                quantity=event.quantity,
                gross_rate=event.gross_rate,
                gross_amount=round_money(abs(event.gross_amount or 0.0)),
                net_amount=round_money(abs(event.net_amount or 0.0)),
                tax=round_money(abs(event.tax or 0.0)),
                has_po=event.has_po,
                has_re=event.has_re,
                action_id=event.action_id,
                source_statement_file=event.source_statement_file,
                cash_amount=None,
                accrual_amount=round_money(abs(event.gross_amount or 0.0)),
                matching_notes="cash_row_missing",
                evidence_state=evidence_state,
            )
        )

    unmatched_cash_rows = [
        row
        for key, cash_group in cash_by_key.items()
        if key not in matched_cash_keys
        for row in cash_group
    ]
    if unmatched_cash_rows:
        examples = ", ".join(
            f"{row.ticker} {row.isin} {row.settle_date.isoformat()} actionID={row.action_id or '-'}"
            for row in unmatched_cash_rows[:3]
        )
        raise ValueError(f"Unmatched ETF cash dividend rows were found: {examples}")

    if tax_year is not None:
        events = [event for event in events if event.pay_date.year == tax_year]

    events.sort(key=lambda event: (event.pay_date, event.ticker, event.event_id))
    return events
