from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal
from datetime import date, datetime
from itertools import combinations
from pathlib import Path

import polars as pl

from scripts.reporting_funds.ibkr_source import (
    build_broker_dividend_events,
    load_ibkr_etf_cash_dividend_rows,
    load_ibkr_etf_dividend_accrual_rows,
    load_ibkr_etf_trades,
)
from scripts.reporting_funds.models import (
    BrokerDividendEvent,
    IbkrTrade,
    OekbReport,
    PayoutStateRow,
    round_money,
    round_qty,
)
from scripts.reporting_funds.oekb_csv import load_matching_oekb_reports, load_required_oekb_reports
from tax_automation.const import EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET
from tax_automation.currencies import ExchangeRates
from tax_automation.moving_average import (
    EVENT_TYPE_AUSTRIAN_BASIS_RESET,
    PositionState,
    aggregate_state_rows,
    apply_event,
    build_basis_adjustment_event,
    build_basis_reset_event,
    build_buy_event,
    build_sell_event,
    clone_states,
    load_position_states,
    position_events_to_df,
    position_states_to_df,
    replay_events,
)
from tax_automation.precision import cast_decimal_columns_to_float, quantize_fx, quantize_money, quantize_qty, to_decimal, to_output_float

NEGATIVE_DEEMED_DISTRIBUTION_IGNORE = "ignore"
NEGATIVE_DEEMED_DISTRIBUTION_APPLY_FULL = "apply_full"
NEGATIVE_DEEMED_DISTRIBUTION_APPLY_PARTIAL = "apply_partial"
NEGATIVE_DEEMED_DISTRIBUTION_BLOCK = "unresolved_block"
NEGATIVE_DEEMED_DISTRIBUTION_APPLIED_AUTO = "applied_auto_reconciled_payout_set"
PAYOUT_EVIDENCE_CONFIRMED_CASH = "confirmed_cash"
PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY = "accrual_pre_payout_only"
PAYOUT_EVIDENCE_REALIZED_CASH_MISSING = "accrual_realized_cash_missing"
PAYOUT_STATUS_UNRESOLVED_OPEN = "unresolved_open"


def build_fx_table(
    start_date: date,
    end_date: date,
    raw_exchange_rates_path: str | Path,
    currencies: tuple[str, ...],
) -> dict[str, tuple[list[date], list[Decimal]]]:
    raw_exchange_rates_path = Path(raw_exchange_rates_path)
    overwrite_rates_cache = raw_exchange_rates_path == Path("data/input/currencies/raw_exchange_rates.csv")
    exchange_rates = ExchangeRates(
        start_date=start_date,
        end_date=end_date,
        currencies=currencies,
        overwrite=overwrite_rates_cache,
        raw_file_path=str(raw_exchange_rates_path),
    )

    rates_df = exchange_rates.get_rates()
    fx_table: dict[str, tuple[list[date], list[Decimal]]] = {}
    for currency in sorted(set(rates_df["currency"].to_list())):
        currency_df = rates_df.filter(pl.col("currency") == currency).sort("rate_date")
        fx_table[currency] = (
            currency_df["rate_date"].to_list(),
            [quantize_fx(value) for value in currency_df["exchange_rate"].to_list()],
        )
    return fx_table


def get_fx_rate(fx_table: dict[str, tuple[list[date], list[Decimal]]], currency: str, event_date: date) -> Decimal:
    if currency == "EUR":
        return Decimal("1")

    if currency not in fx_table:
        raise ValueError(f"Missing FX series for currency {currency}")

    available_dates, available_rates = fx_table[currency]
    index = bisect_right(available_dates, event_date) - 1
    if index < 0:
        raise ValueError(f"No FX rate available for {currency} on or before {event_date}")

    matched_date = available_dates[index]
    if (event_date - matched_date).days > EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET:
        raise ValueError(
            f"Closest FX rate for {currency} on {event_date} is too old: {matched_date} "
            f"(>{EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET} days)"
        )

    return quantize_fx(available_rates[index])


def load_state(path: str | Path) -> list[PositionState]:
    return load_position_states(path)


def load_opening_state_snapshot(
    path: str | Path,
    *,
    allowed_asset_classes: set[str] | None = None,
) -> tuple[list[PositionState], date]:
    snapshot_df = pl.read_csv(path)
    if snapshot_df.is_empty():
        raise ValueError(f"Opening state snapshot is empty: {path}")
    if "snapshot_date" not in snapshot_df.columns:
        raise ValueError(f"Opening state snapshot is missing required column 'snapshot_date': {path}")

    snapshot_dates = {date.fromisoformat(str(value)) for value in snapshot_df["snapshot_date"].to_list()}
    if len(snapshot_dates) != 1:
        raise ValueError(
            f"Opening state snapshot must contain exactly one snapshot_date value, found {len(snapshot_dates)} in {path}"
        )
    snapshot_date = next(iter(snapshot_dates))

    if allowed_asset_classes is not None and "asset_class" in snapshot_df.columns:
        snapshot_df = snapshot_df.filter(pl.col("asset_class").is_in(sorted(allowed_asset_classes)))
    if snapshot_df.is_empty():
        raise ValueError(f"Opening state snapshot contains no matching rows after asset-class filtering: {path}")
    return aggregate_state_rows(snapshot_df.to_dicts()), snapshot_date


def load_payout_state(path: str | Path) -> dict[str, PayoutStateRow]:
    payout_state_path = Path(path)
    if not payout_state_path.exists():
        return {}

    payout_df = pl.read_csv(payout_state_path)
    payout_state: dict[str, PayoutStateRow] = {}
    for row in payout_df.to_dicts():
        payout = PayoutStateRow(
            payout_key=str(row["payout_key"]),
            ticker=str(row["ticker"]),
            isin=str(row["isin"]),
            ex_date=date.fromisoformat(str(row["ex_date"])) if row.get("ex_date") else None,
            pay_date=date.fromisoformat(str(row["pay_date"])),
            quantity=round_qty(to_decimal(row["quantity"])),
            currency=str(row["currency"]),
            broker_gross_amount_ccy=round_money(to_decimal(row.get("broker_gross_amount_ccy") or 0)),
            broker_net_amount_ccy=round_money(to_decimal(row.get("broker_net_amount_ccy") or 0)),
            broker_tax_amount_ccy=round_money(to_decimal(row.get("broker_tax_amount_ccy") or 0)),
            source_tax_year=int(row["source_tax_year"]),
            evidence_state=str(row.get("evidence_state") or PAYOUT_EVIDENCE_CONFIRMED_CASH),
            status=str(row["status"]),
            resolved_tax_year=str(row.get("resolved_tax_year") or ""),
            resolved_by_report_year=str(row.get("resolved_by_report_year") or ""),
            resolved_by_report_file=str(row.get("resolved_by_report_file") or ""),
            resolution_mode=str(row.get("resolution_mode") or ""),
            action_id=str(row.get("action_id") or ""),
            source_statement_file=str(row.get("source_statement_file") or ""),
            notes=str(row.get("notes") or ""),
        )
        payout_state[payout.payout_key] = payout
    return payout_state


def payout_state_to_df(rows: list[PayoutStateRow]) -> pl.DataFrame:
    schema = {
        "payout_key": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "ex_date": pl.String,
        "pay_date": pl.String,
        "quantity": pl.Float64,
        "currency": pl.String,
        "broker_gross_amount_ccy": pl.Float64,
        "broker_net_amount_ccy": pl.Float64,
        "broker_tax_amount_ccy": pl.Float64,
        "source_tax_year": pl.Int64,
        "evidence_state": pl.String,
        "status": pl.String,
        "resolved_tax_year": pl.String,
        "resolved_by_report_year": pl.String,
        "resolved_by_report_file": pl.String,
        "resolution_mode": pl.String,
        "action_id": pl.String,
        "source_statement_file": pl.String,
        "notes": pl.String,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame([row.to_record() for row in rows])).sort(["ticker", "pay_date", "payout_key"])


def prepare_positions_for_new_year(positions: list[PositionState]) -> list[PositionState]:
    return list(clone_states(positions).values())


def _apply_position_event(
    positions: list[PositionState],
    event,
    *,
    event_rows: list[dict[str, object]] | None = None,
    sale_rows: list[dict[str, object]] | None = None,
) -> None:
    state_map = clone_states(positions)
    result = apply_event(state_map, event)
    positions[:] = sorted(state_map.values(), key=lambda item: (item.asset_class, item.ticker, item.isin))
    if event_rows is not None:
        event_rows.append(result.event_record)
    if sale_rows is not None and result.sale_record is not None:
        sale_rows.append(result.sale_record)


def apply_trade(
    positions: list[PositionState],
    trade: IbkrTrade,
    fx_table: dict[str, tuple[list[date], list[Decimal]]],
    sale_rows: list[dict[str, object]] | None = None,
    *,
    event_rows: list[dict[str, object]] | None = None,
    sequence_key: int = 0,
) -> None:
    trade_fx = get_fx_rate(fx_table, trade.currency, trade.trade_date)
    if trade.operation == "buy":
        event = build_buy_event(
            broker="ibkr",
            ticker=trade.ticker,
            isin=trade.isin,
            currency=trade.currency,
            asset_class="ETF",
            trade_date=trade.trade_date,
            quantity=trade.quantity,
            price_ccy=trade.price_ccy,
            fx_to_eur=trade_fx,
            source_id=trade.trade_id,
            source_file=trade.source_statement_file,
            sequence_key=sequence_key,
        )
    else:
        event = build_sell_event(
            broker="ibkr",
            ticker=trade.ticker,
            isin=trade.isin,
            currency=trade.currency,
            asset_class="ETF",
            trade_date=trade.trade_date,
            quantity=trade.quantity,
            price_ccy=trade.price_ccy,
            fx_to_eur=trade_fx,
            source_id=trade.trade_id,
            source_file=trade.source_statement_file,
            notes="Moving-average ETF sale result uses Austrian EUR basis plus cumulative OeKB acquisition-cost corrections.",
            sequence_key=sequence_key,
        )
    _apply_position_event(positions, event, event_rows=event_rows, sale_rows=sale_rows)


def _eligible_positions(positions: list[PositionState], isin: str, eligibility_date: date) -> list[PositionState]:
    del eligibility_date
    return [position for position in positions if position.isin == isin and position.quantity > 0]


def _sum_shares(positions: list[PositionState]) -> Decimal:
    return round_qty(sum(position.quantity for position in positions))


def _ensure_position_compatibility(positions: list[PositionState], report: OekbReport) -> None:
    # OeKB report currency can differ from the broker trade currency for the same ETF ISIN.
    # Austrian ETF tax rows and basis corrections are converted to EUR from the OeKB report currency,
    # while existing position state already stores basis in EUR. Therefore a currency mismatch between broker trade
    # currency and OeKB report currency is not, by itself, an incompatibility.
    del positions, report
    return None


def apply_basis_correction(
    positions: list[PositionState],
    report: OekbReport,
    tax_year: int,
    fx_table: dict[str, tuple[list[date], list[Decimal]]],
    *,
    quantity_override: Decimal | None = None,
    note_prefix: str = "",
    event_rows: list[dict[str, object]] | None = None,
    sequence_key_start: int = 0,
) -> dict[str, object]:
    if report.is_ausschuettungsmeldung and not (report.ex_tag or report.ausschuettungstag):
        raise ValueError(f"OeKB distribution report for {report.ticker} is missing both Ex-Tag and Ausschüttungstag")

    eligibility_date = report.eligibility_date
    effective_date = report.ausschuettungstag or report.meldedatum
    eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
    _ensure_position_compatibility(eligible_positions, report)

    shares_held = _sum_shares(eligible_positions) if quantity_override is None else round_qty(quantity_override)
    fx_to_eur = get_fx_rate(fx_table, report.currency, effective_date)
    basis_stepup_total_ccy = round_money(report.acquisition_cost_correction_per_share_ccy * shares_held)
    basis_stepup_total_eur = round_money(basis_stepup_total_ccy / fx_to_eur)

    allocated_total = Decimal("0")
    for index, position in enumerate(eligible_positions):
        if shares_held == 0 or not eligible_positions:
            stepup_eur = Decimal("0")
        elif index == len(eligible_positions) - 1:
            stepup_eur = round_money(basis_stepup_total_eur - allocated_total)
        else:
            total_remaining_quantity = _sum_shares(eligible_positions)
            if total_remaining_quantity == 0:
                stepup_eur = Decimal("0")
            else:
                stepup_eur = round_money(basis_stepup_total_eur * (position.quantity / total_remaining_quantity))
            allocated_total += stepup_eur

        if stepup_eur != 0:
            event = build_basis_adjustment_event(
                broker=position.broker,
                ticker=position.ticker,
                isin=position.isin,
                currency=position.currency,
                asset_class=position.asset_class or "ETF",
                eligibility_date=eligibility_date,
                effective_date=effective_date,
                basis_adjustment_eur=stepup_eur,
                quantity=position.quantity,
                source_id=f"{report.ticker}:{effective_date.isoformat()}:10289",
                source_file=report.source_file,
                notes=(
                    f"{note_prefix}{tax_year} OeKB basis correction on {effective_date.isoformat()} "
                    f"(eligible {eligibility_date.isoformat()}, delta {to_output_float(stepup_eur):.6f} EUR)"
                ),
                sequence_key=sequence_key_start + index,
            )
            _apply_position_event(positions, event, event_rows=event_rows)

    return {
        "tax_year": tax_year,
        "ticker": report.ticker,
        "isin": report.isin,
        "report_type": "Ausschüttungsmeldung" if report.is_ausschuettungsmeldung else "Jahresmeldung",
        "eligibility_date": eligibility_date.isoformat(),
        "effective_date": effective_date.isoformat(),
        "currency": report.currency,
        "acquisition_cost_correction_per_share_ccy": to_output_float(round_money(report.acquisition_cost_correction_per_share_ccy)),
        "shares_held_on_eligibility_date": to_output_float(shares_held),
        "basis_stepup_total_ccy": to_output_float(basis_stepup_total_ccy),
        "basis_stepup_total_eur": to_output_float(basis_stepup_total_eur),
        "fx_to_eur": to_output_float(round_money(fx_to_eur)),
        "source_file": report.source_file,
        "notes": (
            (f"{note_prefix.strip()} " if note_prefix else "")
            + ("" if shares_held > 0 else "No eligible shares were held on the eligibility date.")
        ).strip(),
    }


def _open_status_for_event(event: BrokerDividendEvent) -> str:
    if event.evidence_state == PAYOUT_EVIDENCE_CONFIRMED_CASH:
        return PAYOUT_STATUS_UNRESOLVED_OPEN
    if event.evidence_state == PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY:
        return PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY
    return PAYOUT_EVIDENCE_REALIZED_CASH_MISSING


def _payout_is_confirmed_cash(payout: PayoutStateRow) -> bool:
    return payout.evidence_state == PAYOUT_EVIDENCE_CONFIRMED_CASH


def _event_is_confirmed_cash(event: BrokerDividendEvent) -> bool:
    return event.evidence_state == PAYOUT_EVIDENCE_CONFIRMED_CASH


def _upsert_payout_state_row(
    payout_state: dict[str, PayoutStateRow],
    event: BrokerDividendEvent,
) -> None:
    existing = payout_state.get(event.event_id)
    status = _open_status_for_event(event)
    notes = event.matching_notes
    if existing is None:
        payout_state[event.event_id] = PayoutStateRow(
            payout_key=event.event_id,
            ticker=event.ticker,
            isin=event.isin,
            ex_date=event.ex_date,
            pay_date=event.pay_date,
            quantity=event.quantity,
            currency=event.currency,
            broker_gross_amount_ccy=round_money(event.gross_amount or 0.0),
            broker_net_amount_ccy=round_money(event.net_amount or 0.0),
            broker_tax_amount_ccy=round_money(event.tax or 0.0),
            source_tax_year=event.pay_date.year,
            evidence_state=event.evidence_state,
            status=status,
            resolved_tax_year="",
            resolution_mode="",
            action_id=event.action_id,
            source_statement_file=event.source_statement_file,
            notes=notes,
        )
        return

    existing.ticker = event.ticker
    existing.isin = event.isin
    existing.ex_date = event.ex_date
    existing.pay_date = event.pay_date
    existing.quantity = round_qty(event.quantity)
    existing.currency = event.currency
    existing.broker_gross_amount_ccy = round_money(event.gross_amount or 0.0)
    existing.broker_net_amount_ccy = round_money(event.net_amount or 0.0)
    existing.broker_tax_amount_ccy = round_money(event.tax or 0.0)
    existing.source_tax_year = event.pay_date.year
    existing.evidence_state = event.evidence_state
    existing.action_id = event.action_id
    existing.source_statement_file = event.source_statement_file
    if event.evidence_state != PAYOUT_EVIDENCE_CONFIRMED_CASH:
        existing.status = _open_status_for_event(event)
        existing.resolved_tax_year = ""
        existing.resolved_by_report_year = ""
        existing.resolved_by_report_file = ""
        existing.resolution_mode = ""
        existing.notes = notes
    elif existing.status in {
        PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY,
        PAYOUT_EVIDENCE_REALIZED_CASH_MISSING,
        PAYOUT_STATUS_UNRESOLVED_OPEN,
    }:
        existing.status = _open_status_for_event(event)
    if event.evidence_state == PAYOUT_EVIDENCE_CONFIRMED_CASH and notes and notes not in existing.notes:
        existing.add_note(notes)


def _mark_payout_resolved(
    payout: PayoutStateRow,
    *,
    status: str,
    tax_year: int,
    report: OekbReport,
    resolution_mode: str,
    notes: str,
) -> None:
    payout.status = status
    payout.resolved_tax_year = str(tax_year)
    payout.resolved_by_report_year = str(report.meldedatum.year)
    payout.resolved_by_report_file = report.source_file
    payout.resolution_mode = resolution_mode
    payout.add_note(notes)


def _mark_payout_resolved_without_report(
    payout: PayoutStateRow,
    *,
    status: str,
    tax_year: int,
    resolution_mode: str,
    notes: str,
) -> None:
    payout.status = status
    payout.resolved_tax_year = str(tax_year)
    payout.resolved_by_report_year = ""
    payout.resolved_by_report_file = ""
    payout.resolution_mode = resolution_mode
    payout.add_note(notes)


def _build_broker_event_by_id(broker_events: list[BrokerDividendEvent]) -> dict[str, BrokerDividendEvent]:
    return {event.event_id: event for event in broker_events}


def _match_distribution_report_to_payouts(
    payout_rows: list[PayoutStateRow],
    reports: list[OekbReport],
    *,
    tax_year: int,
) -> list[dict[str, object]]:
    resolution_rows: list[dict[str, object]] = []
    distribution_reports = [report for report in reports if report.is_ausschuettungsmeldung]
    for payout in payout_rows:
        if payout.pay_date.year != tax_year:
            continue
        if not _payout_is_confirmed_cash(payout):
            continue
        matches = [
            report
            for report in distribution_reports
            if report.isin == payout.isin
            and (report.ausschuettungstag or report.meldedatum) == payout.pay_date
            and (report.ex_tag is None or payout.ex_date is None or report.ex_tag == payout.ex_date)
        ]
        if len(matches) > 1:
            raise ValueError(
                f"Multiple OeKB distribution reports matched payout {payout.ticker} ({payout.isin}) "
                f"on {payout.pay_date.isoformat()}."
            )
        if len(matches) != 1:
            continue
        report = matches[0]
        _mark_payout_resolved(
            payout,
            status="resolved_same_year_distribution",
            tax_year=tax_year,
            report=report,
            resolution_mode="matched_same_year_distribution",
            notes="resolved by same-year Ausschüttungsmeldung; matched by actionID-aware broker payout event",
        )
        resolution_rows.append(
            {
                "payout_key": payout.payout_key,
                "ticker": payout.ticker,
                "isin": payout.isin,
                "pay_date": payout.pay_date.isoformat(),
                "report_year": report.meldedatum.year,
                "resolution_mode": payout.resolution_mode,
                "status": payout.status,
                "notes": payout.notes,
            }
        )
    return resolution_rows


def _resolve_annual_10595_reports(
    payout_rows: list[PayoutStateRow],
    annual_reports: list[OekbReport],
    *,
    target_tax_year: int,
    positions_for_quantity: list[PositionState],
    fx_table: dict[str, tuple[list[date], list[Decimal]]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    del positions_for_quantity
    income_rows: list[dict[str, object]] = []
    resolution_rows: list[dict[str, object]] = []

    for report in annual_reports:
        if report.non_reported_distribution_per_share_ccy == 0:
            continue
        period = report.annual_reconciliation_period
        if period is None:
            raise ValueError(
                f"OeKB annual report {report.source_file} has 10595 > 0 but no Meldezeitraum or Geschäftsjahres period metadata."
            )

        period_start, period_end = period
        in_period_rows = [
            payout
            for payout in payout_rows
            if payout.isin == report.isin
            and _payout_is_confirmed_cash(payout)
            and period_start <= payout.pay_date <= period_end
        ]
        target_rows = [payout for payout in in_period_rows if payout.pay_date.year == target_tax_year]
        unresolved_target_rows = [payout for payout in target_rows if payout.status == PAYOUT_STATUS_UNRESOLVED_OPEN]
        prior_year_rows = [payout for payout in in_period_rows if payout.pay_date.year < target_tax_year]

        if not unresolved_target_rows and prior_year_rows:
            for payout in prior_year_rows:
                payout.status = "ignored_prior_year_reference"
                payout.resolution_mode = "ignored_prior_year_reference"
                payout.resolved_by_report_year = str(report.meldedatum.year)
                payout.resolved_by_report_file = report.source_file
                payout.add_note("later annual report referenced prior-year payout; ignored for current-year tax and basis")
                resolution_rows.append(
                    {
                        "payout_key": payout.payout_key,
                        "ticker": payout.ticker,
                        "isin": payout.isin,
                        "pay_date": payout.pay_date.isoformat(),
                        "report_year": report.meldedatum.year,
                        "resolution_mode": payout.resolution_mode,
                        "status": payout.status,
                        "notes": payout.notes,
                    }
                )
            continue

        if not target_rows and not prior_year_rows:
            continue

        if not unresolved_target_rows:
            raise ValueError(
                f"Unable to reconcile OeKB code 10595 for {report.ticker} from {report.source_file}. "
                "No unresolved ETF broker payout events were found in the report period."
            )

        for payout in unresolved_target_rows:
            payout_status = (
                "resolved_later_year_annual_report"
                if report.meldedatum.year > target_tax_year
                else "resolved_same_year_annual_report"
            )
            resolution_note = (
                "resolved by next-year annual 10595 back to prior payout year"
                if report.meldedatum.year > target_tax_year
                else "resolved by annual 10595 in target tax year"
            )
            _mark_payout_resolved(
                payout,
                status=payout_status,
                tax_year=target_tax_year,
                report=report,
                resolution_mode="matched_annual_10595",
                notes=resolution_note,
            )
            resolution_rows.append(
                {
                    "payout_key": payout.payout_key,
                    "ticker": payout.ticker,
                    "isin": payout.isin,
                    "pay_date": payout.pay_date.isoformat(),
                    "report_year": report.meldedatum.year,
                    "resolution_mode": payout.resolution_mode,
                    "status": payout.status,
                    "notes": payout.notes,
                }
            )
            income_rows.append(
                {
                    "event_type": "oekb_non_reported_distribution_10595",
                    "tax_year": target_tax_year,
                    "event_date": payout.pay_date.isoformat(),
                    "eligibility_date": payout.ex_date.isoformat() if payout.ex_date else payout.pay_date.isoformat(),
                    "ticker": payout.ticker,
                    "isin": payout.isin,
                    "currency": payout.currency,
                    "quantity": to_output_float(round_qty(payout.quantity)),
                    "amount_per_share_ccy": to_output_float(round_money(
                        payout.broker_gross_amount_ccy / payout.quantity
                    )) if payout.quantity else 0.0,
                    "amount_total_ccy": to_output_float(round_money(payout.broker_gross_amount_ccy)),
                    "amount_total_eur": to_output_float(round_money(
                        payout.broker_gross_amount_ccy / get_fx_rate(fx_table, payout.currency, payout.pay_date)
                    )),
                    "creditable_foreign_tax_total_ccy": 0.0,
                    "creditable_foreign_tax_total_eur": 0.0,
                    "domestic_dividend_kest_total_ccy": 0.0,
                    "domestic_dividend_kest_total_eur": 0.0,
                    "broker_gross_amount_ccy": to_output_float(round_money(payout.broker_gross_amount_ccy)),
                    "broker_net_amount_ccy": to_output_float(round_money(payout.broker_net_amount_ccy)),
                    "broker_tax_amount_ccy": to_output_float(round_money(payout.broker_tax_amount_ccy)),
                    "matched_broker_event_id": payout.payout_key,
                    "source_file": report.source_file,
                    "notes": (
                        f"{resolution_note}; taxed using confirmed broker cash payout within report period "
                        f"{period_start.isoformat()}..{period_end.isoformat()}"
                    ),
                }
            )

    return income_rows, resolution_rows


def _resolve_broker_cash_payouts_outside_annual_periods(
    payout_rows: list[PayoutStateRow],
    annual_reports: list[OekbReport],
    *,
    tax_year: int,
) -> list[dict[str, object]]:
    resolution_rows: list[dict[str, object]] = []
    annual_reports_by_isin: dict[str, list[OekbReport]] = defaultdict(list)
    for report in annual_reports:
        annual_reports_by_isin[report.isin].append(report)

    for payout in payout_rows:
        if payout.pay_date.year != tax_year:
            continue
        if payout.status != PAYOUT_STATUS_UNRESOLVED_OPEN:
            continue

        same_isin_reports = annual_reports_by_isin.get(payout.isin, [])
        if not same_isin_reports:
            continue

        covered_by_annual_period = any(
            period_start <= payout.pay_date <= period_end
            for report in same_isin_reports
            for period_start, period_end in [report.annual_reconciliation_period]
            if report.annual_reconciliation_period is not None
        )
        if covered_by_annual_period:
            continue

        _mark_payout_resolved_without_report(
            payout,
            status="resolved_broker_cash_outside_oekb_period",
            tax_year=tax_year,
            resolution_mode="broker_cash_outside_oekb_period",
            notes=(
                "kept as broker cash payout because no OeKB annual report period covers the pay date; "
                "annual OeKB report remains authoritative for deemed distributed income, basis correction, "
                "and creditable foreign tax"
            ),
        )
        resolution_rows.append(
            {
                "payout_key": payout.payout_key,
                "ticker": payout.ticker,
                "isin": payout.isin,
                "pay_date": payout.pay_date.isoformat(),
                "report_year": None,
                "resolution_mode": payout.resolution_mode,
                "status": payout.status,
                "notes": payout.notes,
            }
        )

    return resolution_rows


def _build_broker_dividend_row(event: BrokerDividendEvent, fx_table: dict[str, tuple[list[date], list[Decimal]]]) -> dict[str, object]:
    fx_to_eur = get_fx_rate(fx_table, event.currency, event.pay_date)
    gross_amount_ccy = round_money(event.gross_amount or 0)
    net_amount_ccy = round_money(event.net_amount or 0)
    tax_ccy = round_money(event.tax or 0)
    notes = f"Collapsed broker accrual lifecycle has_po={event.has_po} has_re={event.has_re}; {event.matching_notes}".strip("; ")
    if tax_ccy != 0:
        notes = (
            f"{notes}; broker withholding retained as audit-only evidence; "
            "OeKB annual report determines ETF creditable foreign tax"
        )
    return {
        "event_type": "broker_dividend_event",
        "tax_year": event.pay_date.year,
        "event_date": event.pay_date.isoformat(),
        "eligibility_date": event.ex_date.isoformat() if event.ex_date else "",
        "ticker": event.ticker,
        "isin": event.isin,
        "currency": event.currency,
        "quantity": to_output_float(round_qty(event.quantity)),
        "amount_per_share_ccy": to_output_float(round_money(event.gross_rate or 0)),
        "amount_total_ccy": to_output_float(gross_amount_ccy),
        "amount_total_eur": to_output_float(round_money(gross_amount_ccy / fx_to_eur)),
        "creditable_foreign_tax_total_ccy": 0.0,
        "creditable_foreign_tax_total_eur": 0.0,
        "domestic_dividend_kest_total_ccy": 0.0,
        "domestic_dividend_kest_total_eur": 0.0,
        "broker_gross_amount_ccy": to_output_float(gross_amount_ccy),
        "broker_net_amount_ccy": to_output_float(net_amount_ccy),
        "broker_tax_amount_ccy": to_output_float(tax_ccy),
        "matched_broker_event_id": event.event_id,
        "source_file": event.source_statement_file,
        "notes": notes,
    }


def _filter_superseded_broker_income_rows(
    income_rows: list[dict[str, object]],
    payout_rows: list[PayoutStateRow],
) -> list[dict[str, object]]:
    keep_broker_event_ids = {
        payout.payout_key
        for payout in payout_rows
        if payout.status in {PAYOUT_STATUS_UNRESOLVED_OPEN, "resolved_broker_cash_outside_oekb_period"}
    }
    filtered_rows: list[dict[str, object]] = []
    for row in income_rows:
        if row.get("event_type") != "broker_dividend_event":
            filtered_rows.append(row)
            continue
        matched_broker_event_id = str(row.get("matched_broker_event_id") or "")
        if not matched_broker_event_id or matched_broker_event_id in keep_broker_event_ids:
            filtered_rows.append(row)
    return filtered_rows


def _select_explicit_distribution_match(report: OekbReport, broker_events: list[BrokerDividendEvent]) -> BrokerDividendEvent:
    payout_date = report.ausschuettungstag or report.meldedatum
    matches = [
        event
        for event in broker_events
        if event.isin == report.isin
        and _event_is_confirmed_cash(event)
        and event.pay_date == payout_date
        and (report.ex_tag is None or event.ex_date is None or event.ex_date == report.ex_tag)
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one broker dividend accrual match for {report.ticker} on {payout_date.isoformat()}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _has_confirmed_distribution_match(report: OekbReport, broker_events: list[BrokerDividendEvent]) -> bool:
    if not report.is_ausschuettungsmeldung:
        return True
    payout_date = report.ausschuettungstag or report.meldedatum
    return any(
        event.isin == report.isin
        and _event_is_confirmed_cash(event)
        and event.pay_date == payout_date
        and (report.ex_tag is None or event.ex_date is None or event.ex_date == report.ex_tag)
        for event in broker_events
    )
def build_income_rows_for_report(
    positions: list[PositionState],
    report: OekbReport,
    tax_year: int,
    fx_table: dict[str, tuple[list[date], list[Decimal]]],
    broker_events: list[BrokerDividendEvent],
    *,
    quantity_override: Decimal | None = None,
    note_prefix: str = "",
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    matched_distribution_event: BrokerDividendEvent | None = None

    def add_row(
        *,
        event_type: str,
        event_date: date,
        eligibility_date: date,
        quantity: Decimal,
        amount_per_share_ccy: Decimal,
        amount_total_ccy: Decimal,
        creditable_foreign_tax_per_share_ccy: Decimal = Decimal("0"),
        creditable_foreign_tax_total_ccy: Decimal = Decimal("0"),
        domestic_dividend_kest_total_ccy: Decimal = Decimal("0"),
        matched_broker_event_id: str = "",
        notes: str = "",
    ) -> None:
        fx_to_eur = get_fx_rate(fx_table, report.currency, event_date)
        rows.append(
            {
                "event_type": event_type,
                "tax_year": tax_year,
                "event_date": event_date.isoformat(),
                "eligibility_date": eligibility_date.isoformat(),
                "ticker": report.ticker,
                "isin": report.isin,
                "currency": report.currency,
                "quantity": to_output_float(round_qty(quantity)),
                "amount_per_share_ccy": to_output_float(round_money(amount_per_share_ccy)),
                "amount_total_ccy": to_output_float(round_money(amount_total_ccy)),
                "amount_total_eur": to_output_float(round_money(amount_total_ccy / fx_to_eur)),
                "creditable_foreign_tax_total_ccy": to_output_float(round_money(creditable_foreign_tax_total_ccy)),
                "creditable_foreign_tax_total_eur": to_output_float(round_money(creditable_foreign_tax_total_ccy / fx_to_eur)),
                "domestic_dividend_kest_total_ccy": to_output_float(round_money(domestic_dividend_kest_total_ccy)),
                "domestic_dividend_kest_total_eur": to_output_float(round_money(domestic_dividend_kest_total_ccy / fx_to_eur)),
                "broker_gross_amount_ccy": 0.0,
                "broker_net_amount_ccy": 0.0,
                "broker_tax_amount_ccy": 0.0,
                "matched_broker_event_id": matched_broker_event_id,
                "source_file": report.source_file,
                "notes": notes,
            }
        )

    def resolve_report_event_timing() -> tuple[date, date, str]:
        nonlocal matched_distribution_event
        if report.is_ausschuettungsmeldung:
            if matched_distribution_event is None:
                matched_distribution_event = _select_explicit_distribution_match(report, broker_events)
            event_date = matched_distribution_event.pay_date
            eligibility_date = report.ex_tag or matched_distribution_event.ex_date or matched_distribution_event.pay_date
            return event_date, eligibility_date, matched_distribution_event.event_id

        return report.meldedatum, report.meldedatum, ""

    if report.reported_distribution_per_share_ccy != 0:
        event_date, eligibility_date, matched_broker_event_id = resolve_report_event_timing()
        eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
        _ensure_position_compatibility(eligible_positions, report)
        quantity = _sum_shares(eligible_positions)
        add_row(
            event_type="oekb_reported_distribution_10286",
            event_date=event_date,
            eligibility_date=eligibility_date,
            quantity=quantity,
            amount_per_share_ccy=report.reported_distribution_per_share_ccy,
            amount_total_ccy=round_money(report.reported_distribution_per_share_ccy * quantity),
            matched_broker_event_id=matched_broker_event_id,
            notes="Explicit OeKB distribution matched to broker dividend accrual event.",
        )

    if report.age_per_share_ccy != 0:
        event_date, eligibility_date, matched_broker_event_id = resolve_report_event_timing()
        eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
        _ensure_position_compatibility(eligible_positions, report)
        quantity = _sum_shares(eligible_positions) if quantity_override is None else round_qty(quantity_override)
        add_row(
            event_type="oekb_deemed_distribution_10287",
            event_date=event_date,
            eligibility_date=eligibility_date,
            quantity=quantity,
            amount_per_share_ccy=report.age_per_share_ccy,
            amount_total_ccy=round_money(report.age_per_share_ccy * quantity),
            matched_broker_event_id=matched_broker_event_id,
            notes=(
                f"{note_prefix}"
                f"{'Distribution-report' if report.is_ausschuettungsmeldung else 'Annual'} "
                "deemed distribution from OeKB report."
            ).strip(),
        )

    if report.creditable_foreign_tax_per_share_ccy != 0:
        event_date, eligibility_date, matched_broker_event_id = resolve_report_event_timing()
        eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
        _ensure_position_compatibility(eligible_positions, report)
        quantity = _sum_shares(eligible_positions) if quantity_override is None else round_qty(quantity_override)
        credit_total_ccy = round_money(report.creditable_foreign_tax_per_share_ccy * quantity)
        add_row(
            event_type="oekb_creditable_foreign_tax_10288",
            event_date=event_date,
            eligibility_date=eligibility_date,
            quantity=quantity,
            amount_per_share_ccy=Decimal("0"),
            amount_total_ccy=Decimal("0"),
            creditable_foreign_tax_per_share_ccy=report.creditable_foreign_tax_per_share_ccy,
            creditable_foreign_tax_total_ccy=credit_total_ccy,
            matched_broker_event_id=matched_broker_event_id,
            notes=(
                f"{note_prefix}"
                f"{'Distribution-report' if report.is_ausschuettungsmeldung else 'Annual'} "
                "creditable foreign tax from OeKB report."
            ).strip(),
        )

    if report.domestic_dividends_loss_offset_per_share_ccy != 0:
        event_date, eligibility_date, matched_broker_event_id = resolve_report_event_timing()
        eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
        _ensure_position_compatibility(eligible_positions, report)
        quantity = _sum_shares(eligible_positions) if quantity_override is None else round_qty(quantity_override)
        add_row(
            event_type="oekb_domestic_dividends_10759",
            event_date=event_date,
            eligibility_date=eligibility_date,
            quantity=quantity,
            amount_per_share_ccy=report.domestic_dividends_loss_offset_per_share_ccy,
            amount_total_ccy=round_money(report.domestic_dividends_loss_offset_per_share_ccy * quantity),
            matched_broker_event_id=matched_broker_event_id,
            notes=(
                f"{note_prefix}"
                f"{'Distribution-report' if report.is_ausschuettungsmeldung else 'Annual'} "
                "domestic dividends eligible for loss offset from OeKB report."
            ).strip(),
        )

    if report.domestic_dividend_kest_per_share_ccy != 0:
        event_date, eligibility_date, matched_broker_event_id = resolve_report_event_timing()
        eligible_positions = _eligible_positions(positions, report.isin, eligibility_date)
        _ensure_position_compatibility(eligible_positions, report)
        quantity = _sum_shares(eligible_positions) if quantity_override is None else round_qty(quantity_override)
        domestic_kest_total_ccy = round_money(report.domestic_dividend_kest_per_share_ccy * quantity)
        add_row(
            event_type="oekb_domestic_dividend_kest_10760",
            event_date=event_date,
            eligibility_date=eligibility_date,
            quantity=quantity,
            amount_per_share_ccy=Decimal("0"),
            amount_total_ccy=Decimal("0"),
            domestic_dividend_kest_total_ccy=domestic_kest_total_ccy,
            matched_broker_event_id=matched_broker_event_id,
            notes=(
                f"{note_prefix}"
                f"{'Distribution-report' if report.is_ausschuettungsmeldung else 'Annual'} "
                "Austrian KESt on domestic dividends from OeKB report."
            ).strip(),
        )

    return rows


def positions_to_df(positions: list[PositionState]) -> pl.DataFrame:
    return position_states_to_df(positions)


def basis_adjustments_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "tax_year": pl.Int64,
        "ticker": pl.String,
        "isin": pl.String,
        "report_type": pl.String,
        "eligibility_date": pl.String,
        "effective_date": pl.String,
        "currency": pl.String,
        "acquisition_cost_correction_per_share_ccy": pl.Float64,
        "shares_held_on_eligibility_date": pl.Float64,
        "basis_stepup_total_ccy": pl.Float64,
        "basis_stepup_total_eur": pl.Float64,
        "fx_to_eur": pl.Float64,
        "source_file": pl.String,
        "notes": pl.String,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(rows)).sort(["ticker", "tax_year", "effective_date", "report_type"])


def income_events_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "event_type": pl.String,
        "tax_year": pl.Int64,
        "event_date": pl.String,
        "eligibility_date": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "currency": pl.String,
        "quantity": pl.Float64,
        "amount_per_share_ccy": pl.Float64,
        "amount_total_ccy": pl.Float64,
        "amount_total_eur": pl.Float64,
        "creditable_foreign_tax_total_ccy": pl.Float64,
        "creditable_foreign_tax_total_eur": pl.Float64,
        "domestic_dividend_kest_total_ccy": pl.Float64,
        "domestic_dividend_kest_total_eur": pl.Float64,
        "broker_gross_amount_ccy": pl.Float64,
        "broker_net_amount_ccy": pl.Float64,
        "broker_tax_amount_ccy": pl.Float64,
        "matched_broker_event_id": pl.String,
        "source_file": pl.String,
        "notes": pl.String,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(rows)).sort(["ticker", "tax_year", "event_date", "event_type"])


def payout_resolution_events_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "payout_key": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "pay_date": pl.String,
        "report_year": pl.Int64,
        "resolution_mode": pl.String,
        "status": pl.String,
        "notes": pl.String,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(rows)).sort(["ticker", "pay_date", "resolution_mode"])


def negative_deemed_distribution_review_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "report_key": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "report_date": pl.String,
        "decision": pl.String,
        "status": pl.String,
        "eligible_quantity_used": pl.Float64,
        "quantity_held_on_report_date": pl.Float64,
        "candidate_payout_count": pl.Int64,
        "candidate_payout_dates": pl.String,
        "candidate_payout_quantities": pl.String,
        "candidate_payout_gross_amounts_ccy": pl.String,
        "deemed_distributed_income_per_share_ccy": pl.Float64,
        "non_reported_distribution_per_share_ccy": pl.Float64,
        "creditable_foreign_tax_per_share_ccy": pl.Float64,
        "basis_correction_per_share_ccy": pl.Float64,
        "basis_age_component_per_share_ccy": pl.Float64,
        "basis_distribution_component_per_share_ccy": pl.Float64,
        "capital_repayment_per_share_ccy": pl.Float64,
        "withheld_tax_on_non_reported_distributions_per_share_ccy": pl.Float64,
        "source_file": pl.String,
        "notes": pl.String,
    }
    if not rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(rows)).sort(["ticker", "report_date"])


def payout_evidence_review_to_df(rows: list[PayoutStateRow], tax_year: int) -> pl.DataFrame:
    schema = {
        "payout_key": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "ex_date": pl.String,
        "pay_date": pl.String,
        "quantity": pl.Float64,
        "currency": pl.String,
        "evidence_state": pl.String,
        "status": pl.String,
        "broker_gross_amount_ccy": pl.Float64,
        "broker_net_amount_ccy": pl.Float64,
        "broker_tax_amount_ccy": pl.Float64,
        "action_id": pl.String,
        "source_statement_file": pl.String,
        "notes": pl.String,
    }
    review_rows = [
        {
            "payout_key": payout.payout_key,
            "ticker": payout.ticker,
            "isin": payout.isin,
            "ex_date": payout.ex_date.isoformat() if payout.ex_date else "",
            "pay_date": payout.pay_date.isoformat(),
            "quantity": to_output_float(round_qty(payout.quantity)),
            "currency": payout.currency,
            "evidence_state": payout.evidence_state,
            "status": payout.status,
            "broker_gross_amount_ccy": to_output_float(round_money(payout.broker_gross_amount_ccy)),
            "broker_net_amount_ccy": to_output_float(round_money(payout.broker_net_amount_ccy)),
            "broker_tax_amount_ccy": to_output_float(round_money(payout.broker_tax_amount_ccy)),
            "action_id": payout.action_id,
            "source_statement_file": payout.source_statement_file,
            "notes": payout.notes,
        }
        for payout in rows
        if payout.pay_date.year == tax_year
        and payout.status in {PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY, PAYOUT_EVIDENCE_REALIZED_CASH_MISSING}
    ]
    if not review_rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(review_rows)).sort(["ticker", "pay_date", "payout_key"])


def position_event_log_to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    return position_events_to_df(rows)


def sales_to_df(sale_rows: list[dict[str, object]]) -> pl.DataFrame:
    schema = {
        "sale_date": pl.String,
        "ticker": pl.String,
        "isin": pl.String,
        "quantity_sold": pl.Float64,
        "sale_price_ccy": pl.Float64,
        "sale_fx": pl.Float64,
        "taxable_proceeds_eur": pl.Float64,
        "realized_base_cost_eur": pl.Float64,
        "realized_oekb_adjustment_eur": pl.Float64,
        "taxable_total_basis_eur": pl.Float64,
        "taxable_gain_loss_eur": pl.Float64,
        "sale_trade_id": pl.String,
        "notes": pl.String,
    }
    if not sale_rows:
        return pl.DataFrame(schema=schema)
    return cast_decimal_columns_to_float(pl.DataFrame(sale_rows)).sort(["ticker", "sale_date", "sale_trade_id"])


def write_csv(df: pl.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)


def write_summary(
    summary_path: Path,
    tax_year: int,
    next_period_state_path: Path,
    next_period_payout_state_path: Path,
    next_period_negative_override_path: Path,
    state_df: pl.DataFrame,
    income_events_df: pl.DataFrame,
    basis_adjustments_df: pl.DataFrame,
    sales_df: pl.DataFrame,
    payout_state_df: pl.DataFrame,
    payout_evidence_review_df: pl.DataFrame,
    negative_review_df: pl.DataFrame,
    *,
    carryforward_only: bool = False,
    authoritative_start_date: date | None = None,
) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    income_total_eur = income_events_df["amount_total_eur"].sum() if income_events_df.height else 0.0
    foreign_tax_total_eur = (
        income_events_df["creditable_foreign_tax_total_eur"].sum() if income_events_df.height else 0.0
    )
    domestic_dividend_kest_total_eur = (
        income_events_df["domestic_dividend_kest_total_eur"].sum() if income_events_df.height else 0.0
    )
    basis_total_eur = basis_adjustments_df["basis_stepup_total_eur"].sum() if basis_adjustments_df.height else 0.0
    sales_total_eur = sales_df["taxable_gain_loss_eur"].sum() if sales_df.height else 0.0
    unresolved_count = (
        payout_state_df.filter(
            (pl.col("pay_date").str.slice(0, 4) == str(tax_year)) & (pl.col("status") == PAYOUT_STATUS_UNRESOLVED_OPEN)
        ).height
        if payout_state_df.height
        else 0
    )
    pending_pre_payout_count = (
        payout_evidence_review_df.filter(pl.col("status") == PAYOUT_EVIDENCE_PRE_PAYOUT_ONLY).height
        if payout_evidence_review_df.height
        else 0
    )
    pending_realized_cash_missing_count = (
        payout_evidence_review_df.filter(pl.col("status") == PAYOUT_EVIDENCE_REALIZED_CASH_MISSING).height
        if payout_evidence_review_df.height
        else 0
    )
    unresolved_negative_count = (
        negative_review_df.filter(pl.col("status") == "unresolved_block").height if negative_review_df.height else 0
    )
    broker_cash_filing_event_ids: list[str] = []
    if payout_state_df.height:
        broker_cash_filing_event_ids = (
            payout_state_df.filter(pl.col("resolution_mode") == "broker_cash_outside_oekb_period")
            .select(pl.col("payout_key").cast(pl.String))
            .to_series()
            .to_list()
        )

    filing_distribution_event_types = {
        "oekb_reported_distribution_10286",
        "oekb_non_reported_distribution_10595",
    }
    filing_distribution_income_df = income_events_df.filter(
        pl.col("event_type").is_in(filing_distribution_event_types)
        | (
            (pl.col("event_type") == "broker_dividend_event")
            & pl.col("matched_broker_event_id").cast(pl.String).is_in(broker_cash_filing_event_ids)
        )
    )
    filing_distribution_total_eur = (
        filing_distribution_income_df["amount_total_eur"].sum() if filing_distribution_income_df.height else 0.0
    )
    filing_deemed_distribution_total_eur = (
        income_events_df.filter(pl.col("event_type") == "oekb_deemed_distribution_10287")["amount_total_eur"].sum()
        if income_events_df.height
        else 0.0
    )
    filing_creditable_foreign_tax_total_eur = (
        income_events_df.filter(pl.col("event_type") == "oekb_creditable_foreign_tax_10288")[
            "creditable_foreign_tax_total_eur"
        ].sum()
        if income_events_df.height
        else 0.0
    )
    filing_domestic_dividends_loss_offset_total_eur = (
        income_events_df.filter(pl.col("event_type") == "oekb_domestic_dividends_10759")["amount_total_eur"].sum()
        if income_events_df.height
        else 0.0
    )
    filing_domestic_dividend_kest_total_eur = (
        income_events_df.filter(pl.col("event_type") == "oekb_domestic_dividend_kest_10760")[
            "domestic_dividend_kest_total_eur"
        ].sum()
        if income_events_df.height
        else 0.0
    )

    income_lines = [
        (
            f"- `{row['event_type']}` `{row['ticker']}` @ `{row['event_date']}`: "
            f"amount `{row['amount_total_eur']:.6f} EUR`, "
            f"creditable foreign tax `{row['creditable_foreign_tax_total_eur']:.6f} EUR`, "
            f"domestic dividend KESt `{row['domestic_dividend_kest_total_eur']:.6f} EUR`"
        )
        for row in income_events_df.to_dicts()
        if row["event_type"] != "broker_dividend_event"
    ] or ["- no ETF tax income events were emitted"]

    if carryforward_only:
        scope_label = authoritative_start_date.isoformat() if authoritative_start_date is not None else "not set"
        filing_heading = f"## {tax_year} Carryforward Scope"
        filing_lines = [
            f"- authoritative processing starts at `{scope_label}`",
            "- this run is carryforward-only and is not a filing-preparation output for the target year",
            "- OeKB `10289` basis corrections are applied to seed the next Austrian ETF state",
            "- OeKB income classifications are intentionally not emitted as filing rows in carryforward-only mode",
        ]
        income_heading = f"## {tax_year} Diagnostic Broker Cash Evidence"
    else:
        filing_heading = f"## {tax_year} Filing Inputs"
        filing_lines = [
            (
                f"- `ETF distributions 27.5%`: `{filing_distribution_total_eur:.6f} EUR`"
            ),
            (
                f"- `Ausschüttungsgleiche Erträge 27.5%`: `{filing_deemed_distribution_total_eur:.6f} EUR`"
            ),
            (
                f"- `Domestic dividends in loss offset (KZ 189)`: "
                f"`{filing_domestic_dividends_loss_offset_total_eur:.6f} EUR`"
            ),
            (
                f"- `Austrian KESt on domestic dividends (KZ 899)`: "
                f"`{filing_domestic_dividend_kest_total_eur:.6f} EUR`"
            ),
            (
                f"- `Creditable foreign tax`: `{filing_creditable_foreign_tax_total_eur:.6f} EUR`"
            ),
            (
                "- `ETF distributions 27.5%` includes `10286`, `10595`, and only those broker cash payouts that "
                "remain the tax event because no OeKB report period covered the pay date"
            ),
            (
                "- `Ausschüttungsgleiche Erträge 27.5%` is the subtotal of OeKB `10287` rows"
            ),
            (
                "- `Domestic dividends in loss offset (KZ 189)` is the subtotal of OeKB `10759` rows"
            ),
            (
                "- `Austrian KESt on domestic dividends (KZ 899)` is the subtotal of OeKB `10760` rows"
            ),
            (
                "- accrual-only or realized-without-cash payout rows are deferred until broker cash is actually confirmed"
            ),
            (
                "- excludes matched broker payout rows when OeKB distribution classification replaced them"
            ),
            (
                "- basis corrections from `10289` are not entered separately in E1kv; they only adjust future sale basis"
            ),
            (
                "- `10759` and `10760` are separate filing fields and do not modify `10286`, `10287`, `10595`, or `10288`"
            ),
        ]
        income_heading = f"## {tax_year} ETF Income Events"

    basis_lines = [
        (
            f"- `{row['ticker']}` `{row['report_type']}` @ `{row['effective_date']}`: "
            f"shares `{row['shares_held_on_eligibility_date']}`, "
            f"basis delta `{row['basis_stepup_total_eur']:.6f} EUR`"
        )
        for row in basis_adjustments_df.to_dicts()
    ] or ["- no ETF basis corrections were applied"]

    open_lot_lines = []
    if state_df.height:
        open_lots_df = state_df.filter(pl.col("quantity") > 0)
        for row in (
            open_lots_df.group_by("ticker")
            .agg(
                pl.len().alias("position_count"),
                pl.sum("quantity").alias("quantity"),
                pl.sum("base_cost_total_eur").alias("base_cost_total_eur"),
                pl.sum("basis_adjustment_total_eur").alias("basis_adjustment_total_eur"),
                pl.sum("total_basis_eur").alias("total_basis_eur"),
            )
            .sort("ticker")
            .to_dicts()
        ):
            open_lot_lines.append(
                f"- `{row['ticker']}`: open positions `{row['position_count']}`, open quantity `{row['quantity']}`, "
                f"base cost `{row['base_cost_total_eur']:.6f} EUR`, "
                f"OeKB basis adjustment `{row['basis_adjustment_total_eur']:.6f} EUR`, "
                f"total basis `{row['total_basis_eur']:.6f} EUR`"
            )
    if not open_lot_lines:
        open_lot_lines = ["- no open reporting-fund positions remain"]

    next_period_lines = [
        (
            f"- opening ETF tax state for `{tax_year + 1}`: "
            f"`{next_period_state_path.as_posix()}`"
        ),
        (
            f"- payout resolution carryforward state for `{tax_year + 1}`: "
            f"`{next_period_payout_state_path.as_posix()}`"
        ),
    ]
    next_period_lines.append(
        f"- combine those carryforward files with the next year's raw IBKR XML inputs, trade history, "
        f"and OeKB reports when running the `{tax_year + 1}` workflow"
    )

    negative_override_lines = [
        f"- manual override file path: `{next_period_negative_override_path.as_posix()}`"
    ]
    if unresolved_negative_count == 0:
        negative_override_lines.append(
            "- current run has no unresolved negative deemed-distribution review items, so no manual override is needed"
        )
    else:
        negative_override_lines.append(
            "- use this only if the workflow marks a negative `10287` case as unresolved and requests a manual decision"
        )

    summary_path.write_text(
        "\n".join(
            [
                "# Reporting Funds Summary",
                "",
                filing_heading,
                *filing_lines,
                "",
                income_heading,
                *income_lines,
                f"- `diagnostic_total_income_eur`: `{income_total_eur:.6f} EUR`",
                f"- `diagnostic_total_creditable_foreign_tax_eur`: `{foreign_tax_total_eur:.6f} EUR`",
                f"- `diagnostic_total_domestic_dividend_kest_eur`: `{domestic_dividend_kest_total_eur:.6f} EUR`",
                "- diagnostic totals are workflow totals, not direct filing fields",
                "",
                "## Basis Adjustments",
                *basis_lines,
                f"- `total_basis_adjustment_eur`: `{basis_total_eur:.6f} EUR`",
                "",
                "## Position State",
                *open_lot_lines,
                "",
                "## Sales",
                f"- `taxable_gain_loss_total_eur`: `{sales_total_eur:.6f} EUR`",
                "",
                "## Next Reporting Period Inputs",
                *next_period_lines,
                "",
                "## Manual Override Fallback",
                *negative_override_lines,
                "",
                "## Notes",
                "- OeKB remains the ETF tax source of truth.",
                "- Broker dividend accrual rows are cross-checked against ETF cash transactions using actionID first.",
                f"- unresolved payout rows blocking this year: `{unresolved_count}`",
                f"- pending pre-payout accrual-only rows deferred out of current-year taxation: `{pending_pre_payout_count}`",
                f"- realized accrual rows without broker cash deferred pending later cash confirmation: `{pending_realized_cash_missing_count}`",
                f"- unresolved negative deemed-distribution review rows blocking this year: `{unresolved_negative_count}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _determine_required_isins(
    tax_year: int,
    previous_positions: list[PositionState],
    all_trades: list[IbkrTrade],
    has_opening_state: bool,
) -> set[str]:
    current_year_trades = {trade.isin for trade in all_trades if trade.trade_date.year == tax_year}
    if has_opening_state:
        previous_open_isins = {position.isin for position in previous_positions if position.quantity > 0}
        return current_year_trades | previous_open_isins

    quantity_by_isin: dict[str, Decimal] = defaultdict(Decimal)
    for trade in all_trades:
        if trade.trade_date.year > tax_year:
            continue
        sign = 1 if trade.operation == "buy" else -1
        quantity_by_isin[trade.isin] += sign * trade.quantity

    year_end_open_isins = {isin for isin, quantity in quantity_by_isin.items() if quantity > 0}
    return current_year_trades | year_end_open_isins


def _resolve_fx_bounds(
    tax_year: int,
    opening_trades: list[IbkrTrade],
    current_year_trades: list[IbkrTrade],
    reports: list[OekbReport],
    broker_events: list[BrokerDividendEvent],
    has_previous_state: bool,
) -> tuple[date, date]:
    relevant_dates: list[date] = []
    relevant_dates.extend(trade.trade_date for trade in current_year_trades)
    relevant_dates.extend(report.meldedatum for report in reports)
    relevant_dates.extend(report.eligibility_date for report in reports)
    relevant_dates.extend((report.payout_date or report.meldedatum) for report in reports)
    relevant_dates.extend(event.pay_date for event in broker_events)
    relevant_dates.extend(event.ex_date for event in broker_events if event.ex_date is not None)
    if not has_previous_state:
        relevant_dates.extend(trade.trade_date for trade in opening_trades)

    if not relevant_dates:
        return date(tax_year, 1, 1), date(tax_year, 12, 31)

    return min(relevant_dates), max(relevant_dates)


def _resolve_required_currencies(
    opening_trades: list[IbkrTrade],
    current_year_trades: list[IbkrTrade],
    reports: list[OekbReport],
    broker_events: list[BrokerDividendEvent],
) -> tuple[str, ...]:
    currencies = {trade.currency for trade in opening_trades}
    currencies.update(trade.currency for trade in current_year_trades)
    currencies.update(report.currency for report in reports)
    currencies.update(event.currency for event in broker_events)
    currencies.discard("EUR")
    return tuple(sorted(currencies or {"USD"}))


def _report_event_date(report: OekbReport) -> date:
    return report.payout_date or report.meldedatum


def _report_in_authoritative_scope(report: OekbReport, authoritative_start_date: date | None) -> bool:
    if authoritative_start_date is None:
        return True
    if report.eligibility_date < authoritative_start_date:
        return False
    return _report_event_date(report) >= authoritative_start_date


def _negative_report_key(report: OekbReport) -> str:
    return f"{report.isin}:{report.meldedatum.isoformat()}"


def _load_negative_deemed_distribution_overrides(path: str | Path) -> dict[str, dict[str, str]]:
    override_path = Path(path)
    if not override_path.exists():
        return {}

    overrides_df = pl.read_csv(override_path)
    overrides: dict[str, dict[str, str]] = {}
    for row in overrides_df.to_dicts():
        report_key = str(row.get("report_key") or "").strip()
        if not report_key:
            continue
        overrides[report_key] = {
            "decision": str(row.get("decision") or "").strip(),
            "eligible_quantity": str(row.get("eligible_quantity") or "").strip(),
            "notes": str(row.get("notes") or "").strip(),
        }
    return overrides


def _candidate_negative_report_payouts(report: OekbReport, broker_events: list[BrokerDividendEvent]) -> list[BrokerDividendEvent]:
    same_isin_events = [
        event
        for event in broker_events
        if event.isin == report.isin and event.pay_date < report.meldedatum and event.cash_amount is not None
    ]
    period = report.annual_reconciliation_period
    if period is None:
        return same_isin_events
    period_start, period_end = period
    return [event for event in same_isin_events if period_start <= event.pay_date <= period_end]


def _negative_report_target_distribution_per_share(report: OekbReport) -> Decimal | None:
    for value in (
        report.non_reported_distribution_per_share_ccy,
        report.basis_distribution_component_per_share_ccy,
        report.total_distributions_per_share_ccy,
    ):
        if value is not None and value > 0:
            return round_money(value)
    return None


def _broker_event_distribution_per_share(event: BrokerDividendEvent) -> Decimal | None:
    if event.gross_rate is not None and event.gross_rate > 0:
        return round_money(event.gross_rate)
    if event.gross_amount is not None and event.quantity > 0:
        return round_money(event.gross_amount / event.quantity)
    return None


def _reconcile_negative_report_payout_subset(
    report: OekbReport,
    candidate_payouts: list[BrokerDividendEvent],
    *,
    tolerance: Decimal = Decimal("0.0002"),
) -> tuple[list[BrokerDividendEvent], Decimal | None]:
    target_per_share = _negative_report_target_distribution_per_share(report)
    if target_per_share is None or not candidate_payouts:
        return [], target_per_share

    payout_rates: list[tuple[BrokerDividendEvent, Decimal]] = []
    for event in candidate_payouts:
        rate = _broker_event_distribution_per_share(event)
        if rate is None:
            return [], target_per_share
        payout_rates.append((event, rate))

    matching_subsets: list[list[BrokerDividendEvent]] = []
    for subset_size in range(1, len(payout_rates) + 1):
        for subset in combinations(payout_rates, subset_size):
            subset_total = round_money(sum(rate for _, rate in subset))
            if abs(subset_total - target_per_share) <= tolerance:
                matching_subsets.append([event for event, _ in subset])

    if len(matching_subsets) != 1:
        return [], target_per_share
    return matching_subsets[0], target_per_share


def _parse_override_quantity(raw_value: str, report_key: str) -> Decimal | None:
    if not raw_value:
        return None
    try:
        return round_qty(Decimal(raw_value))
    except ValueError as exc:
        raise ValueError(f"Invalid eligible_quantity override for negative deemed-distribution report {report_key}.") from exc


def _resolve_negative_deemed_distribution_review(
    report: OekbReport,
    positions: list[PositionState],
    broker_events: list[BrokerDividendEvent],
    overrides: dict[str, dict[str, str]],
) -> dict[str, object]:
    report_key = _negative_report_key(report)
    eligible_positions = _eligible_positions(positions, report.isin, report.meldedatum)
    _ensure_position_compatibility(eligible_positions, report)
    quantity_held_on_report_date = _sum_shares(eligible_positions)
    candidate_payouts = _candidate_negative_report_payouts(report, broker_events)
    matched_payouts, target_distribution_per_share = _reconcile_negative_report_payout_subset(report, candidate_payouts)

    decision = NEGATIVE_DEEMED_DISTRIBUTION_BLOCK
    status = NEGATIVE_DEEMED_DISTRIBUTION_BLOCK
    eligible_quantity_used = Decimal("0")
    notes = ""

    auto_apply_payouts = matched_payouts
    if not auto_apply_payouts and target_distribution_per_share is None and len(candidate_payouts) == 1:
        auto_apply_payouts = candidate_payouts

    if auto_apply_payouts:
        candidate_quantity = round_qty(min(event.quantity for event in auto_apply_payouts))
        eligible_quantity_used = min(quantity_held_on_report_date, candidate_quantity)
        if eligible_quantity_used > 0:
            decision = (
                NEGATIVE_DEEMED_DISTRIBUTION_APPLY_FULL
                if abs(eligible_quantity_used - quantity_held_on_report_date) <= Decimal("0.00000001")
                else NEGATIVE_DEEMED_DISTRIBUTION_APPLY_PARTIAL
            )
            status = NEGATIVE_DEEMED_DISTRIBUTION_APPLIED_AUTO
            if matched_payouts:
                notes = (
                    "Applied automatically using the broker payout subset whose per-share distribution amounts reconcile "
                    "to the OeKB annual non-reported distribution total; eligible quantity is capped by the minimum "
                    "matched payout quantity and the quantity held on the report date."
                )
            else:
                notes = (
                    "Applied automatically using the single linked broker payout in the annual report period; "
                    "eligible quantity is capped by quantity held on the report date."
                )

    override = overrides.get(report_key)
    if override is not None:
        decision = override.get("decision") or NEGATIVE_DEEMED_DISTRIBUTION_BLOCK
        notes = override.get("notes") or ""
        override_quantity = _parse_override_quantity(override.get("eligible_quantity", ""), report_key)
        if decision == NEGATIVE_DEEMED_DISTRIBUTION_APPLY_FULL:
            eligible_quantity_used = quantity_held_on_report_date if override_quantity is None else override_quantity
            status = "applied_full"
        elif decision == NEGATIVE_DEEMED_DISTRIBUTION_APPLY_PARTIAL:
            if override_quantity is None:
                raise ValueError(
                    f"Negative deemed-distribution override for {report_key} uses apply_partial but no eligible_quantity was provided."
                )
            eligible_quantity_used = override_quantity
            status = "applied_partial"
        elif decision == NEGATIVE_DEEMED_DISTRIBUTION_IGNORE:
            status = NEGATIVE_DEEMED_DISTRIBUTION_IGNORE
        else:
            decision = NEGATIVE_DEEMED_DISTRIBUTION_BLOCK
            status = NEGATIVE_DEEMED_DISTRIBUTION_BLOCK

        if eligible_quantity_used < 0 or eligible_quantity_used - quantity_held_on_report_date > Decimal("0.00000001"):
            raise ValueError(
                f"Negative deemed-distribution override for {report_key} uses eligible_quantity={eligible_quantity_used}, "
                f"which exceeds the quantity held on the report date ({quantity_held_on_report_date})."
            )

    return {
        "report_key": report_key,
        "ticker": report.ticker,
        "isin": report.isin,
        "report_date": report.meldedatum.isoformat(),
        "decision": decision,
        "status": status,
        "eligible_quantity_used": to_output_float(round_qty(eligible_quantity_used)),
        "quantity_held_on_report_date": to_output_float(round_qty(quantity_held_on_report_date)),
        "candidate_payout_count": len(candidate_payouts),
        "candidate_payout_dates": "|".join(event.pay_date.isoformat() for event in candidate_payouts),
        "candidate_payout_quantities": "|".join(str(to_output_float(round_qty(event.quantity))) for event in candidate_payouts),
        "candidate_payout_gross_rates_ccy": "|".join(
            str(round_money(rate))
            for rate in (_broker_event_distribution_per_share(event) for event in candidate_payouts)
            if rate is not None
        ),
        "candidate_payout_gross_amounts_ccy": "|".join(str(to_output_float(round_money(event.gross_amount or 0))) for event in candidate_payouts),
        "matched_payout_count": len(auto_apply_payouts),
        "matched_payout_dates": "|".join(event.pay_date.isoformat() for event in auto_apply_payouts),
        "matched_payout_quantities": "|".join(str(to_output_float(round_qty(event.quantity))) for event in auto_apply_payouts),
        "matched_payout_gross_rates_ccy": "|".join(
            str(round_money(rate))
            for rate in (_broker_event_distribution_per_share(event) for event in auto_apply_payouts)
            if rate is not None
        ),
        "target_distribution_per_share_ccy": to_output_float(round_money(target_distribution_per_share or 0)),
        "deemed_distributed_income_per_share_ccy": to_output_float(round_money(report.age_per_share_ccy)),
        "non_reported_distribution_per_share_ccy": to_output_float(round_money(report.non_reported_distribution_per_share_ccy)),
        "creditable_foreign_tax_per_share_ccy": to_output_float(round_money(report.creditable_foreign_tax_per_share_ccy)),
        "basis_correction_per_share_ccy": to_output_float(round_money(report.acquisition_cost_correction_per_share_ccy)),
        "basis_age_component_per_share_ccy": to_output_float(round_money(report.basis_age_component_per_share_ccy or 0)),
        "basis_distribution_component_per_share_ccy": to_output_float(round_money(report.basis_distribution_component_per_share_ccy or 0)),
        "capital_repayment_per_share_ccy": to_output_float(round_money(report.capital_repayment_per_share_ccy or 0)),
        "withheld_tax_on_non_reported_distributions_per_share_ccy": to_output_float(round_money(
            report.withheld_tax_on_non_reported_distributions_per_share_ccy or 0
        )),
        "source_file": report.source_file,
        "notes": notes,
    }


def _build_ticker_by_isin(
    previous_positions: list[PositionState],
    all_trades: list[IbkrTrade],
    broker_events: list[BrokerDividendEvent],
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for position in previous_positions:
        mapping.setdefault(position.isin, position.ticker)
    for trade in all_trades:
        mapping.setdefault(trade.isin, trade.ticker)
    for event in broker_events:
        mapping.setdefault(event.isin, event.ticker)
    return mapping


def _merge_lookup_broker_events(
    primary_events: list[BrokerDividendEvent],
    lookup_events: list[BrokerDividendEvent],
) -> list[BrokerDividendEvent]:
    merged: dict[str, BrokerDividendEvent] = {event.event_id: event for event in primary_events}
    for event in lookup_events:
        merged.setdefault(event.event_id, event)
    return sorted(merged.values(), key=lambda event: (event.pay_date, event.ticker, event.event_id))


def run_workflow(
    *,
    person: str,
    tax_year: int,
    ibkr_tax_xml_path: str | Path,
    historical_ibkr_tax_xml_path: str | Path | None = None,
    ibkr_trade_history_path: str | Path,
    raw_exchange_rates_path: str | Path = "data/input/currencies/raw_exchange_rates.csv",
    oekb_root_dir: str | Path | None = None,
    state_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    resolution_cutoff_date: str | date | None = None,
    strict_unresolved_payouts: bool = True,
    negative_deemed_income_overrides_path: str | Path | None = None,
    opening_state_path: str | Path | None = None,
    authoritative_start_date: date | None = None,
    carryforward_only: bool = False,
) -> dict[str, Path]:
    reporting_funds_root = Path(f"data/output/{person}/reporting_funds")
    output_dir_path = Path(output_dir or reporting_funds_root / str(tax_year))
    state_dir_path = Path(state_dir or reporting_funds_root)
    oekb_root_dir_path = Path(oekb_root_dir or "data/input/oekb")
    oekb_dir_path = oekb_root_dir_path / str(tax_year)
    previous_state_path = state_dir_path / f"fund_tax_state_{tax_year - 1}_final.csv"
    current_state_path = state_dir_path / f"fund_tax_state_{tax_year}_final.csv"
    payout_state_path = state_dir_path / "fund_tax_payout_state.csv"
    negative_review_override_path = Path(
        negative_deemed_income_overrides_path or state_dir_path / "fund_tax_negative_deemed_distribution_overrides.csv"
    )
    has_previous_state = previous_state_path.exists()
    opening_state_snapshot_path = Path(opening_state_path) if opening_state_path else None
    has_opening_state_snapshot = opening_state_snapshot_path is not None
    if resolution_cutoff_date is None:
        resolution_cutoff = date(tax_year + 1, 12, 31)
    elif isinstance(resolution_cutoff_date, date):
        resolution_cutoff = resolution_cutoff_date
    else:
        resolution_cutoff = datetime.strptime(str(resolution_cutoff_date), "%Y-%m-%d").date()

    tax_xml_path = str(ibkr_tax_xml_path)
    historical_tax_xml_path = str(historical_ibkr_tax_xml_path) if historical_ibkr_tax_xml_path else None
    trade_history_path = str(ibkr_trade_history_path)

    if has_previous_state:
        previous_positions = load_state(previous_state_path)
        opening_snapshot_date = None
    elif has_opening_state_snapshot:
        previous_positions, opening_snapshot_date = load_opening_state_snapshot(
            opening_state_snapshot_path,
            allowed_asset_classes={"ETF"},
        )
    else:
        previous_positions = []
        opening_snapshot_date = None
    previous_payout_state = load_payout_state(payout_state_path)
    negative_deemed_overrides = _load_negative_deemed_distribution_overrides(negative_review_override_path)
    accrual_rows = load_ibkr_etf_dividend_accrual_rows(tax_xml_path)
    cash_rows = load_ibkr_etf_cash_dividend_rows(tax_xml_path)
    all_broker_events = build_broker_dividend_events(accrual_rows, cash_rows)
    historical_lookup_broker_events: list[BrokerDividendEvent] = []
    if historical_tax_xml_path:
        historical_lookup_broker_events = build_broker_dividend_events(
            load_ibkr_etf_dividend_accrual_rows(historical_tax_xml_path),
            load_ibkr_etf_cash_dividend_rows(historical_tax_xml_path),
        )
    broker_events_for_lookup = _merge_lookup_broker_events(all_broker_events, historical_lookup_broker_events)
    all_trades = load_ibkr_etf_trades(
        trade_history_path,
        require_raw_trades=not (has_previous_state or has_opening_state_snapshot),
    )
    ticker_by_isin = _build_ticker_by_isin(previous_positions, all_trades, broker_events_for_lookup)
    required_isins = _determine_required_isins(
        tax_year=tax_year,
        previous_positions=previous_positions,
        all_trades=all_trades,
        has_opening_state=has_previous_state or has_opening_state_snapshot,
    )
    same_year_reports = (
        load_required_oekb_reports(oekb_dir_path, tax_year, required_isins, ticker_by_isin=ticker_by_isin)
        if required_isins
        else []
    )
    lookahead_reports = []
    lookahead_dir = oekb_root_dir_path / str(tax_year + 1)
    if required_isins:
        lookahead_reports = [
            report
            for report in load_matching_oekb_reports(
                lookahead_dir,
                required_isins,
                tax_year=tax_year + 1,
                ticker_by_isin=ticker_by_isin,
            )
            if report.is_jahresmeldung and report.meldedatum <= resolution_cutoff
        ]

    year_start = date(tax_year, 1, 1)
    year_end = date(tax_year, 12, 31)
    processing_start_date = year_start
    if authoritative_start_date is not None and authoritative_start_date.year == tax_year:
        processing_start_date = max(processing_start_date, authoritative_start_date)
    if opening_snapshot_date is not None and opening_snapshot_date.year == tax_year:
        processing_start_date = max(processing_start_date, opening_snapshot_date)

    if carryforward_only and authoritative_start_date is None:
        raise ValueError("carryforward_only requires authoritative_start_date to be provided.")

    same_year_reports = [report for report in same_year_reports if _report_in_authoritative_scope(report, authoritative_start_date)]
    lookahead_reports = [
        report
        for report in lookahead_reports
        if report.annual_reconciliation_period is None
        or authoritative_start_date is None
        or report.annual_reconciliation_period[1] >= authoritative_start_date
    ]

    if opening_snapshot_date is not None:
        opening_trades = [trade for trade in all_trades if opening_snapshot_date <= trade.trade_date < year_start]
    else:
        opening_trades = [trade for trade in all_trades if trade.trade_date < year_start]
    current_year_trades = [trade for trade in all_trades if processing_start_date <= trade.trade_date <= year_end]
    current_year_broker_events = [
        event for event in all_broker_events if event.pay_date.year == tax_year and event.pay_date >= processing_start_date
    ]
    current_year_confirmed_broker_events = [event for event in current_year_broker_events if _event_is_confirmed_cash(event)]
    fx_start_date, fx_end_date = _resolve_fx_bounds(
        tax_year=tax_year,
        opening_trades=opening_trades,
        current_year_trades=current_year_trades,
        reports=same_year_reports + lookahead_reports,
        broker_events=current_year_broker_events,
        has_previous_state=has_previous_state,
    )
    currencies = _resolve_required_currencies(
        opening_trades,
        current_year_trades,
        same_year_reports + lookahead_reports,
        current_year_broker_events,
    )
    fx_table = build_fx_table(
        start_date=fx_start_date,
        end_date=fx_end_date,
        raw_exchange_rates_path=raw_exchange_rates_path,
        currencies=currencies,
    )

    has_seeded_opening_state = has_previous_state or has_opening_state_snapshot
    working_positions = prepare_positions_for_new_year(previous_positions) if has_seeded_opening_state else []
    position_event_rows: list[dict[str, object]] = []
    if has_opening_state_snapshot and not has_previous_state and opening_snapshot_date is not None:
        reset_events = [
            build_basis_reset_event(
                broker=state.broker,
                ticker=state.ticker,
                isin=state.isin,
                currency=state.currency,
                asset_class=state.asset_class or "ETF",
                event_date=opening_snapshot_date,
                quantity=state.quantity,
                base_cost_total_eur=state.base_cost_total_eur,
                basis_adjustment_total_eur=state.basis_adjustment_total_eur,
                basis_method=state.basis_method or "move_in_fmv_reset",
                source_file=state.source_file,
                notes="Opening Austrian ETF basis-reset state.",
                sequence_key=index,
            )
            for index, state in enumerate(previous_positions)
        ]
        _, position_event_rows, _ = replay_events([], reset_events)
    if not has_previous_state:
        for index, trade in enumerate(opening_trades):
            apply_trade(working_positions, trade, fx_table=fx_table, sale_rows=None, sequence_key=index)

    payout_state = previous_payout_state
    payout_state_source_events = all_broker_events
    if carryforward_only:
        payout_state_source_events = [event for event in all_broker_events if event.pay_date >= processing_start_date]
    for event in payout_state_source_events:
        _upsert_payout_state_row(payout_state, event)

    payout_resolution_rows: list[dict[str, object]] = []
    payout_resolution_rows.extend(
        _match_distribution_report_to_payouts(
            list(payout_state.values()),
            same_year_reports,
            tax_year=tax_year,
        )
    )

    sale_rows: list[dict[str, object]] = []
    basis_adjustment_rows: list[dict[str, object]] = []
    income_rows: list[dict[str, object]] = [_build_broker_dividend_row(event, fx_table) for event in current_year_confirmed_broker_events]
    negative_review_rows: list[dict[str, object]] = []
    events: list[tuple[date, int, int, object]] = []
    events.extend((trade.trade_date, 1, index, trade) for index, trade in enumerate(current_year_trades))
    events.extend((report.eligibility_date, 0, index, report) for index, report in enumerate(same_year_reports))
    events.sort(key=lambda item: (item[0], item[1], item[2]))
    skipped_negative_report_keys: set[str] = set()

    for event_index, (_, _, _, payload) in enumerate(events):
        if isinstance(payload, OekbReport):
            if payload.is_ausschuettungsmeldung and not _has_confirmed_distribution_match(payload, current_year_confirmed_broker_events):
                continue
            if payload.age_per_share_ccy < 0:
                review_row = _resolve_negative_deemed_distribution_review(
                    payload,
                    working_positions,
                    broker_events_for_lookup,
                    negative_deemed_overrides,
                )
                negative_review_rows.append(review_row)
                report_key = str(review_row["report_key"])
                decision = str(review_row["decision"])
                eligible_quantity_used = quantize_qty(review_row["eligible_quantity_used"])

                if decision == NEGATIVE_DEEMED_DISTRIBUTION_IGNORE:
                    skipped_negative_report_keys.add(report_key)
                    continue
                if decision == NEGATIVE_DEEMED_DISTRIBUTION_BLOCK:
                    skipped_negative_report_keys.add(report_key)
                    continue

                if str(review_row["status"]) == NEGATIVE_DEEMED_DISTRIBUTION_APPLIED_AUTO:
                    note_prefix = "auto-applied negative deemed-distribution using reconciled broker payout set; "
                else:
                    note_prefix = (
                        "manual negative deemed-distribution override apply_full; "
                        if decision == NEGATIVE_DEEMED_DISTRIBUTION_APPLY_FULL
                        else "manual negative deemed-distribution override apply_partial; "
                    )
                if not carryforward_only:
                    income_rows.extend(
                        build_income_rows_for_report(
                            working_positions,
                            payload,
                            tax_year=tax_year,
                            fx_table=fx_table,
                            broker_events=current_year_confirmed_broker_events,
                            quantity_override=eligible_quantity_used,
                            note_prefix=note_prefix,
                        )
                    )
                basis_adjustment_rows.append(
                    apply_basis_correction(
                        working_positions,
                        payload,
                        tax_year=tax_year,
                        fx_table=fx_table,
                        quantity_override=eligible_quantity_used,
                        note_prefix=note_prefix,
                        event_rows=position_event_rows,
                        sequence_key_start=len(position_event_rows) + event_index,
                    )
                )
                continue
            if not carryforward_only:
                income_rows.extend(
                    build_income_rows_for_report(
                        working_positions,
                        payload,
                        tax_year=tax_year,
                        fx_table=fx_table,
                        broker_events=current_year_confirmed_broker_events,
                    )
                )
            basis_adjustment_rows.append(
                apply_basis_correction(
                    working_positions,
                    payload,
                    tax_year=tax_year,
                    fx_table=fx_table,
                    event_rows=position_event_rows,
                    sequence_key_start=len(position_event_rows) + event_index,
                )
            )
        else:
            apply_trade(
                working_positions,
                payload,
                fx_table=fx_table,
                sale_rows=sale_rows,
                event_rows=position_event_rows,
                sequence_key=len(position_event_rows) + event_index,
            )

    annual_reports_for_resolution = [
        report
        for report in same_year_reports + lookahead_reports
        if report.is_jahresmeldung and _negative_report_key(report) not in skipped_negative_report_keys
    ]
    non_reported_income_rows, annual_resolution_rows = _resolve_annual_10595_reports(
        list(payout_state.values()),
        annual_reports_for_resolution,
        target_tax_year=tax_year,
        positions_for_quantity=working_positions,
        fx_table=fx_table,
    )
    if not carryforward_only:
        income_rows.extend(non_reported_income_rows)
    payout_resolution_rows.extend(annual_resolution_rows)
    payout_resolution_rows.extend(
        _resolve_broker_cash_payouts_outside_annual_periods(
            list(payout_state.values()),
            annual_reports_for_resolution,
            tax_year=tax_year,
        )
    )

    payout_state_rows = list(payout_state.values())
    unresolved_current_year_rows = [
        payout
        for payout in payout_state_rows
        if payout.pay_date.year == tax_year
        and payout.status == PAYOUT_STATUS_UNRESOLVED_OPEN
    ]
    income_rows = _filter_superseded_broker_income_rows(income_rows, payout_state_rows)

    state_df = positions_to_df(working_positions)
    position_events_df = position_event_log_to_df(position_event_rows)
    income_events_df = income_events_to_df(income_rows)
    basis_adjustments_df = basis_adjustments_to_df(basis_adjustment_rows)
    sales_df = sales_to_df(sale_rows)
    payout_state_df = payout_state_to_df(payout_state_rows)
    payout_resolution_df = payout_resolution_events_to_df(payout_resolution_rows)
    payout_evidence_review_df = payout_evidence_review_to_df(payout_state_rows, tax_year)
    negative_review_df = negative_deemed_distribution_review_to_df(negative_review_rows)

    write_csv(state_df, current_state_path)
    income_events_path = output_dir_path / f"fund_tax_income_events_{tax_year}.csv"
    basis_adjustments_path = output_dir_path / f"fund_tax_basis_adjustments_{tax_year}.csv"
    position_events_path = output_dir_path / f"fund_tax_events_{tax_year}.csv"
    sales_path = output_dir_path / f"fund_tax_sales_{tax_year}.csv"
    payout_state_output_path = state_dir_path / "fund_tax_payout_state.csv"
    payout_resolution_path = output_dir_path / f"fund_tax_payout_resolution_events_{tax_year}.csv"
    payout_evidence_review_path = output_dir_path / f"fund_tax_payout_evidence_review_{tax_year}.csv"
    negative_review_path = output_dir_path / f"fund_tax_negative_deemed_distribution_review_{tax_year}.csv"
    summary_path = output_dir_path / f"reporting_funds_{tax_year}_summary.md"
    write_csv(position_events_df, position_events_path)
    write_csv(income_events_df, income_events_path)
    write_csv(basis_adjustments_df, basis_adjustments_path)
    write_csv(sales_df, sales_path)
    write_csv(payout_state_df, payout_state_output_path)
    write_csv(payout_resolution_df, payout_resolution_path)
    write_csv(payout_evidence_review_df, payout_evidence_review_path)
    write_csv(negative_review_df, negative_review_path)
    write_summary(
        summary_path,
        tax_year=tax_year,
        next_period_state_path=current_state_path,
        next_period_payout_state_path=payout_state_output_path,
        next_period_negative_override_path=negative_review_override_path,
        state_df=state_df,
        income_events_df=income_events_df,
        basis_adjustments_df=basis_adjustments_df,
        sales_df=sales_df,
        payout_state_df=payout_state_df,
        payout_evidence_review_df=payout_evidence_review_df,
        negative_review_df=negative_review_df,
        carryforward_only=carryforward_only,
        authoritative_start_date=authoritative_start_date,
    )

    unresolved_negative_rows = [row for row in negative_review_rows if row["status"] == NEGATIVE_DEEMED_DISTRIBUTION_BLOCK]
    if unresolved_negative_rows:
        unresolved_text = "; ".join(
            (
                f"{row['ticker']} ({row['isin']}) report_date={row['report_date']} "
                f"deemed_distributed_income_per_share={row['deemed_distributed_income_per_share_ccy']}"
            )
            for row in unresolved_negative_rows
        )
        raise ValueError(
            "Negative deemed distributed income requires manual review. "
            f"Review file: {negative_review_path}. Unresolved reports: {unresolved_text}"
        )

    if strict_unresolved_payouts and unresolved_current_year_rows:
        unresolved_text = "; ".join(
            (
                f"{row.ticker} ({row.isin}) pay_date={row.pay_date.isoformat()} ex_date="
                f"{row.ex_date.isoformat() if row.ex_date else '-'} gross={row.broker_gross_amount_ccy}"
            )
            for row in unresolved_current_year_rows
        )
        raise ValueError(
            "Unresolved ETF broker payouts remain after same-year and lookahead OeKB resolution: "
            f"{unresolved_text}"
        )

    return {
        "state": current_state_path,
        "events": position_events_path,
        "income_events": income_events_path,
        "basis_adjustments": basis_adjustments_path,
        "oekb_adjustments": basis_adjustments_path,
        "sales": sales_path,
        "payout_state": payout_state_output_path,
        "payout_resolution_events": payout_resolution_path,
        "payout_evidence_review": payout_evidence_review_path,
        "negative_deemed_distribution_review": negative_review_path,
        "summary": summary_path,
    }
