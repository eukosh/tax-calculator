from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import date, datetime

from tax_automation.broker_history import round_money, round_qty
from tax_automation.precision import to_output_float


def _append_unique_note_text(existing: str, incoming: str) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for part in (piece.strip() for piece in existing.split(";")):
        if not part or part in seen:
            continue
        merged.append(part)
        seen.add(part)
    for part in (piece.strip() for piece in incoming.split(";")):
        if not part or part in seen:
            continue
        merged.append(part)
        seen.add(part)
    return "; ".join(merged)


@dataclass(frozen=True)
class IbkrTrade:
    ticker: str
    isin: str
    trade_date: date
    trade_datetime: datetime
    operation: str
    quantity: Decimal
    price_ccy: Decimal
    currency: str
    trade_id: str
    account_id: str = ""
    source_statement_file: str = ""


@dataclass(frozen=True)
class OekbReport:
    ticker: str
    isin: str
    meldedatum: date
    currency: str
    is_jahresmeldung: bool
    is_ausschuettungsmeldung: bool
    ausschuettungstag: date | None
    ex_tag: date | None
    meldezeitraum_beginn: date | None
    meldezeitraum_ende: date | None
    geschaeftsjahres_beginn: date | None
    geschaeftsjahres_ende: date | None
    reported_distribution_per_share_ccy: Decimal
    age_per_share_ccy: Decimal
    non_reported_distribution_per_share_ccy: Decimal
    creditable_foreign_tax_per_share_ccy: Decimal
    acquisition_cost_correction_per_share_ccy: Decimal
    source_file: str
    domestic_dividends_loss_offset_per_share_ccy: Decimal = Decimal("0")
    domestic_dividend_kest_per_share_ccy: Decimal = Decimal("0")
    total_shares_at_inflow: Decimal | None = None
    total_distributions_per_share_ccy: Decimal | None = None
    capital_repayment_per_share_ccy: Decimal | None = None
    basis_age_component_per_share_ccy: Decimal | None = None
    basis_distribution_component_per_share_ccy: Decimal | None = None
    withheld_tax_on_non_reported_distributions_per_share_ccy: Decimal | None = None

    @property
    def eligibility_date(self) -> date:
        return self.ex_tag or self.ausschuettungstag or self.meldedatum

    @property
    def payout_date(self) -> date | None:
        return self.ausschuettungstag or self.meldedatum

    @property
    def annual_reconciliation_period(self) -> tuple[date, date] | None:
        if self.meldezeitraum_beginn and self.meldezeitraum_ende:
            return self.meldezeitraum_beginn, self.meldezeitraum_ende
        if self.geschaeftsjahres_beginn and self.geschaeftsjahres_ende:
            return self.geschaeftsjahres_beginn, self.geschaeftsjahres_ende
        return None


@dataclass(frozen=True)
class IbkrDividendAccrualRow:
    ticker: str
    isin: str
    currency: str
    report_date: date
    date: date
    ex_date: date | None
    pay_date: date | None
    quantity: Decimal
    tax: Decimal | None
    gross_rate: Decimal | None
    gross_amount: Decimal | None
    net_amount: Decimal | None
    code: str
    action_id: str
    account_id: str = ""
    source_statement_file: str = ""


@dataclass(frozen=True)
class IbkrCashDividendRow:
    ticker: str
    isin: str
    currency: str
    settle_date: date
    ex_date: date | None
    amount: Decimal
    action_id: str
    account_id: str = ""
    report_date: date | None = None
    source_statement_file: str = ""


@dataclass(frozen=True)
class BrokerDividendEvent:
    ticker: str
    isin: str
    currency: str
    ex_date: date | None
    pay_date: date
    quantity: Decimal
    gross_rate: Decimal | None
    gross_amount: Decimal | None
    net_amount: Decimal | None
    tax: Decimal | None
    has_po: bool
    has_re: bool
    action_id: str
    source_statement_file: str
    cash_amount: Decimal | None = None
    accrual_amount: Decimal | None = None
    matching_notes: str = ""
    evidence_state: str = "confirmed_cash"

    @property
    def event_id(self) -> str:
        return self.action_id or (
            f"{self.isin}:{self.ex_date.isoformat() if self.ex_date else 'none'}:"
            f"{self.pay_date.isoformat()}:{round_qty(self.quantity)}"
        )


@dataclass
class PayoutStateRow:
    payout_key: str
    ticker: str
    isin: str
    ex_date: date | None
    pay_date: date
    quantity: Decimal
    currency: str
    broker_gross_amount_ccy: Decimal
    broker_net_amount_ccy: Decimal
    broker_tax_amount_ccy: Decimal
    source_tax_year: int
    evidence_state: str
    status: str
    resolved_tax_year: str = ""
    resolved_by_report_year: str = ""
    resolved_by_report_file: str = ""
    resolution_mode: str = ""
    action_id: str = ""
    source_statement_file: str = ""
    notes: str = ""

    def add_note(self, note: str) -> None:
        if not note:
            return
        self.notes = _append_unique_note_text(self.notes, note)

    def to_record(self) -> dict[str, object]:
        return {
            "payout_key": self.payout_key,
            "ticker": self.ticker,
            "isin": self.isin,
            "ex_date": self.ex_date.isoformat() if self.ex_date else "",
            "pay_date": self.pay_date.isoformat(),
            "quantity": to_output_float(round_qty(self.quantity)),
            "currency": self.currency,
            "broker_gross_amount_ccy": to_output_float(round_money(self.broker_gross_amount_ccy)),
            "broker_net_amount_ccy": to_output_float(round_money(self.broker_net_amount_ccy)),
            "broker_tax_amount_ccy": to_output_float(round_money(self.broker_tax_amount_ccy)),
            "source_tax_year": self.source_tax_year,
            "evidence_state": self.evidence_state,
            "status": self.status,
            "resolved_tax_year": self.resolved_tax_year,
            "resolved_by_report_year": self.resolved_by_report_year,
            "resolved_by_report_file": self.resolved_by_report_file,
            "resolution_mode": self.resolution_mode,
            "action_id": self.action_id,
            "source_statement_file": self.source_statement_file,
            "notes": self.notes,
        }


@dataclass
class Lot:
    ticker: str
    isin: str
    lot_id: str
    buy_date: date
    original_quantity: Decimal
    remaining_quantity: Decimal
    currency: str
    buy_price_ccy: Decimal
    buy_fx_to_eur: Decimal
    original_cost_eur: Decimal
    cumulative_oekb_stepup_eur: Decimal = Decimal("0")
    status: str = "open"
    broker: str = "ibkr"
    account_id: str = ""
    notes: str = ""
    last_adjustment_year: str = ""
    last_adjustment_reference: str = ""
    last_sale_date: str = ""
    sold_quantity_ytd: Decimal = Decimal("0")
    source_trade_id: str = ""
    source_statement_file: str = ""

    @property
    def adjusted_basis_eur(self) -> Decimal:
        return round_money(self.original_cost_eur + self.cumulative_oekb_stepup_eur)

    def add_note(self, note: str) -> None:
        if not note:
            return
        self.notes = _append_unique_note_text(self.notes, note)

    def to_record(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "isin": self.isin,
            "lot_id": self.lot_id,
            "buy_date": self.buy_date.isoformat(),
            "original_quantity": to_output_float(round_qty(self.original_quantity)),
            "remaining_quantity": to_output_float(round_qty(self.remaining_quantity)),
            "currency": self.currency,
            "buy_price_ccy": to_output_float(round_money(self.buy_price_ccy)),
            "buy_fx_to_eur": to_output_float(round_money(self.buy_fx_to_eur)),
            "original_cost_eur": to_output_float(round_money(self.original_cost_eur)),
            "cumulative_oekb_stepup_eur": to_output_float(round_money(self.cumulative_oekb_stepup_eur)),
            "adjusted_basis_eur": to_output_float(self.adjusted_basis_eur),
            "status": self.status,
            "broker": self.broker,
            "account_id": self.account_id,
            "notes": self.notes,
            "last_adjustment_year": self.last_adjustment_year,
            "last_adjustment_reference": self.last_adjustment_reference,
            "last_sale_date": self.last_sale_date,
            "sold_quantity_ytd": to_output_float(round_qty(self.sold_quantity_ytd)),
            "source_trade_id": self.source_trade_id,
            "source_statement_file": self.source_statement_file,
        }
