from enum import StrEnum, auto

KEST_RATE = 0.275
MAX_DTT_RATE = 0.15  # Maximum double taxation treaty rate
EXCHANGE_RATE_DATES_ACCEPTABLE_OFFSET = 7  # days
FLOAT_PRECISION = 4


class TransactionTypeIBKR(StrEnum):
    dividend = "Dividends"
    tax = "Withholding Tax"
    other_fee = "Other Fee"
    pil = "Payment In Lieu Of Dividends"


class CorporateActionTypesFF(StrEnum):
    dividend = auto()
    dividend_reverted = auto()


class Column(StrEnum):
    amount = auto()
    amount_euro = auto()
    amount_euro_net = auto()
    amount_euro_received_total = auto()
    amount_per_share = auto()
    corporate_action_id = auto()
    currency = auto()
    date = auto()
    dividends = auto()
    dividends_euro = auto()
    dividends_euro_net_total = auto()
    dividends_euro_total = auto()
    exchange_rate = auto()
    kest_gross = auto()
    kest_gross_total = auto()
    kest_net = auto()
    kest_net_total = auto()
    profit = auto()
    profit_gross = auto()
    profit_euro = auto()
    profit_gross_euro = auto()
    profit_gross_euro_total = auto()
    profit_euro_net = auto()
    profit_euro_net_total = auto()
    profit_euro_total = auto()
    profit_total = auto()
    rate_date = auto()
    shares_count = auto()
    ticker = auto()
    type = auto()
    withholding_tax = auto()
    withholding_tax_euro = auto()
    withholding_tax_euro_total = auto()


class RevolutColumn(StrEnum):
    date = auto()
    type = auto()
    currency = auto()
    amount = auto()


class CurrencyCode(StrEnum):
    euro = "EUR"
    usd = "USD"


class RevolutType(StrEnum):
    buy = "buy"
    interest = "interest"
    fee = "fee"
