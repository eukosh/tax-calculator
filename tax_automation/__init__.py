"""Public package exports."""

from tax_automation.broker_history import RawBrokerTrade, build_fx_table_from_rates_df, get_fx_rate, load_ibkr_stock_like_trades
from tax_automation.currencies import ExchangeRates, ExchangeRatesCacheError
from tax_automation.providers.ibkr import apply_pivot, handle_dividend_adjustments
from tax_automation.utils import convert_to_euro, extract_elements, join_exchange_rates, read_xml_to_df

__all__ = [
    "ExchangeRates",
    "ExchangeRatesCacheError",
    "RawBrokerTrade",
    "apply_pivot",
    "build_fx_table_from_rates_df",
    "convert_to_euro",
    "extract_elements",
    "get_fx_rate",
    "handle_dividend_adjustments",
    "join_exchange_rates",
    "load_ibkr_stock_like_trades",
    "read_xml_to_df",
]
