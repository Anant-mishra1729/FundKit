"""FundKit — Async-first Python library for Indian Mutual Fund analytics."""

from fundkit.data.historical_nav_client import HistoricalNAVClient
from fundkit.data.nav_client import NAVClient
from fundkit.data.scheme_details import SchemeDetailsClient

__all__ = ["HistoricalNAVClient", "NAVClient", "SchemeDetailsClient"]

__version__ = "0.1.0"
