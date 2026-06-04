"""FundKit - Async-first Python library for Indian Mutual Fund analytics."""

from fundkit.data import clear_memory_caches
from fundkit.data.historical_nav_client import HistoricalNAVClient
from fundkit.data.nav_client import NAVClient
from fundkit.data.scheme_details import SchemeDetailsClient

__all__ = [
    "HistoricalNAVClient",
    "NAVClient",
    "SchemeDetailsClient",
    "clear_memory_caches",
]

__version__ = "0.1.3.post0"
