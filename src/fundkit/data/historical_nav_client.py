"""Historical NAV Data."""

from datetime import date
from pathlib import Path
from typing import Literal, Self

import polars as pl
from platformdirs import user_cache_dir


class HistoricalNAVClient:
    """Fetch Historical NAV Data from AFMI-India website."""

    _cache_path = Path(user_cache_dir("fundkit")) / "historical"
    OUTPUT_DATAFRAME_FORMAT = Literal["polars", "pandas"]
    _df: pl.DataFrame | None = None
    _df_loaded_on: date | None = None

    def __enter__(self) -> Self:
        return self
