"""Base AMFI Client Class."""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from importlib.util import find_spec
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self

import polars as pl
from platformdirs import user_cache_dir

from fundkit.data.scheme_parser import SchemeParser
from fundkit.exceptions import CacheCreationError, PandasExportError

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class BaseAMFIClient:
    """Shared base for all AMFI data clients."""

    _cache_path = Path(user_cache_dir("fundkit"))

    OUTPUT_DATAFRAME_FORMAT = Literal["polars", "pandas"]

    # Scheme vars
    _nav_df: pl.DataFrame | None = None
    _nav_df_loaded_on: date | None = None
    _scheme_codes: frozenset[int] | None = None
    _scheme_code_to_amc_id: dict[int, int] | None = None

    def __init__(self, verbose: bool = False) -> None:
        self._verbose = verbose
        if verbose and not logging.getLogger("fundkit").handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            logging.getLogger("fundkit").setLevel(logging.INFO)
            logging.getLogger("fundkit").addHandler(handler)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._nav_df = None
        self._nav_df_loaded_on = None

    def _log(self, message: str) -> None:
        if self._verbose:
            logger.info(message)

    def _export_dataframe(self, df: pl.DataFrame, df_format: OUTPUT_DATAFRAME_FORMAT) -> pl.DataFrame | pd.DataFrame:
        if df_format == "pandas":
            if find_spec("pandas") is None:
                raise PandasExportError(
                    "Pandas is required to use df_format='pandas'. "
                    "Install it via 'pip install pandas' "
                    "or 'pip install fundkit[pandas]'."
                )
            return df.to_pandas()
        return df

    async def _get_nav_cache(self) -> pl.DataFrame:
        today = date.today()

        # Load from memory
        if BaseAMFIClient._nav_df is not None and BaseAMFIClient._nav_df_loaded_on == today:
            self._log("Memory hit: returning in-memory NAV DataFrame")
            if BaseAMFIClient._scheme_code_to_amc_id is None:
                BaseAMFIClient._scheme_code_to_amc_id = dict(
                    zip(
                        BaseAMFIClient._nav_df["scheme_code"].to_list(),
                        BaseAMFIClient._nav_df["amc_id"].to_list(),
                        strict=True,
                    )
                )
            if BaseAMFIClient._scheme_codes is None:
                BaseAMFIClient._scheme_codes = frozenset(BaseAMFIClient._nav_df["scheme_code"].to_list())
            return BaseAMFIClient._nav_df

        # Load from disk cache
        BaseAMFIClient._nav_df = None
        BaseAMFIClient._nav_df_loaded_on = None

        cache_file_path = self._cache_path / "nav.parquet"
        if cache_file_path.exists() and date.fromtimestamp(cache_file_path.stat().st_mtime) == today:
            self._log(f"Disk hit: loading NAV cache from {cache_file_path}.")
            BaseAMFIClient._nav_df = await asyncio.to_thread(pl.read_parquet, cache_file_path)
            BaseAMFIClient._nav_df_loaded_on = today
            BaseAMFIClient._scheme_code_to_amc_id = dict(
                zip(
                    BaseAMFIClient._nav_df["scheme_code"].to_list(),
                    BaseAMFIClient._nav_df["amc_id"].to_list(),
                    strict=True,
                )
            )
            BaseAMFIClient._scheme_codes = frozenset(BaseAMFIClient._nav_df["scheme_code"].to_list())
            return BaseAMFIClient._nav_df

        # Fetch from AMFI
        self._log("Cache miss: fetching NAV data from AMFI.")
        async with SchemeParser() as parser:
            BaseAMFIClient._nav_df = await parser.fetch_nav_data()
            BaseAMFIClient._nav_df_loaded_on = today
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(BaseAMFIClient._nav_df.write_parquet, cache_file_path)
            self._log(f"NAV cache written to {cache_file_path}.")

        except OSError as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e

        BaseAMFIClient._scheme_code_to_amc_id = dict(
            zip(
                BaseAMFIClient._nav_df["scheme_code"].to_list(),
                BaseAMFIClient._nav_df["amc_id"].to_list(),
                strict=True,
            )
        )
        BaseAMFIClient._scheme_codes = frozenset(BaseAMFIClient._nav_df["scheme_code"].to_list())
        return BaseAMFIClient._nav_df

    async def _search_scheme_str(
        self,
        query: str,
        col_type: Literal["scheme_name", "amc", "scheme_type"],
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:

        if suggestion_count is not None and suggestion_count < 1:
            raise ValueError(f"suggestion_count must be at least 1, got {suggestion_count}.")

        df = await self._get_nav_cache()
        if col_type in ("amc", "scheme_type"):
            col = pl.col(col_type).cast(pl.String)
        elif not case_sensitive:
            col = pl.col("scheme_name_lower")
            query = query.lower()
        else:
            col = pl.col("scheme_name")

        rows = df.filter(col.str.contains(query, literal=True))

        if rows.is_empty():
            return rows

        if suggestion_count is not None:
            rows = rows.head(suggestion_count)

        rows = rows.drop("scheme_name_lower")

        return self._export_dataframe(rows, df_format)

    async def _search_scheme_code(
        self,
        scheme_code: int | list[int],
        suggestion_count: int | None = None,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        if suggestion_count is not None and suggestion_count < 1:
            raise ValueError(f"suggestion_count must be at least 1, got {suggestion_count}.")

        codes = scheme_code if isinstance(scheme_code, list) else [scheme_code]
        if not codes:
            raise ValueError("scheme_code list cannot be empty.")

        df = await self._get_nav_cache()  # ensure _scheme_codes is populated

        invalid = valid_codes = None

        if BaseAMFIClient._scheme_codes is not None:
            invalid = [c for c in codes if c not in BaseAMFIClient._scheme_codes]
            valid_codes = [c for c in codes if c in BaseAMFIClient._scheme_codes]

        if invalid:
            self._log(f"Ignoring invalid scheme code(s): {invalid}")

        if not valid_codes:
            empty = pl.DataFrame(schema=df.schema).drop("scheme_name_lower")
            return empty.to_pandas() if df_format == "pandas" else empty

        result = df.filter(pl.col("scheme_code").is_in(valid_codes)).drop("scheme_name_lower")

        if suggestion_count is not None:
            result = result.head(suggestion_count)

        return self._export_dataframe(result, df_format)

    async def _search_amc_id(self, scheme_code: int) -> int | None:
        """Lookup for AMC ID given the scheme code.

        Args:
            scheme_code (int): Scheme Code

        Returns:
            int: AMC ID

        """
        if BaseAMFIClient._scheme_code_to_amc_id is None:
            await self._get_nav_cache()
        assert BaseAMFIClient._scheme_code_to_amc_id is not None
        return BaseAMFIClient._scheme_code_to_amc_id.get(scheme_code)

    async def is_valid_scheme_code(self, scheme_code: int) -> bool:
        """Validate the scheme code.

        Args:
            scheme_code (int): Scheme Code.

        Returns:
            bool: True if the scheme code is valid, otherwise False.

        """
        if BaseAMFIClient._scheme_codes is None:
            await self._get_nav_cache()
        assert BaseAMFIClient._scheme_codes is not None
        return scheme_code in BaseAMFIClient._scheme_codes

    async def get_scheme_codes(
        self,
        query: str | int | None = None,
        by: Literal["scheme_name", "scheme_code"] | None = None,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return scheme codes and names, optionally filtered.

        Args:
            query: Search term — int for scheme_code, str for scheme_name.
            by: Column to filter on. Must match query type.
            df_format: Output format.

        Raises:
            ValueError: If only one of query/by is provided.
            ValueError: If query type doesn't match by column.

        Returns:
            DataFrame with scheme_code and scheme_name columns.

        """
        if (query is None) != (by is None):
            raise ValueError("Both query and by must be provided together, or neither.")

        df = await self._get_nav_cache()
        result = df.select(["scheme_code", "scheme_name", "scheme_name_lower"])

        if query is not None and by is not None:
            if by == "scheme_code":
                if not isinstance(query, int):
                    raise ValueError(f"query must be int when by='scheme_code', got {type(query).__name__}.")
                result = result.filter(pl.col("scheme_code") == query)
            elif by == "scheme_name":
                if not isinstance(query, str):
                    raise ValueError(f"query must be str when by='scheme_name', got {type(query).__name__}.")
                result = result.filter(pl.col("scheme_name_lower").str.contains(query.lower(), literal=True))

        result = result.drop("scheme_name_lower")
        return self._export_dataframe(result, df_format)

    async def get_amc_list(
        self,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return all unique AMC names with their amc_id.

        Args:
            df_format: Output format.

        Returns:
            DataFrame with amc and amc_id columns, sorted by amc_id.

        """
        df = await self._get_nav_cache()
        result = (
            df
            .select(["amc", "amc_id"])
            .with_columns(pl.col("amc").cast(pl.String))
            .unique(subset=["amc"])
            .drop_nulls(subset=["amc"])
            .sort("amc_id")
        )
        return self._export_dataframe(result, df_format)
