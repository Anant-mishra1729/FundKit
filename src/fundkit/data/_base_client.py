"""Base AMFI Client."""

from __future__ import annotations

import asyncio
import logging
import os
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

DEFAULT_AUTOCOMPLETE_LIMIT = 50


def clear_memory_caches() -> None:
    """Release all in-process data caches (NAV, scheme details, historical locks).

    Disk parquet caches are kept. Call from long-running apps (e.g. Streamlit) when
    you need to free RAM or force a clean reload on the next request.
    """
    from fundkit.data.historical_nav_client import HistoricalNAVClient
    from fundkit.data.scheme_details import SchemeDetailsClient

    BaseAMFIClient.clear_memory_cache()
    SchemeDetailsClient.clear_memory_cache()
    HistoricalNAVClient.clear_historical_cache_locks()


class BaseClient:
    """Base client with shared utilities."""

    _cache_path = Path(user_cache_dir("fundkit"))

    OUTPUT_DATAFRAME_FORMAT = Literal["polars", "pandas"]

    def __init__(self, verbose: bool = False) -> None:
        self._verbose = verbose
        if verbose and not logging.getLogger("fundkit").handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            logging.getLogger("fundkit").setLevel(logging.INFO)
            logging.getLogger("fundkit").addHandler(handler)

    def _log(self, message: str) -> None:
        if self._verbose:
            logger.info(message)

    @staticmethod
    async def _write_parquet_atomic(path: Path, df: pl.DataFrame) -> None:
        """Write parquet atomically so readers never see a partial file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.stem}.{os.getpid()}.tmp")
        try:
            await asyncio.to_thread(df.write_parquet, tmp_path)
            await asyncio.to_thread(os.replace, tmp_path, path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

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


class BaseAMFIClient(BaseClient):
    """Shared base for all AMFI data clients."""

    _NAV_CACHE_FILENAME = "nav.parquet"

    _nav_cache_df: pl.DataFrame | None = None
    _nav_cache_loaded_on: date | None = None
    _scheme_codes: frozenset[int] | None = None
    _scheme_code_to_amc_id: dict[int, int] | None = None
    _nav_cache_lock: asyncio.Lock = asyncio.Lock()

    def __init__(self, verbose: bool = False) -> None:
        super().__init__(verbose=verbose)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    @staticmethod
    def _build_indices(df: pl.DataFrame) -> None:
        """Populate the shared scheme-code lookup structures from a loaded NAV DataFrame.

        Called once after every cache load (memory, disk, or network).
        """
        unique = df.unique(subset=["scheme_code"], keep="last")
        scheme_codes = unique["scheme_code"].to_list()
        amc_ids = unique["amc_id"].to_list()
        BaseAMFIClient._scheme_code_to_amc_id = dict(
            zip(scheme_codes, amc_ids, strict=True)
        )
        BaseAMFIClient._scheme_codes = frozenset(scheme_codes)

    @classmethod
    def clear_memory_cache(cls) -> None:
        """Release the in-process latest-NAV cache and lookup indices."""
        cls._nav_cache_df = None
        cls._nav_cache_loaded_on = None
        cls._scheme_code_to_amc_id = None
        cls._scheme_codes = None

    @classmethod
    def _nav_cache_path(cls) -> Path:
        return cls._cache_path / cls._NAV_CACHE_FILENAME

    async def refresh_nav_cache(self) -> None:
        """Force-refresh the shared latest-NAV cache from AMFI.

        Raises:
            CacheCreationError: If the cache file cannot be written.

        """
        today = date.today()
        async with BaseAMFIClient._nav_cache_lock:
            self._log("Refreshing NAV cache: fetching from AMFI.")
            async with SchemeParser() as parser:
                BaseAMFIClient._nav_cache_df = await parser.fetch_nav_data()
                BaseAMFIClient._nav_cache_loaded_on = today
            nav_cache_path = self._nav_cache_path()
            try:
                await BaseClient._write_parquet_atomic(
                    nav_cache_path, BaseAMFIClient._nav_cache_df
                )
                self._log(f"NAV cache written to {nav_cache_path}.")
            except OSError as e:
                raise CacheCreationError("Error occurred while generating NAV cache") from e
            BaseAMFIClient._build_indices(BaseAMFIClient._nav_cache_df)

    async def _get_nav_cache(self) -> pl.DataFrame:
        today = date.today()

        if (
            BaseAMFIClient._nav_cache_df is not None
            and BaseAMFIClient._nav_cache_loaded_on == today
        ):
            self._log("Memory hit: returning in-memory NAV cache")
            if BaseAMFIClient._scheme_code_to_amc_id is None or BaseAMFIClient._scheme_codes is None:
                BaseAMFIClient._build_indices(BaseAMFIClient._nav_cache_df)
            return BaseAMFIClient._nav_cache_df

        async with BaseAMFIClient._nav_cache_lock:
            if (
                BaseAMFIClient._nav_cache_df is not None
                and BaseAMFIClient._nav_cache_loaded_on == today
            ):
                self._log("Memory hit (post-lock): returning in-memory NAV cache")
                if BaseAMFIClient._scheme_code_to_amc_id is None or BaseAMFIClient._scheme_codes is None:
                    BaseAMFIClient._build_indices(BaseAMFIClient._nav_cache_df)
                return BaseAMFIClient._nav_cache_df

            BaseAMFIClient._nav_cache_df = None
            BaseAMFIClient._nav_cache_loaded_on = None
            BaseAMFIClient._scheme_code_to_amc_id = None
            BaseAMFIClient._scheme_codes = None

            nav_cache_path = self._nav_cache_path()
            if nav_cache_path.exists() and date.fromtimestamp(nav_cache_path.stat().st_mtime) == today:
                self._log(f"Disk hit: loading NAV cache from {nav_cache_path}.")
                BaseAMFIClient._nav_cache_df = await asyncio.to_thread(
                    pl.read_parquet, nav_cache_path
                )
                BaseAMFIClient._nav_cache_loaded_on = today
                BaseAMFIClient._build_indices(BaseAMFIClient._nav_cache_df)
                return BaseAMFIClient._nav_cache_df

            self._log("Cache miss: fetching NAV data from AMFI.")
            async with SchemeParser() as parser:
                BaseAMFIClient._nav_cache_df = await parser.fetch_nav_data()
                BaseAMFIClient._nav_cache_loaded_on = today
            try:
                await BaseClient._write_parquet_atomic(
                    nav_cache_path, BaseAMFIClient._nav_cache_df
                )
                self._log(f"NAV cache written to {nav_cache_path}.")
            except OSError as e:
                raise CacheCreationError("Error occurred while generating NAV cache") from e

            BaseAMFIClient._build_indices(BaseAMFIClient._nav_cache_df)
            return BaseAMFIClient._nav_cache_df

    async def _search_scheme_str(
        self,
        query: str,
        col_type: Literal["scheme_name", "amc", "scheme_type"],
        limit: int | None = None,
        case_sensitive: bool = True,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:

        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        df = await self._get_nav_cache()
        col = pl.col(col_type).cast(pl.String) if col_type in ("amc", "scheme_type") else pl.col("scheme_name")

        if not case_sensitive:
            query = query.lower()
            col = pl.col("scheme_name_lower") if col_type == "scheme_name" else col.str.to_lowercase()

        rows = df.filter(col.str.contains(query, literal=True))

        if rows.is_empty():
            empty_df = pl.DataFrame(schema=df.schema)
            return self._export_dataframe(empty_df, df_format)

        if limit is not None:
            rows = rows.head(limit)

        rows = rows.drop("scheme_name_lower")

        return self._export_dataframe(rows, df_format)

    async def _search_scheme_code(
        self,
        scheme_code: int | list[int],
        limit: int | None = None,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        codes = scheme_code if isinstance(scheme_code, list) else [scheme_code]
        if not codes:
            raise ValueError("scheme_code list cannot be empty.")

        df = await self._get_nav_cache()  # ensure _scheme_codes is populated

        result = df.filter(pl.col("scheme_code").is_in(codes)).drop("scheme_name_lower")

        if limit is not None:
            result = result.head(limit)

        return self._export_dataframe(result, df_format)

    async def _search_amc_id(self, scheme_code: int) -> int | None:
        """Lookup for AMC ID given the scheme code.

        Args:
            scheme_code (int): Scheme Code

        Returns:
            int: AMC ID

        """
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
        await self._get_nav_cache()
        assert BaseAMFIClient._scheme_codes is not None
        return scheme_code in BaseAMFIClient._scheme_codes

    async def get_scheme_codes(
        self,
        query: str | int | None = None,
        by: Literal["scheme_name", "scheme_code"] | None = None,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return scheme codes and names, optionally filtered.

        Args:
            query: Search term - int for scheme_code, str for scheme_name.
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

    async def list_schemes(
        self,
        *,
        amc_id: int | None = None,
        scheme_type: str | None = None,
        name_query: str | None = None,
        limit: int | None = None,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return schemes for dropdowns and filters.

        Args:
            amc_id: Restrict to one fund house.
            scheme_type: Exact match on scheme type (e.g. ``Open Ended Schemes`` section).
            name_query: Case-insensitive substring match on scheme name.
            limit: Max rows (recommended for UI autocomplete).
            df_format: ``polars`` or ``pandas``.

        Returns:
            Columns: ``scheme_code``, ``scheme_name``, ``amc``, ``scheme_type``,
            ``amc_id``, ``nav``, ``date``.

        """
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        df = await self._get_nav_cache()
        result = df.select(
            [
                "scheme_code",
                "scheme_name",
                "amc",
                "scheme_type",
                "amc_id",
                "nav",
                "date",
                "scheme_name_lower",
            ]
        )

        if amc_id is not None:
            result = result.filter(pl.col("amc_id") == amc_id)
        if scheme_type is not None:
            result = result.filter(pl.col("scheme_type") == scheme_type)
        if name_query is not None:
            result = result.filter(
                pl.col("scheme_name_lower").str.contains(name_query.lower(), literal=True)
            )
        if limit is not None:
            result = result.head(limit)

        return self._export_dataframe(result.drop("scheme_name_lower"), df_format)

    async def search_schemes(
        self,
        query: str,
        *,
        limit: int = DEFAULT_AUTOCOMPLETE_LIMIT,
        case_sensitive: bool = False,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Autocomplete search on scheme name (bounded row count for UIs)."""
        return await self._search_scheme_str(
            query=query,
            col_type="scheme_name",
            limit=limit,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def get_scheme_types(
        self,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return distinct scheme types for a filter dropdown."""
        df = await self._get_nav_cache()
        result = (
            df
            .select("scheme_type")
            .unique()
            .drop_nulls()
            .sort("scheme_type")
        )
        return self._export_dataframe(result, df_format)

    async def get_amc_list(
        self,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
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
