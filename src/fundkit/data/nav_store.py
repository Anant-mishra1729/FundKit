from __future__ import annotations  # noqa: D100

import logging
import re
from datetime import date
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self, overload

import polars as pl
from platformdirs import user_cache_dir

from fundkit.data.scheme_parser import SchemeParser

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class CacheCreationError(Exception):
    """Error occured in NAV cache creation."""

    pass


class NAVStore:
    """Fetch NAV Data from AFMI-India website."""

    _cache_path = Path(user_cache_dir("fundkit"))
    OUTPUT_DATAFRAME_FORMAT = Literal["polars", "pandas"]
    _df: pl.DataFrame | None = None
    _df_loaded_on: date | None = None

    def __init__(self, verbose: bool = False) -> None:
        self._verbose = verbose

    def _log(self, message: str) -> None:
        if self._verbose:
            logger.info(message)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        NAVStore._df = None
        NAVStore._df_loaded_on = None

    async def refresh_nav_cache(self) -> None:
        """Refresh NAV Cache.

        Raises:
            CacheCreationError: OS Error occured in NAV cache creation.

        """
        today = date.today()
        self._log("Refreshing cache: fetching NAV data from AMFI.")
        async with SchemeParser() as parser:
            NAVStore._df = await parser.fetch_nav_data()
            NAVStore._df_loaded_on = today
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            cache_file_path = self._cache_path / "nav.parquet"
            await asyncio.to_thread(NAVStore._df.write_parquet, cache_file_path)
            self._log(f"NAV cache written to {cache_file_path}.")

        except OSError as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e

    async def _get_cache(self) -> pl.DataFrame:
        today = date.today()

        if NAVStore._df is not None and NAVStore._df_loaded_on == today:
            self._log("Memory hit: returning in-memory NAV DataFrame")
            return NAVStore._df

        NAVStore._df = None
        NAVStore._df_loaded_on = None

        cache_file_path = self._cache_path / "nav.parquet"
        if cache_file_path.exists() and date.fromtimestamp(cache_file_path.stat().st_mtime) == today:
            self._log(f"Disk hit: loading NAV cache from {cache_file_path}.")
            NAVStore._df = await asyncio.to_thread(pl.read_parquet, cache_file_path)
            NAVStore._df_loaded_on = today
            return NAVStore._df

        self._log("Cache miss: fetching NAV data from AMFI.")
        async with SchemeParser() as parser:
            NAVStore._df = await parser.fetch_nav_data()
            NAVStore._df_loaded_on = today
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(NAVStore._df.write_parquet, cache_file_path)
            self._log(f"NAV cache written to {cache_file_path}.")

        except OSError as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e
        return NAVStore._df

    async def is_valid_scheme_code(self, scheme_code: int) -> bool:
        """Validate the scheme code.

        Args:
            scheme_code (int): Scheme Code.

        Returns:
            bool: True if the scheme code is valid, otherwise False.

        """
        df = await self._get_cache()
        return not df.filter(pl.col("scheme_code") == scheme_code).is_empty()

    @overload
    async def search_scheme_by_code(
        self, scheme_code: int, df_format: OUTPUT_DATAFRAME_FORMAT = "polars"
    ) -> pl.DataFrame | pd.DataFrame: ...
    @overload
    async def search_scheme_by_code(
        self, scheme_code: list[int], df_format: OUTPUT_DATAFRAME_FORMAT = "polars"
    ) -> pl.DataFrame | pd.DataFrame: ...

    async def search_scheme_by_code(
        self, scheme_code: int | list[int], df_format: OUTPUT_DATAFRAME_FORMAT = "polars"
    ) -> pl.DataFrame | pd.DataFrame:
        """Fetch NAV data for given scheme codes.

        Args:
            scheme_code (int | list[int]): A single scheme code or a list of scheme codes.
            df_format : OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
            Specifies the output DataFrame format.

            Defaults to ``"polars"``.

            Returns a pandas DataFrame if ``df_format="pandas"``.
            ``pandas`` must be installed to use this format.

        Raises:
            ValueError: If any of the provided scheme codes are not found in the data.

        Returns:
            pl.DataFrame | pd.DataFrame: Returns a Polars DataFrame when multiple scheme codes are provided.

        """
        df = await self._get_cache()
        codes = scheme_code if isinstance(scheme_code, list) else [scheme_code]
        result = df.filter(pl.col("scheme_code").is_in(codes))
        if df_format == "pandas":
            return result.to_pandas()
        return result

    async def _search_scheme_str(
        self,
        query: str,
        col_type: Literal["scheme_name", "amc", "scheme_type"],
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        df = await self._get_cache()
        if case_sensitive:
            rows = df.filter(pl.col(col_type).str.contains(query, literal=True))
        else:
            rows = df.filter(pl.col(col_type).str.contains(f"(?i){re.escape(query)}", literal=False))

        if rows.is_empty():
            return rows

        if df_format == "pandas":
            if suggestion_count is not None:
                return rows.to_pandas().head(suggestion_count)
            return rows.to_pandas()
        if suggestion_count is not None:
            return rows.head(suggestion_count)
        return rows

    async def search_scheme_by_name(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by name.

        Args:
            query (str): A string related to scheme name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
            Specifies the output DataFrame format.

            Defaults to ``"polars"``.

            Returns a pandas DataFrame if ``df_format="pandas"``.
            ``pandas`` must be installed to use this format.

        Returns:
            pl.DataFrame | None: Returns a Polars DataFrame

        """
        return await self._search_scheme_str(
            query=query,
            col_type="scheme_name",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def search_scheme_by_amc(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by AMC (Asset Management Company).

        Args:
            query (str): A string related to AMC name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
            Specifies the output DataFrame format.

            Defaults to ``"polars"``.

            Returns a pandas DataFrame if ``df_format="pandas"``.
            ``pandas`` must be installed to use this format.

        Returns:
            pl.DataFrame | None: Returns a Polars DataFrame

        """
        return await self._search_scheme_str(
            query=query,
            col_type="amc",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def search_scheme_by_type(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by Scheme Type.

        Args:
            query (str): A string related to AMC name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
            Specifies the output DataFrame format.

            Defaults to ``"polars"``.

            Returns a pandas DataFrame if ``df_format="pandas"``.
            ``pandas`` must be installed to use this format.

        Returns:
            pl.DataFrame | None: Returns a Polars DataFrame

        """
        return await self._search_scheme_str(
            query=query,
            col_type="scheme_type",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )


if __name__ == "__main__":
    import asyncio

    async def main() -> None:  # noqa: D103
        async with NAVStore(verbose=True) as store:
            data = await store.search_scheme_by_name("quant", case_sensitive=False, suggestion_count=2)
            print(data)

            data = await store.search_scheme_by_name(
                "birla", case_sensitive=False, suggestion_count=4, df_format="pandas"
            )
            print(data)

            await store.refresh_nav_cache()

            data = await store.search_scheme_by_code(scheme_code=119551)
            print(data)

            data = await store.search_scheme_by_type("open", case_sensitive=False, suggestion_count=5)
            print(data)

    asyncio.run(main())
