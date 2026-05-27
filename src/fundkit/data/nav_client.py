from __future__ import annotations  # noqa: D100

import asyncio
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from fundkit.data._base_client import BaseAMFIClient
from fundkit.data.scheme_parser import SchemeParser
from fundkit.exceptions import CacheCreationError

if TYPE_CHECKING:
    import pandas as pd


class NAVClient(BaseAMFIClient):
    """Fetch the latest Net Asset Value (NAV) data for mutual funds."""

    _df: pl.DataFrame | None = None
    _df_loaded_on: date | None = None

    def __init__(self, verbose: bool = False) -> None:
        super().__init__(verbose)

    async def refresh_nav_cache(self) -> None:
        """Refresh the NAV cache.

        Raises:
            CacheCreationError: Raised when an OS error occurs during NAV cache creation.

        """
        today = date.today()
        self._log("Refreshing cache: fetching NAV data from AMFI.")
        async with SchemeParser() as parser:
            NAVClient._df = await parser.fetch_nav_data()
            NAVClient._df_loaded_on = today
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            cache_file_path = self._cache_path / "nav.parquet"
            await asyncio.to_thread(NAVClient._df.write_parquet, cache_file_path)
            self._log(f"NAV cache written to {cache_file_path}.")

        except OSError as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e

    async def get_nav(
        self,
        scheme_code: int | list[int],
        suggestion_count: int | None = None,
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search NAV data using scheme codes.

        Arg:
            scheme_code (int | list[int]): A single scheme code or a list of scheme codes.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output DataFrame format.
            Supported values are "polars" (default) and "pandas".

        Returns:
            pl.DataFrame | pd.DataFrame: A filtered DataFrame containing NAV data for the requested scheme code(s).

        """
        return await self._search_scheme_code(
            scheme_code=scheme_code,
            suggestion_count=suggestion_count,
            df_format=df_format,
        )

    async def get_nav_by_name(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by name.

        Args:
            query (str): A search string related to the scheme name.
            suggestion_count (int): The maximum number of suggestions to return.
            case_sensitive (bool): Whether to perform a case-sensitive search.
                                   Enabling case sensitivity may improve search performance.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output DataFrame format.
                                   Supported values are "polars" (default) and "pandas".

        Returns:
            pl.DataFrame | pd.DataFrame | None: A DataFrame containing matching scheme results,
            or None if no matches are found.

        """
        return await self._search_scheme_str(
            query=query,
            col_type="scheme_name",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def get_nav_by_amc(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by AMC (Asset Management Company) name.

        Args:
            query (str): A search string related to the AMC name.
            suggestion_count (int): The maximum number of suggestions to return.
            case_sensitive (bool): Whether to perform a case-sensitive search.
                                   Enabling case sensitivity may improve search performance.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output DataFrame format.
                                   Supported values are "polars" (default) and "pandas".

        Returns:
           pl.DataFrame | pd.DataFrame | None: A DataFrame containing matching scheme results,
           or None if no matches are found.

        """
        return await self._search_scheme_str(
            query=query,
            col_type="amc",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def get_nav_by_type(
        self,
        query: str,
        suggestion_count: int | None = None,
        case_sensitive: bool = True,
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by scheme type (Open Ended, Close ended etc).

        Args:
            query (str): A search string related to the scheme type.
            suggestion_count (int): The maximum number of suggestions to return.
            case_sensitive (bool): Whether to perform a case-sensitive search.
                                   Enabling case sensitivity may improve search performance.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output DataFrame format.
                                   Supported values are "polars" (default) and "pandas".

        Returns:
            pl.DataFrame | pd.DataFrame | None: A DataFrame containing matching scheme results,
            or None if no matches are found.

        """
        return await self._search_scheme_str(
            query=query,
            col_type="scheme_type",
            suggestion_count=suggestion_count,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )
