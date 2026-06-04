"""NAV Client: Get latest NAV data."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from fundkit.data._base_client import BaseAMFIClient

if TYPE_CHECKING:
    import pandas as pd


class NAVClient(BaseAMFIClient):
    """Fetch the latest Net Asset Value (NAV) data for mutual funds."""

    async def get_nav(
        self,
        scheme_code: int | list[int],
        limit: int | None = None,
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
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
            limit=limit,
            df_format=df_format,
        )

    async def get_nav_by_name(
        self,
        query: str,
        limit: int | None = None,
        case_sensitive: bool = True,
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by name.

        Args:
            query (str): A search string related to the scheme name.
            limit (int): The maximum number of suggestions to return.
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
            limit=limit,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def get_nav_by_amc(
        self,
        query: str,
        limit: int | None = None,
        case_sensitive: bool = True,
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by AMC (Asset Management Company) name.

        Args:
            query (str): A search string related to the AMC name.
            limit (int): The maximum number of suggestions to return.
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
            limit=limit,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )

    async def get_nav_by_type(
        self,
        query: str,
        limit: int | None = None,
        case_sensitive: bool = True,
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by scheme type (Open Ended, Close ended etc).

        Args:
            query (str): A search string related to the scheme type.
            limit (int): The maximum number of suggestions to return.
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
            limit=limit,
            case_sensitive=case_sensitive,
            df_format=df_format,
        )
