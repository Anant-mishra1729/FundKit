from __future__ import annotations  # noqa: D100

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

    async def search_scheme_by_code(
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

    async def search_scheme_by_name(
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

    async def search_scheme_by_amc(
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

    async def search_scheme_by_type(
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


if __name__ == "__main__":
    import asyncio

    async def main() -> None:  # noqa: D103
        async with NAVClient(verbose=True) as client:
            # ----------- Fetch NAV data for a single scheme ----------
            nav = await client.search_scheme_by_code(128628)
            print("Single Scheme NAV Data")
            print(f"Scheme Code           : {nav['scheme_code'].item()}")
            print(f"ISIN (Growth/Payout)  : {nav['isin_growth_or_payout'].item()}")
            print(f"ISIN (Div Reinvest)   : {nav['isin_div_reinvestment'].item()}")
            print(f"Scheme Name           : {nav['scheme_name'].item()}")
            print(f"NAV                   : {nav['nav'].item()}")
            print(f"Date                  : {nav['date'].item()}")
            print(f"AMC                   : {nav['amc'].item()}")
            print(f"Scheme Type           : {nav['scheme_type'].item()}")
            print()

            # ------------ Fetch NAV data for multiple schemes ---------
            df = await client.search_scheme_by_code([119597, 120505, 108272])
            print(df)

            # ------------ Search scheme by name ----------------------
            results = await client.search_scheme_by_name("bluechip", case_sensitive=False)

            # ------------ Search scheme by AMC -----------------------
            results = await client.search_scheme_by_amc("SBI")

            # ------------ Search scheme by Fund type -----------------
            results = await client.search_scheme_by_type("Open Ended Schemes")
            print(results)

            # ------------ Validate scheme code ---------------
            is_valid = await client.is_valid_scheme_code(119597)
            print(is_valid)

            # ------------ Force refresh the disk-cache ----------------
            await client.refresh_nav_cache()

    asyncio.run(main())
