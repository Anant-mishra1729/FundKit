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
    """Fetch NAV Data from AFMI-India website."""

    _df: pl.DataFrame | None = None
    _df_loaded_on: date | None = None

    def __init__(self, verbose: bool = False) -> None:
        super().__init__(verbose)

    async def refresh_nav_cache(self) -> None:
        """Refresh NAV Cache.

        Raises:
            CacheCreationError: OS Error occured in NAV cache creation.

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
        """Fetch NAV data for given scheme codes.

        Arg:
            scheme_code (int | list[int]): A single scheme code or a list of scheme codes.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output format — "polars" (default) or "pandas".

        Returns:
            pl.DataFrame | pd.DataFrame: Filtered DataFrame with NAV data for the requested scheme(s).

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
            query (str): A string related to scheme name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : NAVClient.OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
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
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by AMC (Asset Management Company).

        Args:
            query (str): A string related to AMC name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : NAVClient.OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
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
        df_format: NAVClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search schemes by Scheme Type.

        Args:
            query (str): A string related to AMC name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True
            df_format : NAVClient.OUTPUT_DATAFRAME_FORMAT["polars", "pandas"], optional
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
        async with NAVClient(verbose=True) as client:
            nav = await client.search_scheme_by_code(128628)
            print(f"Scheme Code: {nav['scheme_code'].item()}")  # Scheme Code: 128628
            print(f"ISIN (Growth/Payout): {nav['isin_growth_or_payout'].item()}")  # ISIN (Growth/Payout): INF179KA1JC4
            print(f"ISIN (Div Reinvest): {nav['isin_div_reinvestment'].item()}")  # ISIN (Div Reinvest): -
            print(
                f"Scheme Name: {nav['scheme_name'].item()}"
            )  # Scheme Name: HDFC Banking and PSU Debt Fund - Growth Option
            print(f"NAV: {nav['nav'].item()}")  # NAV: 23.729
            print(f"Date: {nav['date'].item()}")  # Date: 2026-05-22
            print(f"AMC: {nav['amc'].item()}")  # AMC: HDFC Mutual Fund
            print(
                f"Scheme Type: {nav['scheme_type'].item()}"
            )  # Scheme Type: Open Ended Schemes(Debt Scheme - Banking and PSU Fund)

            # Multiple schemes
            df = await client.search_scheme_by_code([119597, 120505, 108272])
            print(df)

            # Search by name
            results = await client.search_scheme_by_name("bluechip", case_sensitive=False)

            # Search by AMC
            results = await client.search_scheme_by_amc("SBI")

            # Search by fund type
            results = await client.search_scheme_by_type("Open Ended Schemes")
            print(results)

            # Validate scheme code
            is_valid = await client.is_valid_scheme_code(119597)
            print(is_valid)

            # Force refresh cache
            await client.refresh_nav_cache()
            print()

    asyncio.run(main())
