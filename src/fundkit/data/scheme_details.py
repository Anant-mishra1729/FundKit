"""Scheme Details."""

import asyncio
from datetime import date, timedelta
from typing import Self

import httpx
import polars as pl

from fundkit.data._base_client import BaseAMFIClient

class SchemeDetails(BaseAMFIClient):
    """Get Scheme Details from AFMI."""

    _DEFAULT_SCHEME_DATA_URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx"
    _scheme_details_df: pl.DataFrame | None = None
    _scheme_details_df_loaded_on : date | None = None
    _SCHEME_DATA_STALE_DAYS = 7

    async def __aenter__(self) -> Self:
        return self
    
    async def _fetch(self) -> str:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(SchemeDetails._DEFAULT_SCHEME_DATA_URL,params={
                    "mf" : 0
                })
                response.raise_for_status()
                return response.text
        except httpx.HTTPStatusError as e:
            raise httpx.HTTPStatusError(
                f"Scheme details fetch failed with status {e.response.status_code}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx.RequestError as e:
            raise httpx.RequestError(f"Failed to fetch scheme details: {e}") from e


    
    async def _get_scheme_cache(self) -> pl.DataFrame | None:
        # Loading from memory
        today = date.today()
        seven_days_ago = today - timedelta(days= SchemeDetails._SCHEME_DATA_STALE_DAYS)
        if SchemeDetails._scheme_details_df is not None \
            and SchemeDetails._scheme_details_df_loaded_on is not None \
            and seven_days_ago <= SchemeDetails._scheme_details_df_loaded_on <= today:
            self._log("Memory hit: returning in-memory Scheme DataFrame")
            return SchemeDetails._scheme_details_df
        
        # Loading from cache
        cache_file_path = self._cache_path / "scheme_details.parquet"
        if cache_file_path.exists():
            SchemeDetails._scheme_details_df = await asyncio.to_thread(pl.read_parquet, cache_file_path)
            SchemeDetails._scheme_details_df_loaded_on = today
            return SchemeDetails._scheme_details_df

        return None




if __name__ == "__main__":
    async def main() -> None:
        async with SchemeDetails() as client:
            await client._get_scheme_cache()
    
    asyncio.run(main())

    
            

