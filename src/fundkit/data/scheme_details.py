"""Scheme Details."""

from __future__ import annotations

import asyncio
from datetime import date
from io import BytesIO
from typing import TYPE_CHECKING, Self

import httpx
import polars as pl

from fundkit.data._base_client import BaseAMFIClient
from fundkit.exceptions import CacheCreationError, InvalidAMFIResponseError
from fundkit.schema.scheme_details import SchemeDetails

if TYPE_CHECKING:
    import pandas as pd


class SchemeDetailsClient(BaseAMFIClient):
    """Get Scheme Details from AFMI."""

    _DEFAULT_SCHEME_DATA_URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx"
    _scheme_details_df: pl.DataFrame | None = None
    _scheme_details_df_loaded_on: date | None = None
    _SCHEME_DATA_TTL_DAYS = 7
    _scheme_details_map: dict[int, SchemeDetails] | None = None

    async def __aenter__(self) -> Self:
        return self

    async def _fetch(self) -> bytes:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(SchemeDetailsClient._DEFAULT_SCHEME_DATA_URL, params={"mf": 0})
                response.raise_for_status()
                return response.content
        except httpx.HTTPStatusError as e:
            raise httpx.HTTPStatusError(
                f"Scheme details fetch failed with status {e.response.status_code}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx.RequestError as e:
            raise httpx.RequestError(f"Failed to fetch scheme details: {e}") from e

    async def _fetch_and_parse(self) -> pl.DataFrame:
        """Fetch, parse, and enrich with amc_id via scheme_code."""
        raw = await self._fetch()

        # Ensure _scheme_code_to_amc_id is populated — loads NAV cache if needed
        await self._get_nav_cache()
        assert BaseAMFIClient._scheme_code_to_amc_id is not None

        return self._parse(raw, BaseAMFIClient._scheme_code_to_amc_id)

    def _parse(self, raw_data: bytes, mf_map: dict[int, int]) -> pl.DataFrame:
        try:
            df = pl.read_csv(BytesIO(raw_data))
            df.columns = [col.strip() for col in df.columns]
            return (
                df
                .rename({
                    "AMC": "amc",
                    "Code": "scheme_code",
                    "Scheme Name": "scheme_name",
                    "Scheme Type": "scheme_type",
                    "Scheme Category": "scheme_category",
                    "Scheme NAV Name": "scheme_nav_name",
                    "Scheme Minimum Amount": "minimum_amount_raw",
                    "Launch Date": "launch_date",
                    "Closure Date": "closure_date",
                    "ISIN Div Payout/ ISIN GrowthISIN Div Reinvestment": "isin",
                })
                .with_columns(
                    pl.col("scheme_code").cast(pl.Int64),
                    pl.col("scheme_name").cast(pl.String),
                    pl.col("scheme_nav_name").cast(pl.String),
                    pl.col("scheme_type").cast(pl.Categorical),
                    pl.col("scheme_category").cast(pl.Categorical),
                    pl.col("amc").cast(pl.String),
                    pl.col("isin").cast(pl.String),
                    pl.col("minimum_amount").cast(pl.String),
                    pl.col("launch_date").str.to_date("%d-%b-%Y"),
                    pl.col("closure_date").str.to_date("%d-%b-%Y"),
                )
                .with_columns(
                    pl
                    .col("scheme_code")
                    .cast(pl.String)
                    .replace_strict(mf_map, default=None)
                    .alias("amc_id")
                    .cast(pl.Int32),
                )
                .select(
                    "scheme_code",
                    "scheme_name",
                    "scheme_nav_name",
                    "scheme_type",
                    "scheme_category",
                    "amc",
                    "amc_id",
                    "isin",
                    "minimum_amount",
                    "launch_date",
                    "closure_date",
                )
                .sort("scheme_code")
                .with_columns(pl.col("scheme_code").set_sorted())
            )
        except Exception as e:
            raise InvalidAMFIResponseError("Inavlid data from AMFI") from e

    async def _get_scheme_cache(self) -> pl.DataFrame:
        # Loading from memory
        today = date.today()
        if (
            SchemeDetailsClient._scheme_details_df is not None
            and SchemeDetailsClient._scheme_details_df_loaded_on is not None
            and (today - SchemeDetailsClient._scheme_details_df_loaded_on).days
            <= SchemeDetailsClient._SCHEME_DATA_TTL_DAYS
        ):
            self._log("Memory hit: returning in-memory Scheme DataFrame")
            if SchemeDetailsClient._scheme_details_map is None:
                SchemeDetailsClient._scheme_details_map = {
                    row["scheme_code"]: SchemeDetails.model_validate(row)
                    for row in SchemeDetailsClient._scheme_details_df.iter_rows(named=True)
                }
            return SchemeDetailsClient._scheme_details_df

        # Loading from cache
        SchemeDetailsClient._scheme_details_df = None
        SchemeDetailsClient._scheme_details_df_loaded_on = None
        SchemeDetailsClient._scheme_details_map = None
        cache_file_path = self._cache_path / "scheme_details.parquet"
        if cache_file_path.exists():
            self._log(f"Disk hit: loading scheme cache from {cache_file_path}.")
            age = (today - date.fromtimestamp(cache_file_path.stat().st_mtime)).days
            if age <= self._SCHEME_DATA_TTL_DAYS:
                SchemeDetailsClient._scheme_details_df = await asyncio.to_thread(pl.read_parquet, cache_file_path)
                SchemeDetailsClient._scheme_details_df_loaded_on = today
                SchemeDetailsClient._scheme_details_map = {
                    row["scheme_code"]: SchemeDetails.model_validate(row)
                    for row in SchemeDetailsClient._scheme_details_df.iter_rows(named=True)
                }
                return SchemeDetailsClient._scheme_details_df

        # Fetch from AMFI
        self._log("Cache miss: fetching NAV data from AMFI.")
        cache_file_path = self._cache_path / "scheme_details.parquet"
        SchemeDetailsClient._scheme_details_df = await self._fetch_and_parse()
        SchemeDetailsClient._scheme_details_df_loaded_on = today
        SchemeDetailsClient._scheme_details_map = {
            row["scheme_code"]: SchemeDetails.model_validate(row)
            for row in SchemeDetailsClient._scheme_details_df.iter_rows(named=True)
        }
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(SchemeDetailsClient._scheme_details_df.write_parquet, cache_file_path)
            self._log(f"NAV cache written to {cache_file_path}.")

        except OSError as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e

        return SchemeDetailsClient._scheme_details_df

    async def get_scheme_details(
        self,
        scheme_code: int,
    ) -> SchemeDetails | None:
        """Get metadata for a single scheme.

        Args:
            scheme_code: AMFI scheme code.

        Returns:
            SchemeDetails if found, None if not in AMFI data.

        """
        if SchemeDetailsClient._scheme_details_map is None:
            await self._get_scheme_cache()

        assert SchemeDetailsClient._scheme_details_map is not None
        return SchemeDetailsClient._scheme_details_map.get(scheme_code)

    async def get_scheme_details_bulk(
        self,
        scheme_codes: list[int],
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Get metadata for multiple schemes.

        Args:
            scheme_codes: List of AMFI scheme codes.
            df_format: Output format — "polars" (default) or "pandas".

        Returns:
            Filtered DataFrame. Empty DataFrame if no codes found.

        """
        if not scheme_codes:
            raise ValueError("scheme_codes list cannot be empty.")

        df = await self._get_scheme_cache()
        result = df.filter(pl.col("scheme_code").is_in(scheme_codes))

        return result.to_pandas() if df_format == "pandas" else result
