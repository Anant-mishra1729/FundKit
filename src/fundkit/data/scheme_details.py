"""Scheme Details client."""

from __future__ import annotations

import asyncio
import datetime as dt
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self

import httpx
import polars as pl

from fundkit.data._base_client import DEFAULT_AUTOCOMPLETE_LIMIT, BaseClient
from fundkit.exceptions import CacheCreationError, InvalidAMFIResponseError

if TYPE_CHECKING:
    import pandas as pd


class SchemeDetailsClient(BaseClient):
    """Get Scheme Details from AFMI."""

    _SCHEME_DETAILS_CACHE_FILENAME = "scheme_details.parquet"
    _SCHEME_DETAILS_URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx"
    _scheme_details_cache_ttl_days = 7

    _scheme_details_cache_df: pl.DataFrame | None = None
    _scheme_details_cache_loaded_on: date | None = None
    _scheme_details_cache_lock: asyncio.Lock = asyncio.Lock()

    def __init__(self, verbose: bool = False) -> None:
        super().__init__(verbose=verbose)
        self._http_client: httpx.AsyncClient | None = None

    @classmethod
    def _scheme_details_cache_path(cls) -> Path:
        return cls._cache_path / cls._SCHEME_DETAILS_CACHE_FILENAME

    @classmethod
    def clear_memory_cache(cls) -> None:
        """Release the in-process scheme-details cache."""
        cls._scheme_details_cache_df = None
        cls._scheme_details_cache_loaded_on = None

    async def __aenter__(self) -> Self:
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _fetch(self) -> bytes:
        try:
            if self._http_client is None:
                raise RuntimeError(
                    "HTTP client not initialized. "
                    "Use 'async with SchemeDetailsClient()' context manager."
                )
            response = await self._http_client.get(
                SchemeDetailsClient._SCHEME_DETAILS_URL, params={"mf": 0}
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise httpx.HTTPStatusError(
                f"Scheme details fetch failed with status {e.response.status_code}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx.RequestError as e:
            raise httpx.RequestError(f"Scheme details fetch failed: {e!s}", request=e.request) from e
        else:
            return response.content

    async def _fetch_and_parse(self) -> pl.DataFrame:
        raw = await self._fetch()
        return self._parse(raw)

    def _parse(self, raw_data: bytes) -> pl.DataFrame:
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
                    pl.col("minimum_amount_raw").cast(pl.String),
                    pl.col("launch_date").str.to_date("%d-%b-%Y", strict=False),
                    pl.col("closure_date").str.to_date("%d-%b-%Y", strict=False),
                )
                .with_columns(
                    pl
                    .col("minimum_amount_raw")
                    .str.replace_all(",", "")  # "1,000" → "1000"
                    .str.extract(r"(\d+(?:\.\d+)?)")  # "Rs 5000 and..." → "5000"
                    .cast(pl.Float64, strict=False)  # None if unparseable
                    .alias("minimum_amount")
                )
                .select(
                    "scheme_code",
                    "scheme_name",
                    "scheme_nav_name",
                    "scheme_type",
                    "scheme_category",
                    "amc",
                    "isin",
                    "minimum_amount",
                    "minimum_amount_raw",
                    "launch_date",
                    "closure_date",
                )
                .sort("scheme_code")
                .with_columns(pl.col("scheme_code").set_sorted())
            )
        except Exception as e:
            raise InvalidAMFIResponseError("Invalid data from AMFI") from e

    async def _get_scheme_details_cache(self) -> pl.DataFrame:
        today = date.today()
        ttl = SchemeDetailsClient._scheme_details_cache_ttl_days

        if (
            SchemeDetailsClient._scheme_details_cache_df is not None
            and SchemeDetailsClient._scheme_details_cache_loaded_on is not None
            and (today - SchemeDetailsClient._scheme_details_cache_loaded_on).days <= ttl
        ):
            self._log("Memory hit: returning in-memory scheme details cache")
            return SchemeDetailsClient._scheme_details_cache_df

        async with SchemeDetailsClient._scheme_details_cache_lock:
            if (
                SchemeDetailsClient._scheme_details_cache_df is not None
                and SchemeDetailsClient._scheme_details_cache_loaded_on is not None
                and (today - SchemeDetailsClient._scheme_details_cache_loaded_on).days <= ttl
            ):
                self._log("Memory hit (post-lock): returning in-memory scheme details cache")
                return SchemeDetailsClient._scheme_details_cache_df

            SchemeDetailsClient._scheme_details_cache_df = None
            SchemeDetailsClient._scheme_details_cache_loaded_on = None

            scheme_details_cache_path = self._scheme_details_cache_path()
            if scheme_details_cache_path.exists():
                self._log(f"Disk hit: loading scheme details cache from {scheme_details_cache_path}.")
                mtime = datetime.fromtimestamp(
                    scheme_details_cache_path.stat().st_mtime, tz=dt.UTC
                )
                age_days = (datetime.now(tz=dt.UTC) - mtime).days
                if age_days <= ttl:
                    SchemeDetailsClient._scheme_details_cache_df = await asyncio.to_thread(
                        pl.read_parquet, scheme_details_cache_path
                    )
                    SchemeDetailsClient._scheme_details_cache_loaded_on = today
                    return SchemeDetailsClient._scheme_details_cache_df

            self._log("Cache miss: fetching scheme details from AMFI.")
            SchemeDetailsClient._scheme_details_cache_df = await self._fetch_and_parse()
            SchemeDetailsClient._scheme_details_cache_loaded_on = today
            try:
                await BaseClient._write_parquet_atomic(
                    scheme_details_cache_path,
                    SchemeDetailsClient._scheme_details_cache_df,
                )
                self._log(f"Scheme details cache written to {scheme_details_cache_path}.")
            except OSError as e:
                raise CacheCreationError(
                    "Error occurred while generating scheme details cache"
                ) from e

            return SchemeDetailsClient._scheme_details_cache_df

    async def refresh_scheme_details_cache(self) -> None:
        """Force-refresh scheme metadata from AMFI.

        Raises:
            CacheCreationError: If the cache file cannot be written.

        """
        today = date.today()
        async with SchemeDetailsClient._scheme_details_cache_lock:
            self._log("Refreshing scheme details cache: fetching from AMFI.")
            SchemeDetailsClient._scheme_details_cache_df = await self._fetch_and_parse()
            SchemeDetailsClient._scheme_details_cache_loaded_on = today
            scheme_details_cache_path = self._scheme_details_cache_path()
            try:
                await BaseClient._write_parquet_atomic(
                    scheme_details_cache_path,
                    SchemeDetailsClient._scheme_details_cache_df,
                )
                self._log(f"Scheme details cache written to {scheme_details_cache_path}.")
            except OSError as e:
                raise CacheCreationError(
                    "Error occurred while generating scheme details cache"
                ) from e

    async def list_schemes(
        self,
        *,
        amc: str | None = None,
        scheme_type: str | None = None,
        scheme_category: str | None = None,
        name_query: str | None = None,
        limit: int | None = None,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Return scheme rows for dropdowns (code, names, AMC, type, category)."""
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        df = await self._get_scheme_details_cache()
        result = df

        if amc is not None:
            result = result.filter(pl.col("amc") == amc)
        if scheme_type is not None:
            result = result.filter(pl.col("scheme_type") == scheme_type)
        if scheme_category is not None:
            result = result.filter(pl.col("scheme_category") == scheme_category)
        if name_query is not None:
            q = name_query.lower()
            result = result.filter(
                pl.col("scheme_name").str.to_lowercase().str.contains(q, literal=True)
            )
        if limit is not None:
            result = result.head(limit)

        return self._export_dataframe(
            result.select(
                [
                    "scheme_code",
                    "scheme_name",
                    "scheme_nav_name",
                    "amc",
                    "scheme_type",
                    "scheme_category",
                    "minimum_amount",
                    "launch_date",
                ]
            ),
            df_format=df_format,
        )

    async def search_schemes(
        self,
        query: str,
        *,
        limit: int = DEFAULT_AUTOCOMPLETE_LIMIT,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Autocomplete search on scheme name (bounded for UIs)."""
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        df = await self._get_scheme_details_cache()
        rows = (
            df
            .filter(
                pl.col("scheme_name")
                .str.to_lowercase()
                .str.contains(query.lower(), literal=True)
            )
            .head(limit)
        )
        return self._export_dataframe(rows, df_format=df_format)

    async def list_amcs(
        self,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Distinct AMC names for a filter dropdown."""
        df = await self._get_scheme_details_cache()
        result = df.select("amc").unique().drop_nulls().sort("amc")
        return self._export_dataframe(result, df_format=df_format)

    async def list_scheme_types(
        self,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Distinct scheme types for a filter dropdown."""
        df = await self._get_scheme_details_cache()
        result = df.select("scheme_type").unique().drop_nulls().sort("scheme_type")
        return self._export_dataframe(result, df_format=df_format)

    async def list_scheme_categories(
        self,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Distinct scheme categories for a filter dropdown."""
        df = await self._get_scheme_details_cache()
        result = df.select("scheme_category").unique().drop_nulls().sort("scheme_category")
        return self._export_dataframe(result, df_format=df_format)

    async def get_scheme_details(
        self,
        scheme_codes: int | list[int],
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Get metadata for one or more schemes.

        Args:
            scheme_codes: Single AMFI scheme code or a list of scheme codes.
            df_format: Output format — "polars" (default) or "pandas".

        Returns:
            Filtered DataFrame. Empty DataFrame if no codes match.

        Raises:
            ValueError: If scheme_codes is an empty list.

        """
        if isinstance(scheme_codes, list):
            if not scheme_codes:
                raise ValueError("scheme_codes list cannot be empty.")
            codes = scheme_codes
        else:
            codes = [scheme_codes]

        df = await self._get_scheme_details_cache()
        result = df.filter(pl.col("scheme_code").is_in(codes))

        return self._export_dataframe(result, df_format=df_format)


if __name__ == "__main__":
    async def main() -> None:  # noqa: D103
        async with SchemeDetailsClient(verbose=True) as client:
            details = await client.get_scheme_details(123456, df_format="polars")
            print(details)

    asyncio.run(main())
