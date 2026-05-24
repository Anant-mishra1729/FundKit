"""Historical NAV Data."""

from __future__ import annotations

import asyncio
import random
from datetime import date, timedelta
from typing import TYPE_CHECKING, Self, cast

import httpx
import polars as pl

from fundkit.data._base_client import BaseAMFIClient

if TYPE_CHECKING:
    import pandas as pd


class HistoricalNAVClient(BaseAMFIClient):
    """Fetch historical Net Asset Value (NAV) data for mutual funds."""

    _cache_path = BaseAMFIClient._cache_path / "historical"
    _semaphore: asyncio.Semaphore | None = None
    _NAV_CACHE = BaseAMFIClient._cache_path / "nav.parquet"
    _DEFAULT_END_DATE: date | None = None
    _DEFAULT_AMFI_CONCURRENCY = 5
    _DEFAULT_MAX_RETRIES = 3
    _DEFAULT_BACKOFF = 1.0
    _DEFAULT_HISTORICAL_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

    async def __aenter__(self) -> Self:
        HistoricalNAVClient._semaphore = asyncio.Semaphore(self.max_concurrency)
        return self

    def __init__(
        self,
        verbose: bool = False,
        max_concurrency: int = _DEFAULT_AMFI_CONCURRENCY,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        max_backoff_limit: float = _DEFAULT_BACKOFF,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.max_retries = max_retries
        self.max_backoff_limit = max_backoff_limit
        super().__init__(verbose)

    # ------------------- Fetch and parse functions -----------------
    @staticmethod
    def _date_chunks(start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Split a date range into 89-day chunks.

        Args:
            start_date (date): The start date of the range.
            end_date (date): The start date of the range.

        Returns:
            list[tuple[date,date]]: A list of (start_date, end_date) tuples,
            where each tuple represents a continuous date-range chunk of
            at most 89 days.

        """
        if start_date > end_date:
            raise ValueError("Start Date {start} must be before End Date {end}")

        chunks = []
        current_start = start_date
        while current_start <= end_date:
            current_end = min(current_start + timedelta(days=89), end_date)
            chunks.append((current_start, current_end))
            current_start += timedelta(days=90)

        return chunks

    async def _fetch(self, amc_id: int, start: date, end: date) -> pl.DataFrame:
        """Fetch all chunks concurrently and return parsed DataFrame."""
        chunks = self._date_chunks(start, end)
        self._log(f"Fetching {len(chunks)} chunk(s) for AMC ID: {amc_id}.")

        async with httpx.AsyncClient(timeout=30.0) as client:
            raw_chunks = await asyncio.gather(*[self._fetch_chunk_with_retry(client, amc_id, s, e) for s, e in chunks])

        return self._parse(raw_chunks)

    async def _fetch_chunk_with_retry(
        self,
        client: httpx.AsyncClient,
        amc_id: int,
        start: date,
        end: date,
    ) -> str:
        """Fetch one chunk with semaphore + exponential backoff."""
        assert HistoricalNAVClient._semaphore is not None

        for attempt in range(self.max_retries):
            async with HistoricalNAVClient._semaphore:
                try:
                    response = await client.get(
                        HistoricalNAVClient._DEFAULT_HISTORICAL_URL,
                        params={
                            "mf": amc_id,
                            "frmdt": start.strftime("%d-%b-%Y"),
                            "todt": end.strftime("%d-%b-%Y"),
                            "tp": 1,
                        },
                    )
                    response.raise_for_status()

                except httpx.HTTPStatusError as e:
                    if e.response.status_code < 429 and e.response.status_code != 500:
                        raise  # 4xx is not rate limit - don't retry
                except httpx.RequestError:
                    pass  # Network failure - retry
                else:
                    return response.text

            if attempt < self.max_retries - 1:
                backoff = self.max_backoff_limit * (2**attempt) + random.uniform(0, 0.5)
                self._log(f"Chunk {start}→{end} attempt {attempt + 1} failed. Retrying in {backoff:.1f}s.")
                await asyncio.sleep(backoff)

        raise httpx.RequestError(f"Chunk {start} to {end} failed after {self.max_retries} attempts.")

    def _parse(self, raw_chunks: list[str]) -> pl.DataFrame:
        """Parse the historical text response from AMFI."""
        if not raw_chunks:
            raise ValueError("No data chunks to parse.")

        first, *rest = raw_chunks
        lines = first.splitlines()
        for chunk in rest:
            chunk_lines = chunk.splitlines()
            lines.extend(chunk_lines[1:])  # Skipping header line of each subsequent chunk

        rows = [line.strip() for line in lines if line.strip()]

        if len(rows) < 2:
            raise ValueError(f"Insufficient data after parsing chunks — got {len(rows)} row(s).")

        headers = rows[0].split(";")
        data = [line.split(";") for line in rows[1:] if ";" in line]

        return (
            pl
            .DataFrame(data=data, schema=headers, orient="row")
            .with_columns(
                pl.col("Scheme Code").cast(pl.Int64),
                pl.col("Scheme Name").cast(pl.String),
                pl.col("ISIN Div Payout/ISIN Growth").cast(pl.String),
                pl.col("ISIN Div Reinvestment").cast(pl.String),
                pl.col("Net Asset Value").str.replace(r"^-$", "").cast(pl.Float64, strict=False),
                pl.col("Repurchase Price").str.replace(r"^-$", "").cast(pl.Float64, strict=False),
                pl.col("Sale Price").str.replace(r"^-$", "").cast(pl.Float64, strict=False),
                pl.col("Date").str.to_date(format="%d-%b-%Y"),
            )
            .rename({
                "Scheme Code": "scheme_code",
                "Scheme Name": "scheme_name",
                "ISIN Div Payout/ISIN Growth": "isin_growth_or_payout",
                "ISIN Div Reinvestment": "isin_div_reinvestment",
                "Net Asset Value": "nav",
                "Repurchase Price": "repurchase_price",
                "Sale Price": "sale_price",
                "Date": "date",
            })
            .sort("date")
            .with_columns(pl.col("date").set_sorted())
        )

    # ------------------- Caching functions -----------------
    async def _get_historical_cache(
        self, amc_id: int, scheme_code: int, start_date: date, end_date: date
    ) -> pl.DataFrame | None:
        """Get cached data from disk."""
        cache_path = HistoricalNAVClient._cache_path / f"amc_{amc_id}.parquet"
        self._log(f"Reading cache {cache_path}")
        if not cache_path.exists():
            self._log(f"Cache miss: no cache found at {cache_path}")
            return None

        df = await asyncio.to_thread(pl.read_parquet, cache_path)

        if df.is_empty():
            return None

        scheme_df = df.filter(pl.col("scheme_code") == scheme_code)

        if scheme_df.is_empty():
            return None

        min_cached = cast(date, scheme_df["date"].min())

        # Allow up to 2 days gap on start - covers weekends/holidays
        # Eg: user requests 2023-01-01 (Sunday), AMFI has 2023-01-02 (Monday)
        if start_date < min_cached - timedelta(days=2):  # Compairing with tolerance of 2 days (Weekends)
            return None
        return scheme_df.filter(pl.col("date").is_between(start_date, end_date))

    async def _write_historical_cache(self, amc_id: int, df: pl.DataFrame) -> None:
        cache_path = HistoricalNAVClient._cache_path / f"amc_{amc_id}.parquet"
        try:
            if cache_path.exists():
                existing = await asyncio.to_thread(pl.read_parquet, cache_path)
                df = pl.concat([existing, df]).unique(subset=["scheme_code", "date"]).sort("date")
            self._cache_path.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(df.write_parquet, cache_path)
            self._log(f"AMC {amc_id} cache written to {cache_path}.")
        except OSError as e:
            self._log(f"Warning: could not write AMC cache: {e}")

    # ------------------- Historical search ------------------
    async def search_history(
        self,
        scheme_code: int,
        start_date: date,
        end_date: date | None = None,
        df_format: BaseAMFIClient.OUTPUT_DATAFRAME_FORMAT = "polars",
    ) -> pl.DataFrame | pd.DataFrame:
        """Search historical NAV data for a given scheme code.

        Args:
            scheme_code (int): The mutual fund scheme code.
            start_date (date): The start date for the NAV history range.
            end_date (date | None, optional): The end date for the NAV history range. Defaults to today.
            df_format (OUTPUT_DATAFRAME_FORMAT, optional): Output DataFrame format.
                Supported values are "polars" (default) and "pandas".

        Raises:
            ValueError: If start_date is later than end_date.

        Returns:
            pl.DataFrame | pd.DataFrame: A DataFrame containing historical NAV data
            for the specified date range.

        """
        if end_date is None:
            end_date = HistoricalNAVClient._DEFAULT_END_DATE or date.today()

        if start_date > end_date:
            raise ValueError("Start Date {start} must be before End Date {end}")

        amc_id = await self._search_amc_id(scheme_code=scheme_code)
        if amc_id is None:
            self._log(f"Scheme code {scheme_code} not found or has no fund house mapping. Returning empty DataFrame.")
            return pl.DataFrame() if df_format == "polars" else pd.DataFrame()

        # Search cache
        cached_df = await self._get_historical_cache(
            amc_id=amc_id, scheme_code=scheme_code, start_date=start_date, end_date=end_date
        )

        if cached_df is not None and not cached_df.is_empty():
            self._log(f"Cache hit for scheme: {scheme_code}")
            return cached_df if df_format == "polars" else cached_df.to_pandas()

        # Fetch if not found in cache
        df = await self._fetch(amc_id=amc_id, start=start_date, end=end_date)

        await self._write_historical_cache(amc_id=amc_id, df=df)

        scheme_df = df.filter((pl.col("scheme_code") == scheme_code) & pl.col("date").is_between(start_date, end_date))
        if scheme_df.is_empty():
            self._log(f"Scheme code {scheme_code} not found or has no fund house mapping. Returning empty DataFrame.")
            empty = pl.DataFrame(schema=df.schema)
            return empty if df_format == "polars" else empty.to_pandas()

        return scheme_df if df_format == "polars" else scheme_df.to_pandas()


if __name__ == "__main__":

    async def main() -> None:  # noqa: D103
        async with HistoricalNAVClient(verbose=True) as client:
            data = await client.search_history(124182, start_date=date(2026, 1, 1), end_date=date.today())
            print(data)

        async with HistoricalNAVClient(verbose=True) as client:
            data = await client.search_history(124182, start_date=date(2025, 1, 1), end_date=date(2026, 3, 31))
            print(data)

        async with HistoricalNAVClient(verbose=True) as client:
            data = await client.search_history(124182, start_date=date(2025, 1, 1), df_format="pandas")
            print(data)

    asyncio.run(main())
