"""Historical NAV Client: Get historical NAV data."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, ClassVar, Self, cast

import httpx
import polars as pl

from fundkit.data._base_client import BaseAMFIClient, BaseClient

if TYPE_CHECKING:
    import pandas as pd

from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, httpx.RequestError)


class HistoricalNAVClient(BaseAMFIClient):
    """Fetch historical Net Asset Value (NAV) data for mutual funds.

    Latest scheme metadata (including ``amc_id``) comes from the shared NAV cache
    on :class:`~fundkit.data._base_client.BaseAMFIClient`. Historical series are
    fetched from a separate AMFI endpoint and cached under ``historical/``.
    """

    _historical_cache_dir = BaseClient._cache_path / "historical"
    _DEFAULT_FETCH_CONCURRENCY = 5
    _DEFAULT_MAX_RETRIES = 3
    _DEFAULT_MAX_BACKOFF_SECONDS = 30.0
    _HISTORICAL_NAV_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
    _historical_cache_locks_guard: asyncio.Lock = asyncio.Lock()
    _historical_cache_locks_by_amc_id: ClassVar[dict[int, asyncio.Lock]] = {}

    @classmethod
    def _historical_cache_path(cls, amc_id: int) -> Path:
        return cls._historical_cache_dir / f"amc_{amc_id}.parquet"

    @classmethod
    def clear_historical_cache_locks(cls) -> None:
        """Release per-AMC asyncio locks (in-process only)."""
        cls._historical_cache_locks_by_amc_id.clear()

    @classmethod
    async def _historical_cache_lock_for_amc(cls, amc_id: int) -> asyncio.Lock:
        """Per-AMC lock: serialises read-modify-write on that AMC's parquet file."""
        async with cls._historical_cache_locks_guard:
            lock = cls._historical_cache_locks_by_amc_id.get(amc_id)
            if lock is None:
                lock = asyncio.Lock()
                cls._historical_cache_locks_by_amc_id[amc_id] = lock
            return lock

    def __init__(
        self,
        verbose: bool = False,
        fetch_concurrency: int = _DEFAULT_FETCH_CONCURRENCY,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        max_backoff_seconds: float = _DEFAULT_MAX_BACKOFF_SECONDS,
    ) -> None:
        super().__init__(verbose)
        self._fetch_concurrency = fetch_concurrency
        self._max_retries = max_retries
        self._max_backoff_seconds = max_backoff_seconds
        self._fetch_semaphore: asyncio.Semaphore | None = None
        self._http_client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        self._fetch_semaphore = asyncio.Semaphore(self._fetch_concurrency)
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
        self._fetch_semaphore = None
        await super().__aexit__(exc_type, exc_val, exc_tb)

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
            raise ValueError(f"Start Date {start_date} must be before End Date {end_date}")

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

        results = await asyncio.gather(
            *[self._fetch_chunk_with_retry( amc_id, s, e) for s, e in chunks], return_exceptions=True
        )
        errors = [r for r in results if isinstance(r, BaseException)]
        raw_chunks = [r for r in results if isinstance(r, str)]

        if errors:
            self._log(f"{len(errors)}/{len(chunks)} chunk(s) failed for AMC ID {amc_id}")
        if not raw_chunks:
            raise errors[0]
        return self._parse(raw_chunks)

    async def _fetch_chunk_with_retry(
        self,
        amc_id: int,
        start: date,
        end: date,
    ) -> str:
        """Fetch one chunk - semaphore-limited, tenacity-retried."""
        assert self._fetch_semaphore is not None, (
            "Fetch semaphore not initialised - use 'async with HistoricalNAVClient() as client:'"
        )

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(
                initial=1,
                max=self._max_backoff_seconds,
                jitter=0.5,
            ),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                async with self._fetch_semaphore:
                    assert self._http_client is not None
                    response = await self._http_client.get(
                        HistoricalNAVClient._HISTORICAL_NAV_URL,
                        params={
                            "mf": amc_id,
                            "frmdt": start.strftime("%d-%b-%Y"),
                            "todt": end.strftime("%d-%b-%Y"),
                            "tp": 1,
                        },
                    )
                    response.raise_for_status()
                    return response.text

        raise AssertionError("Unreachable")

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
            raise ValueError(f"Insufficient data after parsing chunks - got {len(rows)} row(s).")

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
        historical_cache_path = self._historical_cache_path(amc_id)
        self._log(f"Reading cache {historical_cache_path}")
        if not historical_cache_path.exists():
            self._log(f"Cache miss: no cache found at {historical_cache_path}")
            return None

        df = await asyncio.to_thread(pl.read_parquet, historical_cache_path)

        if df.is_empty():
            return None

        scheme_df = df.filter(pl.col("scheme_code") == scheme_code)

        if scheme_df.is_empty():
            return None

        min_cached = cast(date, scheme_df["date"].min())
        max_cached = cast(date, scheme_df["date"].max())

        # Allow up to 2 days tolerance for weekends/holidays on range boundaries.
        if start_date < min_cached - timedelta(days=2):
            return None
        if end_date > max_cached + timedelta(days=2):
            return None
        return scheme_df.filter(pl.col("date").is_between(start_date, end_date))

    async def _write_historical_cache(self, amc_id: int, df: pl.DataFrame) -> None:
        historical_cache_path = self._historical_cache_path(amc_id)
        try:
            if historical_cache_path.exists():
                existing = await asyncio.to_thread(pl.read_parquet, historical_cache_path)
                df = pl.concat([existing, df]).unique(subset=["scheme_code", "date"]).sort("date")
            await BaseClient._write_parquet_atomic(historical_cache_path, df)
            self._log(f"AMC {amc_id} cache written to {historical_cache_path}.")
        except OSError as e:
            self._log(f"Warning: could not write AMC cache: {e}")

    # ------------------- Historical search ------------------
    async def get_history(
        self,
        scheme_code: int,
        start_date: date,
        end_date: date | None = None,
        df_format: BaseClient.OUTPUT_DATAFRAME_FORMAT = "polars",
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
            end_date = date.today()

        if start_date > end_date:
            raise ValueError(f"Start Date {start_date} must be before End Date {end_date}")

        amc_id = await self._search_amc_id(scheme_code)
        if amc_id is None:
            self._log(f"Scheme code {scheme_code} not found or has no fund house mapping. Returning empty DataFrame.")
            empty = pl.DataFrame(
                schema={
                    "scheme_code": pl.Int64,
                    "scheme_name": pl.String,
                    "isin_growth_or_payout": pl.String,
                    "isin_div_reinvestment": pl.String,
                    "nav": pl.Float64,
                    "repurchase_price": pl.Float64,
                    "sale_price": pl.Float64,
                    "date": pl.Date,
                }
            )
            return self._export_dataframe(empty, df_format)

        amc_cache_lock = await self._historical_cache_lock_for_amc(amc_id)
        async with amc_cache_lock:
            cached_df = await self._get_historical_cache(
                amc_id=amc_id, scheme_code=scheme_code, start_date=start_date, end_date=end_date
            )

            if cached_df is not None and not cached_df.is_empty():
                self._log(f"Cache hit for scheme: {scheme_code}")
                return self._export_dataframe(cached_df, df_format)

            df = await self._fetch(amc_id=amc_id, start=start_date, end=end_date)
            await self._write_historical_cache(amc_id=amc_id, df=df)

            scheme_df = df.filter(
                (pl.col("scheme_code") == scheme_code)
                & pl.col("date").is_between(start_date, end_date)
            )
            if scheme_df.is_empty():
                self._log(
                    f"Scheme code {scheme_code} not found or has no fund house mapping. "
                    "Returning empty DataFrame."
                )
                empty = pl.DataFrame(schema=df.schema)
                return self._export_dataframe(empty, df_format)

            return self._export_dataframe(scheme_df, df_format)
