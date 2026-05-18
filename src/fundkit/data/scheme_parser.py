from __future__ import annotations  # noqa: D100

import asyncio
import logging
from types import TracebackType
from typing import Self

import httpx
import polars as pl

logging.getLogger("httpx").setLevel(logging.WARNING)


class SchemeParser:
    """Async HTTP client for fetching NAV data from the AMFI portal."""

    _NAV_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"

    _SCHEME_TYPE_PREFIXES = (
        "Open Ended Schemes",
        "Close Ended Schemes",
        "Interval Fund",
    )

    async def __aenter__(self) -> Self:
        self._client = httpx.AsyncClient(timeout=10.0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._client.aclose()

    async def fetch_nav_data(self) -> pl.DataFrame:
        """Fetch NAV data from AFMI-India website and return a DataFrame.

        Raises:
            httpx.HTTPStatusError: On incorrect HTTP response.
            httpx.RequestError: On network / connection failure.
            ValueError: If the AMFI response is empty or malformed.
            ModuleNotFoundError: If the df_format="pandas" is used and pandas is not installed.

        Returns:
            pl.DataFrame: A typed DataFrame with NAV Data.

        """
        raw_text = await self._fetch()
        return self._parse(raw_text)

    async def _fetch(self) -> str:
        try:
            response = await self._client.get(self._NAV_URL)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise httpx.HTTPStatusError(
                f"AMFI responded with {e.response.status_code}",
                request=e.request,
                response=e.response,
            ) from e
        except httpx.RequestError as e:
            raise httpx.RequestError(f"Failed to reach AMFI: {e}") from e
        else:
            return response.text

    def _parse(self, nav_text: str) -> pl.DataFrame:
        rows = [line.strip() for line in nav_text.splitlines() if line.strip()]

        if len(rows) < 2:
            raise ValueError(f"Incorrect AFMI response: expected at least 2 rows, but received {len(rows)}.")
        headers = [*rows[0].split(";"), "AMC", "Scheme Type"]
        lines = rows[1:]

        amc: str | None = None
        scheme_type: str | None = None
        data: list[list[str | None]] = []
        # Parsing rows
        for line in lines:
            if ";" in line:
                data.append([*line.split(";"), amc, scheme_type])
            else:
                amc = None
                if line.startswith(self._SCHEME_TYPE_PREFIXES):
                    scheme_type = line
                else:
                    amc = line

        return (
            pl.DataFrame(data=data, schema=headers, orient="row")
            .with_columns(
                pl.col("Scheme Code").cast(pl.Int64),
                pl.col("ISIN Div Payout/ ISIN Growth").cast(pl.String),
                pl.col("ISIN Div Reinvestment").cast(pl.String),
                pl.col("Scheme Name").cast(pl.String),
                pl.col("Net Asset Value").cast(pl.Float64),
                pl.col("Date").str.to_date(format="%d-%b-%Y"),
                pl.col("AMC").cast(pl.String),
                pl.col("Scheme Type").cast(pl.String),
            )
            .rename(
                {
                    "Scheme Code": "scheme_code",
                    "ISIN Div Payout/ ISIN Growth": "isin_growth_or_payout",
                    "ISIN Div Reinvestment": "isin_div_reinvestment",
                    "Scheme Name": "scheme_name",
                    "Net Asset Value": "nav",
                    "Date": "date",
                    "AMC": "amc",
                    "Scheme Type": "scheme_type",
                }
            )
        )


if __name__ == "__main__":

    async def main() -> None:
        """Test function."""
        async with SchemeParser() as client:
            data = await client.fetch_nav_data()
            print(data)

    asyncio.run(main())
