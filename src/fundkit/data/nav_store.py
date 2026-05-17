import re  # noqa: D100
from datetime import date
from pathlib import Path
from types import TracebackType
from typing import Self, overload

import polars as pl
from platformdirs import user_cache_dir

from fundkit.data.scheme_parser import SchemeParser
from fundkit.schema.nav_scheme import NavScheme


class CacheCreationError(OSError):
    """Error occured in NAV cache creation."""

    pass


class NAVStore:
    """Fetch NAV Data from AFMI-India website."""

    _cache_path = Path(user_cache_dir("fundkit"))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def _get_cache(self) -> pl.DataFrame:
        cache_file_path = self._cache_path / "nav.parquet"
        if cache_file_path.exists():
            creation_time = date.fromtimestamp(cache_file_path.stat().st_mtime)
            if creation_time == date.today():
                # print(f"Cache hit: Fetching data from cache {cache_file_path} - Cache creation date {creation_time}")
                return pl.read_parquet(cache_file_path)
        async with SchemeParser() as parser:
            df = await parser.fetch_nav_data()
        try:
            self._cache_path.mkdir(parents=True, exist_ok=True)
            df.write_parquet(cache_file_path)
        except Exception as e:
            raise CacheCreationError("Error occured while generating NAV Cache") from e
        return df

    @overload
    async def get_nav(self, scheme_code: int) -> NavScheme: ...
    @overload
    async def get_nav(self, scheme_code: list[int]) -> pl.DataFrame: ...

    async def get_nav(self, scheme_code: int | list[int]) -> NavScheme | pl.DataFrame:
        """Fetch NAV data for given scheme codes.

        Args:
            scheme_code (int | list[int]): A single scheme code or a list of scheme codes.

        Raises:
            ValueError: If any of the provided scheme codes are not found in the data.

        Returns:
            NavScheme | pl.DataFrame: Returns either a NavScheme (Pydantic model) for a single scheme code,
            or a Polars DataFrame when multiple scheme codes are provided.

        """
        df = await self._get_cache()
        if isinstance(scheme_code, list):
            rows = df.filter(pl.col("scheme_code").is_in(scheme_code))
            if rows.is_empty():
                raise ValueError(f"Scheme codes {scheme_code} not found in NAV data.")
            return rows
        rows = df.filter(pl.col("scheme_code") == scheme_code)
        if rows.is_empty():
            raise ValueError(f"Scheme code {scheme_code} not found in NAV data.")
        return NavScheme(**rows.row(0, named=True))

    async def search_schemes(self, query: str, suggestion_count: int = 20, case_sensitive: bool = True) -> pl.DataFrame:
        """Search schemes by name.

        Args:
            query (str): A string related to scheme name
            suggestion_count (int): Total suggestions
            case_sensitive (bool): Case sensitive search True/False - Search is faster when case sensitivity is True

        Returns:
            pl.DataFrame | None: Returns a Polars DataFrame

        """
        df = await self._get_cache()
        if case_sensitive:
            rows = df.filter(pl.col("scheme_name").str.contains(query, literal=True)).head(suggestion_count)
        else:
            rows = df.filter(pl.col("scheme_name").str.contains(f"(?i){re.escape(query)}", literal=False))
        return rows


if __name__ == "__main__":
    import asyncio

    async def main() -> None:  # noqa: D103
        async with NAVStore() as store:
            data = await store.search_schemes("SBI", case_sensitive=True, suggestion_count=1000)
            print(data)

    asyncio.run(main())
