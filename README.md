# FundKit

A modern, async-first Python library for Indian mutual fund data. FundKit fetches NAV and scheme information directly from [AMFI](https://www.amfiindia.com/) - no third-party data vendors, no opaque middle layers.

If you're building dashboards, research pipelines, or backend services that need reliable fund data, FundKit gives you typed DataFrames with sensible caching out of the box.

```python
import asyncio
from fundkit import NAVClient

async def main():
    async with NAVClient() as client:
        nav = await client.get_nav(128628)
        print(nav)

asyncio.run(main())
```

## Installation

```bash
pip install fundkit
# or
uv add fundkit
```

Optional pandas support:

```bash
pip install fundkit[pandas]
```

## What's available

The `data` module is ready to use today. Planned modules - `schema`, `portfolio`, `analytics`, `tax`, `sip`, `compare`, and more - are still in progress.

| Client | Purpose |
|--------|---------|
| `NAVClient` | Today's NAV - lookup by scheme code, name, AMC, or fund type |
| `HistoricalNAVClient` | NAV history for a date range |

Both clients return **Polars** DataFrames by default. Pass `df_format="pandas"` if you prefer pandas. They also share helper methods for scheme validation and discovery - see the [data module docs](src/fundkit/data/README.md#api-reference) for the full list.

## Usage

```python
import asyncio
from datetime import date
from fundkit import NAVClient, HistoricalNAVClient

async def main():
    async with NAVClient(verbose=True) as client:
        # Single or multiple scheme codes
        nav = await client.get_nav(128628)
        batch = await client.get_nav([119597, 120505, 108272])

        # Search by name, AMC, or fund type
        by_name = await client.get_nav_by_name("bluechip", case_sensitive=False)
        by_amc = await client.get_nav_by_amc("SBI")
        by_type = await client.get_nav_by_type("Open Ended Schemes")

        # Utilities
        valid = await client.is_valid_scheme_code(119597)
        schemes = await client.get_scheme_codes(query="bluechip", by="scheme_name")
        amcs = await client.get_amc_list()

    async with HistoricalNAVClient(verbose=True) as client:
        history = await client.get_history(
            124182,
            start_date=date(2023, 1, 1),
            end_date=date.today(),
            df_format="pandas",  # optional - defaults to polars
        )

asyncio.run(main())
```

Set `verbose=True` on any client to print cache hits, fetches, and other log messages to the console.

For architecture, caching details, output schemas, and a full method reference, see [`src/fundkit/data/README.md`](src/fundkit/data/README.md).

## How FundKit compares

Most Python libraries in this space follow a similar pattern: a synchronous class that wraps a third-party API (like mfapi.in), returns dicts or pandas DataFrames, and re-fetches on every call. 

That works fine for one-off scripts, but it gets painful in async web apps or when you're filtering thousands of schemes repeatedly.

FundKit takes a different approach:

| | Typical MF data libraries | FundKit |
|---|--------------------------|---------|
| **Data source** | Third-party API proxy | AMFI directly |
| **Execution model** | Synchronous (`requests`) | Async-first (`httpx` + `asyncio`) |
| **Default output** | Dict / JSON / pandas | Typed Polars DataFrame |
| **Bulk filtering** | Load everything, filter in Python | Vectorized Polars operations |
| **Caching** | None or in-memory only | Memory -> disk (parquet) -> network |
| **Typed schemas** | Untyped dicts | Consistent column names and dtypes |

FundKit doesn't try to replace every feature these libraries offer (daily performance metrics, JSON export, MCP servers, etc.) - at least not yet. The focus right now is a solid, fast, cache-aware data layer you can build on top of.

## Caching

FundKit caches data locally so repeated calls in the same day don't hammer AMFI. 

<img src="https://raw.githubusercontent.com/Anant-mishra1729/FundKit/411484b014abe313e17c520562c42841520606f7/images/Caching.svg" width = "400px">


Cache location is platform native:

| Platform | Path |
|----------|------|
| Linux | `~/.cache/fundkit/` |
| macOS | `~/Library/Caches/fundkit/` |
| Windows | `%LOCALAPPDATA%\fundkit\` |

| Data | TTL |
|------|-----|
| Latest NAV | Same calendar day (memory + disk) |
| Historical NAV | Permanent, append-only per AMC |

Historical data is never re-fetched once cached - past NAV is immutable. Latest NAV refreshes automatically when the calendar day changes.

## Why FundKit

- **Async-first** - built on httpx and asyncio; fits naturally into FastAPI, Django async views, or any modern async stack
- **AMFI-native** - one less dependency that can go down or change its API without warning
- **Polars-first** - filtering ~15k schemes is significantly faster than row-by-row Python; pandas export is one argument away
- **Typed and structured** - consistent column names (`scheme_code`, `nav`, `date`, …) whether you're querying one scheme or ten thousand
