"""Historical NAV Client examples."""
import asyncio
from datetime import date

from fundkit import HistoricalNAVClient


async def main() -> None:  # noqa: D103
    async with HistoricalNAVClient(verbose=True) as client:
        data = await client.get_history(124182, start_date=date(2023, 1, 1), end_date=date.today())
        print(data)

    async with HistoricalNAVClient(verbose=True) as client:
        data = await client.get_history(124182, start_date=date(2025, 1, 1), end_date=date(2026, 3, 31))
        print(data)

    async with HistoricalNAVClient(verbose=True) as client:
        data = await client.get_history(124182, start_date=date(2025, 1, 1), df_format="pandas")
        print(data)

asyncio.run(main())
