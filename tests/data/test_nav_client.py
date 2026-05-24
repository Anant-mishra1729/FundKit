"""Basic examples."""

import asyncio
from datetime import date

from fundkit import HistoricalNAVClient, NAVClient


async def main() -> None:
    """Test fundkit."""
    async with NAVClient() as client:
        data = await client.get_nav(123456)
        print(data.columns)

    async with HistoricalNAVClient(verbose=True) as client:
        data = await client.get_history(112340, start_date=date(2026, 4, 27))
        print(data)


asyncio.run(main())
