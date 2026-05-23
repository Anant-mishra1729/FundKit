"""Generate mf_id_map JSON file."""

import asyncio
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


MF_RANGE = range(1, 120)
OUTPUT_PATH = Path("mf_id_map.json")

URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

FROM_DATE = (date.today() - timedelta(days=5)).strftime("%d-%b-%Y")
TO_DATE = date.today().strftime("%d-%b-%Y")


class JsonSaveError(Exception):
    """Error raised on json write."""

    pass


async def check_amfi(client: httpx.AsyncClient, mf_id: int) -> tuple[str | None, int]:
    """GET request on AMFI Download Portal.

    Args:
        client (httpx.AsyncClient): Async HTTPX Client
        mf_id (int): Possible Fund ID

    Returns:
        tuple[str | None, int]: Fund Name - Fund ID mapping

    """
    try:
        response = await client.get(
            URL,
            params={"mf": mf_id, "frmdt": FROM_DATE, "todt": TO_DATE, "tp": 1},
            follow_redirects=True,
        )
        response.raise_for_status()

        text = response.text.strip()

        # Remove HTML
        if not text or text.startswith("<"):
            return None, mf_id

        # Remove ';'
        if ";" not in text:
            return None, mf_id

        for line in text.splitlines():
            line = line.strip()
            if (
                line
                and ";" not in line
                and not line.startswith("Scheme Code")
                and not line.startswith("Open")
                and not line.startswith("Close")
                and not line.startswith("Interval")
            ):
                return line, mf_id

    except Exception:
        pass
    return None, mf_id


async def discover_mf_ids() -> dict[str, int]:
    """Fetch Fund Name - Fund ID mapping."""
    async with httpx.AsyncClient(timeout=15) as client:
        tasks = [check_amfi(client, mf_id) for mf_id in MF_RANGE]
        results = await asyncio.gather(*tasks)

    return {fund_house: mf_id for fund_house, mf_id in results if fund_house is not None}


async def main() -> None:
    """Driver method."""
    mapping = await discover_mf_ids()
    try:
        logger.info(f"Storing output mapping in {OUTPUT_PATH}")
        with Path.open(OUTPUT_PATH, "w") as f:
            json.dump(mapping, f, indent=4)
    except Exception as e:
        raise JsonSaveError(f"Cannot write the JSON data in path {OUTPUT_PATH}") from e


asyncio.run(main())
