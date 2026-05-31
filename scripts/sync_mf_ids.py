"""Generate mf_id_map JSON - probes AMFI NAV download endpoint."""

import asyncio
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

MF_RANGE = range(1, 150)
OUTPUT_PATH = Path("mf_id_map.json")
URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
FROM_DATE = (date.today() - timedelta(days=5)).strftime("%d-%b-%Y")
TO_DATE = date.today().strftime("%d-%b-%Y")


async def check_amfi(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    mf_id: int,
) -> tuple[str | None, int]:
    """Probe one mf_id - returns (fund_name, mf_id) or (None, mf_id)."""
    async with semaphore:
        try:
            response = await client.get(
                URL,
                params={"mf": mf_id, "frmdt": FROM_DATE, "todt": TO_DATE, "tp": 1},
                follow_redirects=True,
            )
            response.raise_for_status()
            text = response.text.strip()

            if not text or text.startswith("<") or ";" not in text:
                return None, mf_id

            for line in text.splitlines():
                line = line.strip()
                if line and ";" not in line and not line.startswith(("Scheme Code", "Open", "Close", "Interval")):
                    logger.info(f"Found: {mf_id} → {line}")
                    return line, mf_id

        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(f"mf_id={mf_id} failed: {e}")
        except Exception as e:
            logger.warning(f"mf_id={mf_id} unexpected error: {e}")

    return None, mf_id


async def discover_mf_ids() -> dict[str, int]:
    """Probe AMFI concurrently - semaphore limits to 10 at a time."""
    semaphore = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=15) as client:
        results = await asyncio.gather(*[check_amfi(client, semaphore, mf_id) for mf_id in MF_RANGE])
    return {name: mf_id for name, mf_id in results if name is not None}


def load_existing() -> dict[str, int]:
    """Load existing map - empty dict if file doesn't exist."""
    if not OUTPUT_PATH.exists():
        return {}
    try:
        return json.loads(OUTPUT_PATH.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read existing map: {e}. Starting fresh.")
        return {}


async def main() -> None:
    """Fetch AMFI MF map."""
    existing = load_existing()
    logger.info(f"Loaded {len(existing)} existing entries.")

    discovered = await discover_mf_ids()
    logger.info(f"Discovered {len(discovered)} fund houses from AMFI.")

    # Merge - discovered wins on conflict, old entries preserved
    new_entries = {k: v for k, v in discovered.items() if k not in existing}
    merged = {**existing, **discovered}

    sorted_merged = dict(sorted(merged.items(), key=lambda x: x[1]))
    OUTPUT_PATH.write_text(json.dumps(sorted_merged, indent=4))
    logger.info(f"Written {len(sorted_merged)} entries to {OUTPUT_PATH}.")

    if new_entries:
        logger.info(f"New fund houses: {new_entries}")
        sys.exit(1)  # signals GitHub Actions to commit
    else:
        logger.info("No new fund houses found.")
        sys.exit(0)


asyncio.run(main())
