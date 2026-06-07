"""Generate mf_id_map JSON - both AMC name variants from AMFI member pages."""

import asyncio
import json
import logging
import sys
from pathlib import Path

import bs4
import httpx

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MF_RANGE = range(1, 150)
OUTPUT_PATH = Path("mf_id_map.json")
MEMBER_URL = "https://www.amfiindia.com/member/{mf_id}"
HEADERS = {"User-Agent": "FundKit/0.1 (+https://github.com/forklore/fundkit)"}


async def fetch_member_page(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    mf_id: int,
) -> tuple[str | None, str | None, int]:
    """Fetch AMFI member page - returns (nav_name, scheme_details_name, mf_id)."""
    async with semaphore:
        try:
            response = await client.get(
                MEMBER_URL.format(mf_id=mf_id),
                follow_redirects=True,
                headers=HEADERS,
            )
            # Invalid ID - redirects away from /member/{id}
            if str(mf_id) not in str(response.url):
                return None, None, mf_id
            if response.status_code == 404:
                return None, None, mf_id

            response.raise_for_status()

            soup = bs4.BeautifulSoup(response.text, "html.parser")
            labels = soup.select(".MuiGrid-grid-md-4")
            values = soup.select(".MuiGrid-grid-md-7")

            data: dict[str, str] = {}
            for label, value in zip(labels, values, strict=False):
                key = label.get_text(strip=True)
                val = value.get_text(strip=True)
                if key:
                    data[key] = val

            nav_name = data.get("Name of the Mutual Fund", "").strip() or None
            scheme_details_name = data.get("Name of Assest Management Co.", "").strip() or None

            if nav_name:
                logger.info(f"Found: {mf_id} - '{nav_name}' / '{scheme_details_name}'")

        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(f"mf_id={mf_id} failed: {e}")
        except Exception as e:
            logger.warning(f"mf_id={mf_id} unexpected error: {e}")
        else:
            return nav_name, scheme_details_name, mf_id

    return None, None, mf_id


async def discover_mf_ids() -> dict[str, int]:
    """Scrape all AMFI member pages - returns both name variants per AMC."""
    semaphore = asyncio.Semaphore(5)
    async with httpx.AsyncClient(timeout=15.0) as client:
        results = await asyncio.gather(*[fetch_member_page(client, semaphore, mf_id) for mf_id in MF_RANGE])

    mapping: dict[str, int] = {}
    for nav_name, scheme_details_name, mf_id in results:
        if nav_name:
            mapping[nav_name] = mf_id
        if scheme_details_name and scheme_details_name != nav_name:
            mapping[scheme_details_name] = mf_id

    return mapping


def load_existing() -> dict[str, int]:
    if not OUTPUT_PATH.exists():
        return {}
    try:
        return json.loads(OUTPUT_PATH.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Could not read existing map: {e}. Starting fresh.")
        return {}


async def main() -> None:
    existing = load_existing()
    logger.info(f"Loaded {len(existing)} existing entries.")

    discovered = await discover_mf_ids()
    logger.info(f"Discovered {len(discovered)} name-id mappings from AMFI.")

    new_entries = {k: v for k, v in discovered.items() if k not in existing}
    merged = {**existing, **discovered}

    # Sort by value (mf_id)
    sorted_merged = dict(sorted(merged.items(), key=lambda x: (x[1], x[0])))
    OUTPUT_PATH.write_text(json.dumps(sorted_merged))
    logger.info(f"Written {len(sorted_merged)} entries to {OUTPUT_PATH}.")

    if new_entries:
        logger.info(f"New entries: {new_entries}")
        sys.exit(1)  # signals GitHub Actions to commit
    else:
        logger.info("No new entries found.")
        sys.exit(0)


asyncio.run(main())
