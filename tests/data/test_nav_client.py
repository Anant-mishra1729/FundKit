"""Basic examples."""

import asyncio

from fundkit import NAVClient


async def main() -> None:  # noqa: D103
    async with NAVClient(verbose=False) as client:
        # ----------- Fetch NAV data for a single scheme ----------
        nav = await client.get_nav(128628)
        print("Single Scheme NAV Data")
        print(f"Scheme Code           : {nav['scheme_code'].item()}")
        print(f"ISIN (Growth/Payout)  : {nav['isin_growth_or_payout'].item()}")
        print(f"ISIN (Div Reinvest)   : {nav['isin_div_reinvestment'].item()}")
        print(f"Scheme Name           : {nav['scheme_name'].item()}")
        print(f"NAV                   : {nav['nav'].item()}")
        print(f"Date                  : {nav['date'].item()}")
        print(f"AMC                   : {nav['amc'].item()}")
        print(f"Scheme Type           : {nav['scheme_type'].item()}")
        print()

        # ------------ Fetch NAV data for multiple schemes ---------
        df = await client.get_nav([119597, 120505, 108272])
        print(df)

        # ------------ Search scheme by name ----------------------
        results = await client.get_nav_by_name("bluechip", case_sensitive=False)

        # ------------ Search scheme by AMC -----------------------
        results = await client.get_nav_by_amc("SBI")

        # ------------ Search scheme by Fund type -----------------
        results = await client.get_nav_by_type("Open Ended Schemes")
        print(results)

        # ------------ Validate scheme code ---------------
        is_valid = await client.is_valid_scheme_code(119597)
        print(is_valid)

        # ------------ Force refresh the disk-cache ----------------
        await client.refresh_nav_cache()

        # ------------ Other functions ------------------
        # All scheme codes
        sch_codes = await client.get_scheme_codes(df_format="pandas")

        # Search schem by scheme names
        sch_codes = await client.get_scheme_codes(query="sbi", by="scheme_name")

        # Search scheme by scheme code
        sch_codes = await client.get_scheme_codes(query=123456, by="scheme_code")
        print(sch_codes)

        sch2 = await client.get_amc_list()
        print(sch2)

asyncio.run(main())
