"""Schema Details model."""

from datetime import date

from pydantic import BaseModel, ConfigDict, model_validator


class SchemeDetails(BaseModel):
    """Scheme details."""

    model_config = ConfigDict(frozen=True)

    scheme_code: int
    scheme_name: str
    scheme_nav_name: str
    scheme_type: str
    scheme_category: str
    amc: str
    amc_id: int | None
    isin: str | None
    minimum_amount_raw: str | None  # "Rs 5000 and in multiples of Re. 1 thereafter"
    minimum_amount: float | None  # 5000.0 - extracted numeric, None if unparseable
    launch_date: date | None
    closure_date: date | None

    @model_validator(mode="before")
    @classmethod
    def extract_minimum_amount(cls, data: dict[str, int | float | str | None]) -> dict[str, int | float | str | None]:
        """Extract numeric value from minimum_amount string."""
        raw = data.get("minimum_amount_raw") or data.get("minimum_amount")
        if not raw:
            data["minimum_amount_raw"] = None
            data["minimum_amount"] = None
            return data

        data["minimum_amount_raw"] = str(raw)

        # Extract first number - handles:
        # "Rs 5000 and in multiples..."
        # "5000"
        # "Rs. 1,000"
        # "Rs 500/- per application"
        import re

        match = re.search(r"[\d,]+(?:\.\d+)?", str(raw).replace(",", ""))  # To extract number from string
        if match:
            try:
                data["minimum_amount"] = float(match.group().replace(",", ""))
            except ValueError:
                data["minimum_amount"] = None
        else:
            data["minimum_amount"] = None

        return data
