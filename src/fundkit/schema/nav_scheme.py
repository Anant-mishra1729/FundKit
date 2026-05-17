from datetime import date  # noqa: D100

from pydantic import BaseModel, ConfigDict, field_validator


class NavScheme(BaseModel):
    """Pydantic model for NAV Data."""

    model_config = ConfigDict(frozen=True)
    scheme_code: int
    isin_growth_or_payout: str | None = None
    isin_div_reinvestment: str | None = None
    scheme_name: str
    nav: float
    date: date

    # To clean the fields before type checking by pydantic
    @field_validator("isin_growth_or_payout", "isin_div_reinvestment", mode="before")
    @classmethod
    def clean_empty_strings(cls, v: str) -> str | None:
        """Clean the fields before type checking by pydantic."""
        return None if v == "-" or v == "" else v
