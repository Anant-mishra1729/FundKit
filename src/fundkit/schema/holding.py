"""Container for all transactions in one scheme."""

from pydantic import BaseModel, ConfigDict, Field

from fundkit.schema.transaction import Transaction


class Holding(BaseModel):
    """Container for all transactions in one scheme."""

    model_config = ConfigDict(frozen=False)

    scheme_code : int = Field(gt=0)
    scheme_name : str | None = None
    transactions : list[Transaction] = Field(default_factory=list)


