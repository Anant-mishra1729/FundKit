"""Container of Holdings."""

from pydantic import BaseModel, ConfigDict, Field

from fundkit.schema.holding import Holding


class Portfolio(BaseModel):
    """Container of Holdings."""

    model_config = ConfigDict(frozen=False)
    name : str | None = None

    holdings : list[Holding] = Field(default_factory=list)
