"""Transaction schema for all FundKit computations."""

import math
from datetime import date
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fundkit.schema.enums import TransactionType


class Transaction(BaseModel):
    """A single cash flow event between an investor and a mutual fund scheme.

    amount and units are always positive and flow direction is encoded in transaction_type:
        PURCHASE / SWITCH_IN         -> cash flows out of investor
        REDEMPTION / SWITCH_OUT      -> cash flows into investor
        DIVIDEND_PAYOUT              -> cash flows into investor
        DIVIDEND_REINVESTMENT        -> no cash flow (amount = 0, units > 0)

    """

    model_config = ConfigDict(frozen=True) # Frozen - transaction is immutable

    scheme_code : int = Field(gt=0)
    scheme_name : str | None = None
    transaction_date : date
    amount: float = Field(ge=0)   # ge=0 DIVIDEND_REINVESTMENT has amount 0
    units : float  = Field(gt=0)
    nav : float = Field(gt=0)
    transaction_type : TransactionType

    @model_validator(mode='after')
    def validate_transaction(self) -> Self:
        """Validate after all fields are set."""
        # DIVIDEND_REINVESTMENT must have zero amount
        if self.transaction_type == TransactionType.DIVIDEND_REINVESTMENT:
            if self.amount != 0:
                raise ValueError(
                    f"DIVIDEND_REINVESTMENT must have amount=0, got {self.amount}. "
                    "Reinvested dividends have no actual cash flow."
                )
            return self

        # All other types must have amount > 0
        if self.amount <= 0:
            raise ValueError(
                f"amount must be > 0 for {self.transaction_type}, got {self.amount}."
            )

        # Check if NAV is approximately equal
        derived_nav = self.amount / self.units
        if not math.isclose(derived_nav, self.nav, rel_tol=0.01):
            raise ValueError(
                f"NAV mismatch: amount / units = {derived_nav:.4f} "
                f"but nav = {self.nav:.4f}. "
                f"Check your transaction data, NAV tolearance is 1%."
            )
        return self

    # Check if transaction date is > today
    @field_validator('transaction_date', mode='after')
    @classmethod
    def validate_transaction_date(cls, val:date) -> date:
        """Validate transaction date > today."""
        if val > date.today():
            raise ValueError(f"Invalid transaction date: {val}, it cannot be in future.")
        return val
