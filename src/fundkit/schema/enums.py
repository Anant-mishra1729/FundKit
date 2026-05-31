"""Enum classes to be used for other modules."""

from enum import StrEnum


class TransactionType(StrEnum):
    """Supported mutual fund transaction types."""

    PURCHASE = "PURCHASE"
    REDEMPTION = "REDEMPTION"
    SWITCH_IN = "SWITCH_IN"
    SWITCH_OUT = "SWITCH_OUT"
    DIVIDEND_PAYOUT = "DIVIDEND_PAYOUT"
    DIVIDEND_REINVESTMENT = "DIVIDEND_REINVESTMENT"


class SIPFrequency(StrEnum):
    """Supported frequencies for Systematic Investment Plans (SIPs)."""

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    QUARTERLY = "QUARTERLY"


class FundCategory(StrEnum):
    """Supported mutual fund categories."""

    EQUITY = "EQUITY"
    DEBT = "DEBT"
    HYBRID = "HYBRID"
    ELSS = "ELSS"
    INDEX = "INDEX"
    LIQUID = "LIQUID"
