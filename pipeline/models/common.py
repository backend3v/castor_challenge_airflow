"""Shared types and helpers for financial entity models."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, TypeAlias

from pydantic import Field

Money: TypeAlias = Annotated[Decimal, Field(max_digits=18, decimal_places=2)]
CurrencyCode: TypeAlias = Annotated[str, Field(min_length=3, max_length=3)]


def normalize_iso_currency(value: object) -> str:
    """Normalize a 3-letter currency code (strip + uppercase)."""
    if not isinstance(value, str):
        msg = "currency must be a string of length 3"
        raise TypeError(msg)
    trimmed = value.strip()
    if len(trimmed) != 3:
        msg = "currency must be exactly 3 characters"
        raise ValueError(msg)
    return trimmed.upper()
