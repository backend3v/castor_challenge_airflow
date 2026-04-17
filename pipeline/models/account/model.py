"""Pydantic model for ``accounts`` (see ``docker/sql/01_schema.sql``)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, Mapping, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pipeline.models.common import CurrencyCode, Money, normalize_iso_currency

AccountType: TypeAlias = Literal["savings", "checking", "investment"]


class Account(BaseModel):
    """Row contract for ``accounts`` (UUID PK, typed account_type, money balance)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    account_id: UUID
    account_name: Annotated[str, Field(max_length=100)]
    account_type: AccountType
    owner_name: Annotated[str, Field(max_length=100)]
    currency: CurrencyCode = "USD"
    balance: Money = Field(default=Decimal("0.00"))
    is_active: bool = True
    created_at: datetime
    updated_at: datetime

    @field_validator("currency", mode="before")
    @classmethod
    def currency_normalized(cls, value: object) -> str:
        return normalize_iso_currency(value)


def parse_account(data: Mapping[str, Any]) -> Account:
    return Account.model_validate(dict(data))
