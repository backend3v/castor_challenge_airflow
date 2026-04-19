"""Pydantic model for ``transactions`` (see ``docker/sql/01_schema.sql``)."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, Mapping, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pipeline.domain.models.common import CurrencyCode, Money, normalize_iso_currency

TransactionType: TypeAlias = Literal["debit", "credit"]
TransactionStatus: TypeAlias = Literal["pending", "completed", "failed"]


class Transaction(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    transaction_id: Annotated[int, Field(ge=1)]
    account_id: UUID
    category_id: Annotated[int, Field(ge=1)]
    amount: Money
    currency: CurrencyCode = "USD"
    transaction_type: TransactionType
    status: TransactionStatus
    description: Annotated[str | None, Field(max_length=255)] = None
    created_at: datetime
    updated_at: datetime

    @field_validator("currency", mode="before")
    @classmethod
    def currency_normalized(cls, value: object) -> str:
        return normalize_iso_currency(value)

    @field_validator("amount")
    @classmethod
    def amount_not_zero(cls, value: Decimal) -> Decimal:
        if value == 0:
            msg = "amount must not be zero (CHECK constraint on transactions.amount)"
            raise ValueError(msg)
        return value


def parse_transaction(data: Mapping[str, Any]) -> Transaction:
    return Transaction.model_validate(dict(data))
