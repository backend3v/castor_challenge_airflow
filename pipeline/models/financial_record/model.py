"""Validated warehouse row (same contract as ``transactions``)."""

from __future__ import annotations

from typing import Any, Mapping

from pydantic import ConfigDict

from pipeline.models.transaction.model import Transaction


class FinancialRecord(Transaction):
    """Schema for rows upserted into ``financial_records``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


def parse_financial_record(data: Mapping[str, Any]) -> FinancialRecord:
    """Validate a mapping into a ``FinancialRecord``."""
    return FinancialRecord.model_validate(dict(data))
