"""Validated warehouse row (same contract as ``transactions``)."""

from __future__ import annotations

from typing import Any, Mapping

from pydantic import ConfigDict

from pipeline.domain.models.transaction.model import Transaction


class FinancialRecord(Transaction):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


def parse_financial_record(data: Mapping[str, Any]) -> FinancialRecord:
    return FinancialRecord.model_validate(dict(data))
