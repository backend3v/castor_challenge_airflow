"""Tests for ``pipeline.transformer``."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError as PydanticValidationError

from pipeline.models.financial_record.model import FinancialRecord


def test_transformer_cuando_fila_valida_entonces_modelo_ok(sample_transaction_row: dict) -> None:
    rec = FinancialRecord.model_validate(sample_transaction_row)
    assert rec.transaction_id is not None
    assert rec.amount == Decimal("10.50")


def test_transformer_cuando_amount_cero_entonces_pydantic_falla() -> None:
    now = datetime(2026, 1, 1, 0, 0, 0)
    bad = {
        "transaction_id": str(uuid4()),
        "account_id": str(uuid4()),
        "category_id": 1,
        "amount": Decimal("0"),
        "currency": "USD",
        "transaction_type": "debit",
        "status": "completed",
        "description": None,
        "created_at": now,
        "updated_at": now,
    }
    with pytest.raises(PydanticValidationError):
        FinancialRecord.model_validate(bad)


@patch("pipeline.transformer._load_staging_rows")
@patch("pipeline.transformer.insert_dead_letter")
@patch("pipeline.transformer._persist_validated")
def test_run_validation_cuando_staging_vacio_entonces_sin_dlq(
    mock_persist: MagicMock,
    mock_dlq: MagicMock,
    mock_load: MagicMock,
) -> None:
    mock_load.return_value = []
    from pipeline.transformer import run_validation

    out = run_validation("run-1")
    assert out["dlq_count"] == 0
    assert out["chunks_validated"] == 0
    mock_dlq.assert_not_called()
