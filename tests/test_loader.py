"""Tests for ``pipeline.loader``."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch


@patch("pipeline.loader.update_checkpoint_after_chunk")
@patch("pipeline.loader._upsert_chunk_guarded")
@patch("pipeline.loader._load_validated_chunks")
def test_run_load_cuando_sin_chunks_entonces_cero_registros(
    mock_chunks: MagicMock,
    mock_upsert: MagicMock,
    mock_cp: MagicMock,
) -> None:
    mock_chunks.return_value = []
    from pipeline.config import get_settings
    from pipeline.loader import run_load

    out = run_load("run-x", settings=get_settings())
    assert out["records_loaded"] == 0
    mock_upsert.assert_not_called()


@patch("pipeline.loader._load_validated_chunks")
def test_row_to_bind_via_chunk(mock_chunks: MagicMock) -> None:
    from uuid import uuid4

    from pipeline.loader import _row_to_bind

    tid, aid = uuid4(), uuid4()
    now = datetime(2026, 3, 1, 10, 0, 0)
    row = {
        "transaction_id": str(tid),
        "account_id": str(aid),
        "category_id": 1,
        "amount": "10.00",
        "currency": "USD",
        "transaction_type": "credit",
        "status": "pending",
        "description": None,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }
    b = _row_to_bind(row)
    assert b["transaction_id"] == tid
