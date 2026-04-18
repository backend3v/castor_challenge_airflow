"""Tests for bulk upsert in ``pipeline.loader``."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from pipeline.loader import _bulk_upsert_statement, _row_to_bind, _upsert_chunk


def test_bulk_upsert_statement_compila_en_postgres() -> None:
    """Un INSERT multi-fila con ON CONFLICT debe compilar para dialecto PostgreSQL."""
    now = datetime(2026, 4, 1, 10, 0, 0)
    tid, aid = uuid4(), uuid4()
    a1, a2 = uuid4(), uuid4()
    row_a = _row_to_bind(
        {
            "transaction_id": str(tid),
            "account_id": str(aid),
            "category_id": 1,
            "amount": "99.99",
            "currency": "USD",
            "transaction_type": "credit",
            "status": "completed",
            "description": None,
            "created_at": now,
            "updated_at": now,
        }
    )
    row_b = _row_to_bind(
        {
            "transaction_id": str(a1),
            "account_id": str(a2),
            "category_id": 1,
            "amount": "1.00",
            "currency": "USD",
            "transaction_type": "debit",
            "status": "pending",
            "description": None,
            "created_at": now,
            "updated_at": now,
        }
    )
    stmt = _bulk_upsert_statement([row_a, row_b])
    compiled = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": False},
        )
    )
    assert "INSERT INTO financial_records" in compiled
    assert "ON CONFLICT" in compiled.upper()


def test_upsert_chunk_una_sola_execute_por_chunk() -> None:
    """Un chunk de N filas debe ejecutar un solo statement (no N executes)."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.begin = MagicMock(return_value=ctx)

    now = datetime(2026, 4, 1, 10, 0, 0)
    rows = []
    for _ in range(7):
        tid, aid = uuid4(), uuid4()
        rows.append(
            _row_to_bind(
                {
                    "transaction_id": str(tid),
                    "account_id": str(aid),
                    "category_id": 1,
                    "amount": Decimal("1.00"),
                    "currency": "USD",
                    "transaction_type": "debit",
                    "status": "pending",
                    "description": "x",
                    "created_at": now,
                    "updated_at": now,
                }
            )
        )

    _upsert_chunk(engine, rows)

    assert conn.execute.call_count == 1
