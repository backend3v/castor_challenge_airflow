"""Transferencia rapida source -> target para pruebas operativas."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipeline.utils.db import get_source_engine, get_target_engine
from pipeline.infrastructure.persistence.loader import _upsert_chunk
from pipeline.domain.models.financial_record.model import FinancialRecord


def run_smoke_transfer(limit: int = 1000) -> dict[str, int]:
    """Extrae una muestra acotada de source y la carga en target para reporteria."""
    query = text(
        """
        SELECT
            transaction_id,
            account_id::text AS account_id,
            category_id,
            amount,
            currency,
            transaction_type,
            status,
            description,
            created_at,
            updated_at
        FROM public.transactions
        ORDER BY updated_at DESC
        LIMIT :lim
        """
    )

    source_engine = get_source_engine()
    with source_engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(query, {"lim": limit}).mappings().all()]

    if not rows:
        return {"rows_read": 0, "rows_loaded": 0}

    valid_rows: list[dict[str, Any]] = [
        FinancialRecord.model_validate(row).model_dump(mode="python") for row in rows
    ]

    _upsert_chunk(get_target_engine(), valid_rows)
    return {"rows_read": len(rows), "rows_loaded": len(valid_rows)}
