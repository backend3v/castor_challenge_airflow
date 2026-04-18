"""Upsert into ``financial_records`` and advance checkpoint per successful chunk."""

from __future__ import annotations

import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.config import Settings, get_settings
from pipeline.db import get_etl_engine
from pipeline.exceptions import LoadError
from pipeline.log import configure_logging, get_logger
from pipeline.resilience.checkpoint import update_checkpoint_after_chunk
from pipeline.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.resilience.retry import with_db_retry

log = get_logger(__name__)

_UPSERT_SQL = text(
    """
    INSERT INTO financial_records (
        transaction_id,
        account_id,
        category_id,
        amount,
        currency,
        transaction_type,
        status,
        description,
        created_at,
        updated_at,
        source_updated_at
    ) VALUES (
        :transaction_id,
        :account_id,
        :category_id,
        :amount,
        :currency,
        :transaction_type,
        :status,
        :description,
        :created_at,
        :updated_at,
        :source_updated_at
    )
    ON CONFLICT (transaction_id) DO UPDATE SET
        account_id = EXCLUDED.account_id,
        category_id = EXCLUDED.category_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        transaction_type = EXCLUDED.transaction_type,
        status = EXCLUDED.status,
        description = EXCLUDED.description,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        source_updated_at = EXCLUDED.source_updated_at,
        loaded_at = NOW()
    """
)


def _load_validated_chunks(
    engine: Engine,
    pipeline_run_id: str,
) -> list[tuple[int, list[dict[str, Any]], datetime]]:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT chunk_index, records, source_high_watermark
                FROM etl_validated
                WHERE pipeline_run_id = :run_id
                ORDER BY chunk_index ASC
                """
            ),
            {"run_id": pipeline_run_id},
        )
        out: list[tuple[int, list[dict[str, Any]], datetime]] = []
        for row in result.mappings().all():
            raw = row["records"]
            if isinstance(raw, str):
                payload = json.loads(raw)
            else:
                payload = raw
            wm = row["source_high_watermark"]
            if isinstance(wm, datetime):
                high_wm: datetime = wm
            else:
                high_wm = datetime.fromisoformat(str(wm).replace("Z", "+00:00"))
            out.append((int(row["chunk_index"]), list(payload), high_wm))
        return out


def _row_to_bind(row: dict[str, Any]) -> dict[str, Any]:
    tid = row["transaction_id"]
    aid = row["account_id"]
    return {
        "transaction_id": UUID(tid) if isinstance(tid, str) else tid,
        "account_id": UUID(aid) if isinstance(aid, str) else aid,
        "category_id": int(row["category_id"]),
        "amount": Decimal(str(row["amount"])) if not isinstance(row["amount"], Decimal) else row["amount"],
        "currency": row["currency"],
        "transaction_type": row["transaction_type"],
        "status": row["status"],
        "description": row.get("description"),
        "created_at": row["created_at"]
        if isinstance(row["created_at"], datetime)
        else datetime.fromisoformat(str(row["created_at"]).replace("Z", "+00:00")),
        "updated_at": row["updated_at"]
        if isinstance(row["updated_at"], datetime)
        else datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00")),
        "source_updated_at": row["updated_at"]
        if isinstance(row["updated_at"], datetime)
        else datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00")),
    }


@with_db_retry
def _upsert_chunk(engine: Engine, rows: list[dict[str, Any]]) -> None:
    with engine.begin() as conn:
        for row in rows:
            conn.execute(_UPSERT_SQL, _row_to_bind(row))


def _upsert_chunk_guarded(engine: Engine, rows: list[dict[str, Any]]) -> None:
    get_db_circuit_breaker().call(lambda: _upsert_chunk(engine, rows))


def run_load(
    pipeline_run_id: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Upsert validated rows and move checkpoint watermark per chunk."""
    configure_logging()
    cfg = settings or get_settings()
    engine = get_etl_engine(cfg)
    t0 = time.perf_counter()
    records_loaded = 0

    try:
        chunks = _load_validated_chunks(engine, pipeline_run_id)
        log.info(
            "carga_iniciada",
            event="run_load",
            pipeline_run_id=pipeline_run_id,
            chunks=len(chunks),
        )

        for chunk_index, rows, high_wm in chunks:
            if rows:
                _upsert_chunk_guarded(engine, rows)
                records_loaded += len(rows)
            update_checkpoint_after_chunk(engine, pipeline_run_id, high_wm, 1)
            log.info(
                "chunk_loaded",
                event="upsert_batch",
                pipeline_run_id=pipeline_run_id,
                chunk_index=chunk_index,
                registros=len(rows),
            )

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        log.info(
            "carga_completada",
            event="run_load",
            pipeline_run_id=pipeline_run_id,
            registros=records_loaded,
            duracion_ms=elapsed_ms,
        )
        return {
            "pipeline_run_id": pipeline_run_id,
            "records_loaded": records_loaded,
            "chunks_processed": len(chunks),
        }
    except Exception as exc:
        log.error(
            "carga_fallida",
            event="run_load",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise LoadError("load step failed") from exc
