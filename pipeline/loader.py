"""Upsert into ``financial_records`` and advance checkpoint per successful chunk."""

from __future__ import annotations

import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, Integer, MetaData, Numeric, String, Table, func, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from pipeline.config import Settings, get_settings
from pipeline.db import get_etl_engine
from pipeline.exceptions import LoadError
from pipeline.log import configure_logging, get_logger
from pipeline.resilience.checkpoint import update_checkpoint_after_chunk
from pipeline.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.resilience.retry import with_db_retry

log = get_logger(__name__)

# SQLAlchemy Core table — must match ``docker/sql/01_schema.sql`` (financial_records)
_METADATA = MetaData()
FINANCIAL_RECORDS = Table(
    "financial_records",
    _METADATA,
    Column("transaction_id", PG_UUID(as_uuid=True), primary_key=True, nullable=False),
    Column("account_id", PG_UUID(as_uuid=True), nullable=False),
    Column("category_id", Integer(), nullable=False),
    Column("amount", Numeric(18, 2), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("transaction_type", String(10), nullable=False),
    Column("status", String(10), nullable=False),
    Column("description", String(255)),
    Column("created_at", DateTime(timezone=False), nullable=False),
    Column("updated_at", DateTime(timezone=False), nullable=False),
    Column("source_updated_at", DateTime(timezone=False), nullable=False),
    Column("loaded_at", DateTime(timezone=False), server_default=text("NOW()"), nullable=False),
)


def _bulk_upsert_statement(rows: list[dict[str, Any]]):
    """Build one ``INSERT ... ON CONFLICT DO UPDATE`` for all rows (PostgreSQL)."""
    fr = FINANCIAL_RECORDS
    insert_stmt = insert(fr).values(rows)
    return insert_stmt.on_conflict_do_update(
        index_elements=[fr.c.transaction_id],
        set_={
            fr.c.account_id: insert_stmt.excluded.account_id,
            fr.c.category_id: insert_stmt.excluded.category_id,
            fr.c.amount: insert_stmt.excluded.amount,
            fr.c.currency: insert_stmt.excluded.currency,
            fr.c.transaction_type: insert_stmt.excluded.transaction_type,
            fr.c.status: insert_stmt.excluded.status,
            fr.c.description: insert_stmt.excluded.description,
            fr.c.created_at: insert_stmt.excluded.created_at,
            fr.c.updated_at: insert_stmt.excluded.updated_at,
            fr.c.source_updated_at: insert_stmt.excluded.source_updated_at,
            fr.c.loaded_at: func.now(),
        },
    )


def _load_validated_chunks(
    engine: Engine,
    pipeline_run_id: str,
) -> list[tuple[int, list[dict[str, Any]], datetime]]:
    from sqlalchemy import text as sql_text

    with engine.connect() as conn:
        result = conn.execute(
            sql_text(
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
    """One round-trip per chunk: multi-row upsert (not one statement per row)."""
    if not rows:
        return
    bound = [_row_to_bind(r) for r in rows]
    stmt = _bulk_upsert_statement(bound)
    with engine.begin() as conn:
        conn.execute(stmt)


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
