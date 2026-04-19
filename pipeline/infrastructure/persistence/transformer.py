"""Validate staged JSON rows with Pydantic; route invalid rows to the dead-letter table."""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.utils.config import Settings, get_settings
from pipeline.utils.db import get_etl_engine
from pipeline.utils.exceptions import CheckpointError, ValidationError
from pipeline.utils.log import configure_logging, get_logger
from pipeline.domain.models.financial_record.model import FinancialRecord
from pipeline.infrastructure.resilience.checkpoint import insert_dead_letter
from pipeline.infrastructure.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.infrastructure.resilience.retry import with_db_retry
from pipeline.utils.serialize import records_json_dumps

log = get_logger(__name__)


def _load_staging_rows(
    engine: Engine,
    pipeline_run_id: str,
) -> list[tuple[int, list[dict[str, Any]]]]:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT chunk_index, records
                FROM etl.etl_staging
                WHERE pipeline_run_id = :run_id
                ORDER BY chunk_index ASC
                """
            ),
            {"run_id": pipeline_run_id},
        )
        out: list[tuple[int, list[dict[str, Any]]]] = []
        for row in result.mappings().all():
            raw = row["records"]
            if isinstance(raw, str):
                payload = json.loads(raw)
            else:
                payload = raw
            out.append((int(row["chunk_index"]), list(payload)))
        return out


def _max_source_updated_at(rows: list[dict[str, Any]]) -> datetime:
    """Max ``updated_at`` from raw extract rows (for checkpoint when DLQ drops all)."""
    times: list[datetime] = []
    for raw in rows:
        u = raw.get("updated_at")
        if isinstance(u, datetime):
            times.append(u)
        elif isinstance(u, str):
            times.append(datetime.fromisoformat(u.replace("Z", "+00:00")))
    if not times:
        return datetime(1970, 1, 1)
    return max(times)


@with_db_retry
def _insert_validated_chunk(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    records: list[dict[str, Any]],
    source_high_watermark: datetime,
) -> None:
    payload = records_json_dumps(records)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl.etl_validated (
                    pipeline_run_id, chunk_index, records, source_high_watermark
                )
                VALUES (:run_id, :idx, CAST(:payload AS jsonb), :wm)
                ON CONFLICT (pipeline_run_id, chunk_index) DO UPDATE SET
                    records = EXCLUDED.records,
                    source_high_watermark = EXCLUDED.source_high_watermark,
                    created_at = NOW()
                """
            ),
            {
                "run_id": pipeline_run_id,
                "idx": chunk_index,
                "payload": payload,
                "wm": source_high_watermark,
            },
        )


def _persist_validated(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    records: list[dict[str, Any]],
    source_high_watermark: datetime,
) -> None:
    get_db_circuit_breaker().call(
        lambda: _insert_validated_chunk(
            engine,
            pipeline_run_id,
            chunk_index,
            records,
            source_high_watermark,
        ),
    )


def _normalize_row_types(row: dict[str, Any]) -> dict[str, Any]:
    """Coerce JSONB / driver types into values Pydantic accepts."""
    from decimal import Decimal
    from uuid import UUID

    out: dict[str, Any] = {}
    for key, val in row.items():
        if key == "transaction_id":
            out[key] = int(val)
        elif key == "account_id" and isinstance(val, str):
            out[key] = UUID(val)
        elif key == "amount":
            out[key] = val if isinstance(val, Decimal) else Decimal(str(val))
        elif key in {"created_at", "updated_at"} and isinstance(val, str):
            out[key] = datetime.fromisoformat(val.replace("Z", "+00:00"))
        else:
            out[key] = val
    return out


def run_validation(
    pipeline_run_id: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Validate each staged chunk; invalid rows go to ``etl_failed_records``."""
    configure_logging()
    cfg = settings or get_settings()
    engine = get_etl_engine(cfg)
    t0 = time.perf_counter()
    dlq_count = 0

    try:
        staging = _load_staging_rows(engine, pipeline_run_id)
        log.info(
            "validacion_iniciada",
            operation="run_validation",
            pipeline_run_id=pipeline_run_id,
            chunks=len(staging),
        )

        for chunk_index, rows in staging:
            chunk_wm = _max_source_updated_at(rows)
            valid: list[dict[str, Any]] = []
            for raw in rows:
                normalized = _normalize_row_types(raw)
                try:
                    rec = FinancialRecord.model_validate(normalized)
                    valid.append(rec.model_dump(mode="python"))
                except PydanticValidationError as exc:
                    dlq_count += 1
                    insert_dead_letter(
                        engine,
                        pipeline_run_id,
                        chunk_index,
                        raw,
                        str(exc),
                    )
            _persist_validated(engine, pipeline_run_id, chunk_index, valid, chunk_wm)

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        log.info(
            "validacion_completada",
            operation="run_validation",
            pipeline_run_id=pipeline_run_id,
            registros_dlq=dlq_count,
            duracion_ms=elapsed_ms,
        )
        return {
            "pipeline_run_id": pipeline_run_id,
            "dlq_count": dlq_count,
            "chunks_validated": len(staging),
        }
    except CheckpointError:
        raise
    except Exception as exc:
        log.error(
            "validacion_fallida",
            operation="run_validation",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise ValidationError("validation step failed") from exc
