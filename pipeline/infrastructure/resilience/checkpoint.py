"""Checkpoint and dead-letter persistence (``etl_checkpoints``, ``etl_failed_records``)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.utils.config import Settings, get_settings
from pipeline.utils.exceptions import CheckpointError
from pipeline.utils.log import get_logger
from pipeline.infrastructure.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.infrastructure.resilience.retry import with_db_retry

log = get_logger(__name__)

_EPOCH = datetime(1970, 1, 1)


@with_db_retry
def _last_completed_watermark_inner(engine: Engine) -> datetime | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT MAX(last_watermark)
                FROM etl.etl_checkpoints
                WHERE status = 'completed'
                """
            ),
        ).scalar()
        return row


def get_last_completed_watermark(engine: Engine) -> datetime | None:
    """Return the latest ``last_watermark`` among completed runs (global resume point)."""
    try:
        return get_db_circuit_breaker().call(lambda: _last_completed_watermark_inner(engine))
    except Exception as exc:
        log.error(
            "last_completed_watermark",
            error=str(exc),
        )
        raise CheckpointError("failed to read last completed watermark") from exc


def compute_extract_since(
    last_completed: datetime | None,
    overlap: timedelta,
) -> datetime:
    """Lower bound for incremental extract (overlap window over last success)."""
    base = last_completed or _EPOCH
    return base - overlap


@with_db_retry
def _ensure_run_inner(
    engine: Engine,
    pipeline_run_id: str,
    initial_watermark: datetime,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl.etl_checkpoints (
                    pipeline_run_id, last_watermark, status, chunks_completed, total_chunks
                )
                VALUES (:run_id, :wm, 'running', 0, NULL)
                ON CONFLICT (pipeline_run_id) DO UPDATE SET
                    status = 'running',
                    updated_at = NOW()
                """
            ),
            {"run_id": pipeline_run_id, "wm": initial_watermark},
        )


def ensure_checkpoint_run(
    engine: Engine,
    pipeline_run_id: str,
    initial_watermark: datetime,
) -> None:
    """Insert or reset a running checkpoint row for this DAG run."""
    try:
        get_db_circuit_breaker().call(
            lambda: _ensure_run_inner(engine, pipeline_run_id, initial_watermark),
        )
        log.info(
            "ensure_checkpoint_run",
            pipeline_run_id=pipeline_run_id,
            initial_watermark=initial_watermark.isoformat(),
        )
    except Exception as exc:
        log.error(
            "ensure_checkpoint_run",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise CheckpointError("failed to start checkpoint row") from exc


@with_db_retry
def _set_total_chunks_inner(engine: Engine, pipeline_run_id: str, total_chunks: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl.etl_checkpoints
                SET total_chunks = :tc, updated_at = NOW()
                WHERE pipeline_run_id = :run_id
                """
            ),
            {"run_id": pipeline_run_id, "tc": total_chunks},
        )


def set_total_chunks(engine: Engine, pipeline_run_id: str, total_chunks: int) -> None:
    """Persist expected chunk count after extraction."""
    try:
        get_db_circuit_breaker().call(
            lambda: _set_total_chunks_inner(engine, pipeline_run_id, total_chunks),
        )
    except Exception as exc:
        log.error(
            "set_total_chunks",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise CheckpointError("failed to set total_chunks") from exc


@with_db_retry
def _update_after_chunk_inner(
    engine: Engine,
    pipeline_run_id: str,
    chunk_high_watermark: datetime,
    chunks_completed_delta: int,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl.etl_checkpoints
                SET last_watermark = GREATEST(last_watermark, :wm),
                    chunks_completed = chunks_completed + :delta,
                    updated_at = NOW()
                WHERE pipeline_run_id = :run_id
                """
            ),
            {
                "run_id": pipeline_run_id,
                "wm": chunk_high_watermark,
                "delta": chunks_completed_delta,
            },
        )


def update_checkpoint_after_chunk(
    engine: Engine,
    pipeline_run_id: str,
    chunk_high_watermark: datetime,
    chunks_completed_delta: int = 1,
) -> None:
    """Advance watermark after a successful load chunk (timestamp-based, not chunk index)."""
    try:
        get_db_circuit_breaker().call(
            lambda: _update_after_chunk_inner(
                engine,
                pipeline_run_id,
                chunk_high_watermark,
                chunks_completed_delta,
            ),
        )
        log.info(
            "update_checkpoint_after_chunk",
            pipeline_run_id=pipeline_run_id,
            chunk_high_watermark=chunk_high_watermark.isoformat(),
            chunks_completed_delta=chunks_completed_delta,
        )
    except Exception as exc:
        log.error(
            "checkpoint_write_failed",
            operation="update_checkpoint_after_chunk",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise CheckpointError("failed to update checkpoint after chunk") from exc


@with_db_retry
def _finalize_inner(
    engine: Engine,
    pipeline_run_id: str,
    status: str,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl.etl_checkpoints
                SET status = :st, updated_at = NOW()
                WHERE pipeline_run_id = :run_id
                """
            ),
            {"run_id": pipeline_run_id, "st": status},
        )


def finalize_checkpoint_status(
    engine: Engine,
    pipeline_run_id: str,
    status: str,
) -> None:
    """Mark run completed or failed."""
    if status not in {"completed", "failed", "running"}:
        msg = f"invalid checkpoint status: {status}"
        raise ValueError(msg)
    try:
        get_db_circuit_breaker().call(lambda: _finalize_inner(engine, pipeline_run_id, status))
        log.info(
            "finalize_checkpoint_status",
            pipeline_run_id=pipeline_run_id,
            status=status,
        )
    except Exception as exc:
        log.error(
            "checkpoint_write_failed",
            operation="finalize_checkpoint_status",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise CheckpointError("failed to finalize checkpoint") from exc


@with_db_retry
def _insert_dead_letter_inner(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    payload: Mapping[str, Any],
    error_message: str,
) -> None:
    import json

    payload_json = json.dumps(dict(payload), default=str)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl.etl_failed_records (
                    pipeline_run_id, chunk_index, payload, error_message
                )
                VALUES (
                    :run_id, :idx, CAST(:payload AS jsonb), :err
                )
                """
            ),
            {
                "run_id": pipeline_run_id,
                "idx": chunk_index,
                "payload": payload_json,
                "err": error_message[:20000],
            },
        )


def insert_dead_letter(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    payload: Mapping[str, Any],
    error_message: str,
) -> None:
    """Append one invalid record to the dead-letter table (does not stop the pipeline)."""
    try:
        get_db_circuit_breaker().call(
            lambda: _insert_dead_letter_inner(
                engine,
                pipeline_run_id,
                chunk_index,
                payload,
                error_message,
            ),
        )
        log.warning(
            "dead_letter_inserted",
            operation="insert_dead_letter",
            pipeline_run_id=pipeline_run_id,
            chunk_index=chunk_index,
            error=error_message[:500],
        )
    except Exception as exc:
        log.error(
            "dead_letter_failed",
            operation="insert_dead_letter",
            pipeline_run_id=pipeline_run_id,
            chunk_index=chunk_index,
            error=str(exc),
        )
        raise CheckpointError("failed to insert dead letter record") from exc


@with_db_retry
def _clear_buffers_inner(engine: Engine, pipeline_run_id: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM etl.etl_staging WHERE pipeline_run_id = :run_id"),
            {"run_id": pipeline_run_id},
        )
        conn.execute(
            text("DELETE FROM etl.etl_validated WHERE pipeline_run_id = :run_id"),
            {"run_id": pipeline_run_id},
        )


def clear_run_buffers(engine: Engine, pipeline_run_id: str) -> None:
    """Remove staging rows for a run (before re-extract or after finalize)."""
    try:
        get_db_circuit_breaker().call(lambda: _clear_buffers_inner(engine, pipeline_run_id))
    except Exception as exc:
        log.error(
            "staging_cleanup_failed",
            operation="clear_run_buffers",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise CheckpointError("failed to clear staging buffers") from exc


def load_checkpoint_context(engine: Engine, settings: Settings | None = None) -> tuple[datetime, datetime]:
    """Return ``(extract_since, initial_checkpoint_watermark)`` for a new run."""
    cfg = settings or get_settings()
    overlap = timedelta(minutes=cfg.watermark_overlap_minutes)
    last_done = get_last_completed_watermark(engine)
    initial_wm = last_done or _EPOCH
    extract_since = compute_extract_since(last_done, overlap)
    return extract_since, initial_wm


def get_run_table_metrics(engine: Engine, pipeline_run_id: str) -> dict[str, int | str | None]:
    """Return operational table metrics for one run (for observability/reporting)."""
    with engine.connect() as conn:
        staging = conn.execute(
            text("SELECT COUNT(*) FROM etl.etl_staging WHERE pipeline_run_id = :run_id"),
            {"run_id": pipeline_run_id},
        ).scalar_one()
        validated = conn.execute(
            text("SELECT COUNT(*) FROM etl.etl_validated WHERE pipeline_run_id = :run_id"),
            {"run_id": pipeline_run_id},
        ).scalar_one()
        dlq = conn.execute(
            text("SELECT COUNT(*) FROM etl.etl_failed_records WHERE pipeline_run_id = :run_id"),
            {"run_id": pipeline_run_id},
        ).scalar_one()
        checkpoint = conn.execute(
            text(
                """
                SELECT chunks_completed, total_chunks, status
                FROM etl.etl_checkpoints
                WHERE pipeline_run_id = :run_id
                """
            ),
            {"run_id": pipeline_run_id},
        ).mappings().first()

    return {
        "staging_chunks": int(staging or 0),
        "validated_chunks": int(validated or 0),
        "dead_letter_rows": int(dlq or 0),
        "chunks_completed": int((checkpoint or {}).get("chunks_completed") or 0),
        "total_chunks": int((checkpoint or {}).get("total_chunks") or 0),
        "checkpoint_status": (checkpoint or {}).get("status"),
    }
