"""Incremental extract from PostgreSQL into ``etl_staging`` (chunked, parallel inserts)."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Semaphore
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.utils.config import Settings, get_settings
from pipeline.utils.db import get_source_engine, get_target_engine
from pipeline.utils.exceptions import ExtractionError
from pipeline.utils.log import configure_logging, get_logger
from pipeline.infrastructure.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.infrastructure.resilience.checkpoint import (
    clear_run_buffers,
    ensure_checkpoint_run,
    load_checkpoint_context,
    set_total_chunks,
)
from pipeline.infrastructure.resilience.retry import with_db_retry
from pipeline.utils.serialize import records_json_dumps

log = get_logger(__name__)

_EXTRACT_SQL = """
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
WHERE updated_at > :extract_since
ORDER BY updated_at ASC
"""


def _rows_to_polars(
    engine: Engine,
    extract_since: datetime,
) -> pl.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(
            text(_EXTRACT_SQL),
            {"extract_since": extract_since},
        )
        rows = [dict(m) for m in result.mappings().all()]
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


@with_db_retry
def _insert_staging_chunk(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    records: list[dict[str, Any]],
) -> None:
    payload = records_json_dumps(records)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl.etl_staging (pipeline_run_id, chunk_index, records)
                VALUES (:run_id, :idx, CAST(:payload AS jsonb))
                """
            ),
            {"run_id": pipeline_run_id, "idx": chunk_index, "payload": payload},
        )


def _insert_staging_guarded(
    engine: Engine,
    pipeline_run_id: str,
    chunk_index: int,
    records: list[dict[str, Any]],
) -> None:
    get_db_circuit_breaker().call(
        lambda: _insert_staging_chunk(engine, pipeline_run_id, chunk_index, records),
    )


def _chunk_worker(
    args: tuple[Engine, str, int, list[dict[str, Any]], Semaphore],
) -> tuple[int, int]:
    engine, run_id, idx, recs, sem = args
    with sem:
        t0 = time.perf_counter()
        _insert_staging_guarded(engine, run_id, idx, recs)
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return idx, elapsed_ms


def run_extraction(pipeline_run_id: str, settings: Settings | None = None) -> dict[str, Any]:
    """Read transactions newer than watermark (with overlap), stage chunks in parallel."""
    configure_logging()

    cfg = settings or get_settings()
    source_engine = get_source_engine(cfg)
    target_engine = get_target_engine(cfg)
    t0 = time.perf_counter()

    try:
        extract_since, initial_wm = load_checkpoint_context(target_engine, cfg)
        clear_run_buffers(target_engine, pipeline_run_id)
        ensure_checkpoint_run(target_engine, pipeline_run_id, initial_wm)

        log.info(
            "run_extraction",
            pipeline_run_id=pipeline_run_id,
            extract_since=extract_since.isoformat(),
            chunk_size=cfg.chunk_size,
        )

        df = _rows_to_polars(source_engine, extract_since)
        row_dicts = df.to_dicts() if len(df) > 0 else []
        chunk_size = cfg.chunk_size
        chunks: list[tuple[int, list[dict[str, Any]]]] = []
        for i in range(0, len(row_dicts), chunk_size):
            chunks.append((len(chunks), row_dicts[i : i + chunk_size]))

        max_workers = cfg.resolved_max_workers()
        semaphore = Semaphore(max_workers)
        workers = max(1, min(max_workers, len(chunks) or 1))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    _chunk_worker,
                    (target_engine, pipeline_run_id, idx, recs, semaphore),
                )
                for idx, recs in chunks
            ]
            for fut in as_completed(futures):
                idx, elapsed_ms = fut.result()
                log.info(
                    "staging_insert",
                    pipeline_run_id=pipeline_run_id,
                    chunk_index=idx,
                    duracion_ms=elapsed_ms,
                )

        total_chunks = len(chunks)
        set_total_chunks(target_engine, pipeline_run_id, total_chunks)

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        log.info(
            "run_extraction",
            pipeline_run_id=pipeline_run_id,
            chunk_count=total_chunks,
            registros=len(row_dicts),
            duracion_ms=elapsed_ms,
        )

        return {
            "pipeline_run_id": pipeline_run_id,
            "chunk_count": total_chunks,
            "extract_since": extract_since.isoformat(),
            "row_count": len(row_dicts),
        }
    except Exception as exc:
        log.error(
            "run_extraction",
            pipeline_run_id=pipeline_run_id,
            error=str(exc),
        )
        raise ExtractionError("extraction failed") from exc
