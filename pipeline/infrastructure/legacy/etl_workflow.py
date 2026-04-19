"""Infrastructure implementation delegating to existing ETL modules."""

from __future__ import annotations

from typing import Any

from pipeline.utils.config import Settings
from pipeline.utils.db import get_target_engine
from pipeline.infrastructure.persistence.extractor import run_extraction as _run_extraction
from pipeline.infrastructure.persistence.finalization import run_finalize as _run_finalize
from pipeline.infrastructure.persistence.loader import run_load as _run_load
from pipeline.infrastructure.resilience.checkpoint import get_run_table_metrics as _get_run_table_metrics
from pipeline.infrastructure.persistence.smoke_transfer import run_smoke_transfer as _run_smoke_transfer
from pipeline.infrastructure.persistence.transformer import run_validation as _run_validation


def run_extraction(pipeline_run_id: str, settings: Settings) -> dict[str, Any]:
    return _run_extraction(pipeline_run_id, settings)


def run_validation(pipeline_run_id: str, settings: Settings) -> dict[str, Any]:
    return _run_validation(pipeline_run_id, settings)


def run_load(pipeline_run_id: str, settings: Settings) -> dict[str, Any]:
    return _run_load(pipeline_run_id, settings)


def run_finalize(
    pipeline_run_id: str,
    summary: dict[str, Any] | None,
    settings: Settings,
) -> dict[str, Any]:
    return _run_finalize(pipeline_run_id, summary, settings)


def get_run_table_metrics(pipeline_run_id: str, settings: Settings) -> dict[str, int | str | None]:
    return _get_run_table_metrics(get_target_engine(settings), pipeline_run_id)


def run_smoke_transfer(limit: int, settings: Settings) -> dict[str, int]:
    _ = settings
    return _run_smoke_transfer(limit=limit)
