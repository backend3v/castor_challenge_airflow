"""Application services (hexagonal entrypoints)."""

from __future__ import annotations

from typing import Any

from pipeline.utils.config import Settings, get_settings
from pipeline.infrastructure.legacy import etl_workflow


def run_extraction(pipeline_run_id: str, settings: Settings | None = None) -> dict[str, Any]:
    return etl_workflow.run_extraction(pipeline_run_id, settings or get_settings())


def run_validation(pipeline_run_id: str, settings: Settings | None = None) -> dict[str, Any]:
    return etl_workflow.run_validation(pipeline_run_id, settings or get_settings())


def run_load(pipeline_run_id: str, settings: Settings | None = None) -> dict[str, Any]:
    return etl_workflow.run_load(pipeline_run_id, settings or get_settings())


def run_finalize(
    pipeline_run_id: str,
    summary: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    return etl_workflow.run_finalize(pipeline_run_id, summary, settings or get_settings())


def get_run_table_metrics(pipeline_run_id: str, settings: Settings | None = None) -> dict[str, int | str | None]:
    return etl_workflow.get_run_table_metrics(pipeline_run_id, settings or get_settings())


def run_smoke_transfer(limit: int = 1000, settings: Settings | None = None) -> dict[str, int]:
    return etl_workflow.run_smoke_transfer(limit, settings or get_settings())
