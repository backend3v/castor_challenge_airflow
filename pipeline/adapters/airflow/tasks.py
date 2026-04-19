"""Task adapter functions used by DAG definitions."""

from __future__ import annotations

from typing import Any

from pipeline.application.services import (
    get_run_table_metrics,
    run_extraction,
    run_finalize,
    run_load,
    run_smoke_transfer,
    run_validation,
)


def extract_task_handler(run_id: str) -> dict[str, Any]:
    return run_extraction(run_id)


def validate_task_handler(extract_meta: dict[str, Any]) -> dict[str, Any]:
    return run_validation(extract_meta["pipeline_run_id"])


def load_task_handler(validation_meta: dict[str, Any]) -> dict[str, Any]:
    out = run_load(validation_meta["pipeline_run_id"])
    out["dlq_count"] = validation_meta.get("dlq_count", 0)
    return out


def report_tables_task_handler(load_meta: dict[str, Any]) -> dict[str, Any]:
    run_id = load_meta["pipeline_run_id"]
    metrics = get_run_table_metrics(run_id)
    out = dict(load_meta)
    out["table_metrics"] = metrics
    return out


def finalize_task_handler(load_meta: dict[str, Any]) -> dict[str, Any]:
    return run_finalize(load_meta["pipeline_run_id"], load_meta)


def smoke_transfer_handler(limit: int = 1000) -> dict[str, int]:
    return run_smoke_transfer(limit=limit)
