"""Financial ETL DAG — extract → validate → load → finalize (TaskFlow)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="financial_etl",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "financial", "incremental"],
    doc_md=__doc__,
)
def financial_etl_dag() -> None:
    @task(task_id="extract")
    def extract_task() -> dict[str, Any]:
        from pipeline.extractor import run_extraction

        ctx = get_current_context()
        run_id = str(ctx["run_id"])
        return run_extraction(run_id)

    @task(task_id="validate")
    def validate_task(extract_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.transformer import run_validation

        return run_validation(extract_meta["pipeline_run_id"])

    @task(task_id="load")
    def load_task(validation_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.loader import run_load

        out = run_load(validation_meta["pipeline_run_id"])
        out["dlq_count"] = validation_meta.get("dlq_count", 0)
        return out

    @task(task_id="finalize")
    def finalize_task(load_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.finalization import run_finalize

        return run_finalize(load_meta["pipeline_run_id"], load_meta)

    ext = extract_task()
    val = validate_task(ext)
    ld = load_task(val)
    finalize_task(ld)


dag = financial_etl_dag()
