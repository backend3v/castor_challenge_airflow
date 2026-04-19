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
        from pipeline.adapters.airflow.tasks import extract_task_handler

        ctx = get_current_context()
        run_id = str(ctx["run_id"])
        return extract_task_handler(run_id)

    @task(task_id="validate")
    def validate_task(extract_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.adapters.airflow.tasks import validate_task_handler

        return validate_task_handler(extract_meta)

    @task(task_id="load")
    def load_task(validation_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.adapters.airflow.tasks import load_task_handler

        return load_task_handler(validation_meta)

    @task(task_id="finalize")
    def finalize_task(load_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.adapters.airflow.tasks import finalize_task_handler

        return finalize_task_handler(load_meta)

    @task(task_id="report_tables")
    def report_tables_task(load_meta: dict[str, Any]) -> dict[str, Any]:
        from pipeline.adapters.airflow.tasks import report_tables_task_handler

        return report_tables_task_handler(load_meta)

    ext = extract_task()
    val = validate_task(ext)
    ld = load_task(val)
    rpt = report_tables_task(ld)
    finalize_task(rpt)

dag = financial_etl_dag()
