"""Smoke DAG rapido: transfiere una muestra de source hacia target."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="financial_etl_smoke",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "financial", "smoke"],
    doc_md=__doc__,
)
def financial_etl_smoke_dag() -> None:
    @task(task_id="transfer_sample")
    def transfer_sample(limit: int = 1000) -> dict[str, int]:
        from pipeline.adapters.airflow.tasks import smoke_transfer_handler

        return smoke_transfer_handler(limit=limit)

    transfer_sample()


dag = financial_etl_smoke_dag()
