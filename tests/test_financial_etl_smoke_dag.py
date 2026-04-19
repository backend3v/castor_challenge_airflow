"""Prueba unica del DAG smoke (se ejecuta aparte de tests/)."""

from __future__ import annotations

from unittest.mock import patch

from dags.financial_etl_smoke_dag import financial_etl_smoke_dag


def test_smoke_dag_transfiere_source_a_target() -> None:
    dag = financial_etl_smoke_dag()

    assert dag.dag_id == "financial_etl_smoke"
    assert set(dag.task_ids) == {"transfer_sample"}

    task = dag.get_task("transfer_sample")
    with patch(
        "pipeline.adapters.airflow.tasks.smoke_transfer_handler",
        return_value={"rows_read": 25, "rows_loaded": 25},
    ) as mocked_transfer:
        out = task.python_callable()

    mocked_transfer.assert_called_once_with(limit=1000)
    assert out["rows_read"] == 25
    assert out["rows_loaded"] == 25
