"""Pruebas del DAG financiero y su contrato de orquestacion."""

from __future__ import annotations

from unittest.mock import patch

from dags.financial_etl_dag import financial_etl_dag


def test_dag_dependencies() -> None:
    dag = financial_etl_dag()

    assert dag.dag_id == "financial_etl"
    # Keep this test stable across Airflow versions; schedule internals vary by release.
    assert dag.timetable is not None
    assert dag.catchup is False
    assert "incremental" in dag.tags
    assert set(dag.task_ids) == {"extract", "validate", "load", "report_tables", "finalize"}

    extract_downstream = dag.get_task("extract").downstream_task_ids
    validate_downstream = dag.get_task("validate").downstream_task_ids
    load_downstream = dag.get_task("load").downstream_task_ids
    report_downstream = dag.get_task("report_tables").downstream_task_ids

    assert extract_downstream == {"validate"}
    assert validate_downstream == {"load"}
    assert load_downstream == {"report_tables"}
    assert report_downstream == {"finalize"}


def test_dag() -> None:
    dag = financial_etl_dag()
    extract_task = dag.get_task("extract")
    validate_task = dag.get_task("validate")
    load_task = dag.get_task("load")
    report_task = dag.get_task("report_tables")
    finalize_task = dag.get_task("finalize")

    fake_ctx = {"run_id": "manual__2026-04-18T00:00:00+00:00"}
    extract_meta = {
        "pipeline_run_id": fake_ctx["run_id"],
        "chunk_count": 2,
        "row_count": 10,
        "extract_since": "2026-04-17T23:55:00",
    }
    validate_meta = {
        "pipeline_run_id": fake_ctx["run_id"],
        "dlq_count": 1,
        "chunks_validated": 2,
    }
    load_meta = {
        "pipeline_run_id": fake_ctx["run_id"],
        "records_loaded": 9,
        "chunks_processed": 2,
    }
    report_meta = {
        **load_meta,
        "table_metrics": {
            "staging_chunks": 2,
            "validated_chunks": 2,
            "dead_letter_rows": 1,
            "chunks_completed": 2,
            "total_chunks": 2,
            "checkpoint_status": "running",
        },
    }
    finalize_meta = {
        "pipeline_run_id": fake_ctx["run_id"],
        "status": "completed",
        "dlq_count": 1,
    }

    with (
        patch("dags.financial_etl_dag.get_current_context", return_value=fake_ctx),
        patch("pipeline.adapters.airflow.tasks.extract_task_handler", return_value=extract_meta) as mock_extract,
        patch("pipeline.adapters.airflow.tasks.validate_task_handler", return_value=validate_meta) as mock_validate,
        patch("pipeline.adapters.airflow.tasks.load_task_handler", return_value=load_meta) as mock_load,
        patch("pipeline.adapters.airflow.tasks.report_tables_task_handler", return_value=report_meta),
        patch("pipeline.adapters.airflow.tasks.finalize_task_handler", return_value=finalize_meta) as mock_finalize,
    ):
        out_extract = extract_task.python_callable()
        out_validate = validate_task.python_callable(out_extract)
        out_load = load_task.python_callable(out_validate)
        out_report = report_task.python_callable(out_load)
        out_finalize = finalize_task.python_callable(out_report)

    mock_extract.assert_called_once_with(fake_ctx["run_id"])
    mock_validate.assert_called_once_with(out_extract)
    mock_load.assert_called_once_with(out_validate)
    mock_finalize.assert_called_once_with(out_report)

    assert out_extract["pipeline_run_id"] == fake_ctx["run_id"]
    assert out_validate["dlq_count"] == 1
    assert out_load["dlq_count"] == 1
    assert out_report["table_metrics"]["staging_chunks"] == 2
    assert out_finalize["status"] == "completed"

