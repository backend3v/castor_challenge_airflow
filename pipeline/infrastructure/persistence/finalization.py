"""Mark run completed and drop staging buffers."""

from __future__ import annotations

from typing import Any

from pipeline.utils.config import Settings, get_settings
from pipeline.utils.db import get_etl_engine
from pipeline.utils.log import configure_logging, get_logger
from pipeline.infrastructure.resilience.checkpoint import clear_run_buffers, finalize_checkpoint_status

log = get_logger(__name__)


def run_finalize(
    pipeline_run_id: str,
    summary: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Set checkpoint to ``completed`` and clear ``etl_staging`` / ``etl_validated``."""
    configure_logging()
    cfg = settings or get_settings()
    engine = get_etl_engine(cfg)
    finalize_checkpoint_status(engine, pipeline_run_id, "completed")
    clear_run_buffers(engine, pipeline_run_id)
    dlq = (summary or {}).get("dlq_count", 0)
    log.info(
        "pipeline_finalizado",
        operation="run_finalize",
        pipeline_run_id=pipeline_run_id,
        registros_dlq=dlq,
    )
    return {"pipeline_run_id": pipeline_run_id, "status": "completed", "dlq_count": dlq}
