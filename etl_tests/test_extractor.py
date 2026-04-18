"""Tests for ``pipeline.extractor``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl

from pipeline.config import get_settings


@patch("pipeline.extractor.set_total_chunks")
@patch("pipeline.extractor.ensure_checkpoint_run")
@patch("pipeline.extractor.clear_run_buffers")
@patch("pipeline.extractor.load_checkpoint_context")
@patch("pipeline.extractor._rows_to_polars")
def test_run_extraction_cuando_dataframe_vacio_entonces_cero_chunks(
    mock_rows: MagicMock,
    mock_ctx: MagicMock,
    mock_clear: MagicMock,
    mock_ensure: MagicMock,
    mock_total: MagicMock,
) -> None:
    from datetime import datetime

    mock_ctx.return_value = (datetime(2020, 1, 1), datetime(1970, 1, 1))
    mock_rows.return_value = pl.DataFrame()
    from pipeline.extractor import run_extraction

    out = run_extraction("run-test", settings=get_settings())
    assert out["chunk_count"] == 0
    assert out["row_count"] == 0
