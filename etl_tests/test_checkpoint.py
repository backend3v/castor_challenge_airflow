"""Tests for ``pipeline.resilience.checkpoint``."""

from __future__ import annotations

from datetime import datetime, timedelta

from pipeline.resilience.checkpoint import compute_extract_since


def test_compute_extract_since_cuando_sin_historial_entonces_epoch_menos_overlap() -> None:
    since = compute_extract_since(None, timedelta(minutes=5))
    assert since.year == 1969 or since.year == 1970


def test_compute_extract_since_cuando_hay_watermark_entonces_resta_overlap() -> None:
    base = datetime(2026, 6, 15, 12, 0, 0)
    since = compute_extract_since(base, timedelta(minutes=5))
    assert since == datetime(2026, 6, 15, 11, 55, 0)
