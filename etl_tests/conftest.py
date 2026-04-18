"""Shared pytest fixtures."""

from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("POSTGRES_DB", "test")

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest


@pytest.fixture
def sample_transaction_row() -> dict:
    """Minimal valid transaction-like dict for Pydantic / loader tests."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    return {
        "transaction_id": str(uuid4()),
        "account_id": str(uuid4()),
        "category_id": 1,
        "amount": Decimal("10.50"),
        "currency": "USD",
        "transaction_type": "debit",
        "status": "completed",
        "description": "test",
        "created_at": now,
        "updated_at": now,
    }


@pytest.fixture
def mock_engine() -> MagicMock:
    """Placeholder engine for unit tests that patch SQL execution."""
    return MagicMock(name="engine")


@pytest.fixture(autouse=True)
def clear_settings_cache() -> None:
    """Isolate ``get_settings()`` between tests."""
    from pipeline.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
