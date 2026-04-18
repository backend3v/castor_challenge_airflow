"""Tests for retry / circuit breaker wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.exc import OperationalError

from pipeline.resilience.circuit_breaker import get_db_circuit_breaker
from pipeline.resilience.retry import is_retryable_db_error


def test_is_retryable_db_error_cuando_operational_entonces_true() -> None:
    exc = OperationalError("stmt", None, None)
    assert is_retryable_db_error(exc) is True


def test_is_retryable_db_error_cuando_valueerror_entonces_false() -> None:
    assert is_retryable_db_error(ValueError("x")) is False


def test_circuit_breaker_callable_ejecuta_fn() -> None:
    cb = get_db_circuit_breaker()
    mock = MagicMock(return_value=42)
    assert cb.call(mock) == 42
    mock.assert_called_once()
