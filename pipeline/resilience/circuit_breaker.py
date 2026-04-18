"""Shared circuit breaker for PostgreSQL access (pybreaker)."""

from __future__ import annotations

import pybreaker

from pipeline.config import get_settings

_breaker: pybreaker.CircuitBreaker | None = None


def get_db_circuit_breaker() -> pybreaker.CircuitBreaker:
    """Lazy singleton so tests can configure env before first use."""
    global _breaker
    if _breaker is None:
        s = get_settings()
        _breaker = pybreaker.CircuitBreaker(
            fail_max=s.circuit_breaker_fail_max,
            reset_timeout=s.circuit_breaker_reset_timeout_seconds,
            name="postgres_etl",
        )
    return _breaker
