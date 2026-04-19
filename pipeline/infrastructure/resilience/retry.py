"""Tenacity retry with random exponential backoff for transient DB errors."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)
from tenacity.before_sleep import before_sleep_log

from pipeline.utils.config import get_settings

P = ParamSpec("P")
R = TypeVar("R")

_retry_logger = logging.getLogger("pipeline.retry")


def is_retryable_db_error(exc: BaseException) -> bool:
    """Return True only for connection / DBAPI failures (not logic errors)."""
    if isinstance(exc, OperationalError):
        return True
    if isinstance(exc, DBAPIError) and getattr(exc, "connection_invalidated", False):
        return True
    return False


def with_db_retry(fn: Callable[P, R]) -> Callable[P, R]:
    """Decorator: full jitter, max attempts from settings, retry only DB/network-like errors."""
    settings = get_settings()
    return retry(
        wait=wait_random_exponential(
            multiplier=1,
            max=int(settings.retry_wait_max_seconds),
        ),
        stop=stop_after_attempt(settings.retry_max_attempts),
        retry=retry_if_exception(is_retryable_db_error),
        before_sleep=before_sleep_log(_retry_logger, logging.WARNING),
        reraise=True,
    )(fn)
