"""Structlog setup (call ``configure_logging()`` before first log in worker processes)."""

from __future__ import annotations

import logging
import os
import sys

import structlog

_configured: bool = False


def configure_logging() -> None:
    """Configure structlog once per process (JSON in Docker when ``ETL_LOG_FORMAT=json``)."""
    global _configured
    if _configured:
        return

    level_name = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    fmt = os.getenv("ETL_LOG_FORMAT", "console").lower()

    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    if fmt == "json":
        processors: list[structlog.types.Processor] = [
            *shared,
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            *shared,
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty()),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )
    _configured = True


def get_logger(name: str):
    """Return a structlog logger, ensuring configuration ran."""
    configure_logging()
    return structlog.get_logger(name)
