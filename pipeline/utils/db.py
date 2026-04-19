"""SQLAlchemy engine factories (source + target, both using NullPool)."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from pipeline.utils.config import Settings, get_settings


def get_source_engine(settings: Settings | None = None) -> Engine:
    """Create source DB engine without client-side pooling."""
    cfg = settings or get_settings()
    return create_engine(
        cfg.source_database_url,
        poolclass=NullPool,
        future=True,
    )


def get_target_engine(settings: Settings | None = None) -> Engine:
    """Create target ETL/reporting DB engine through PgBouncer."""
    cfg = settings or get_settings()
    return create_engine(
        cfg.target_database_url,
        poolclass=NullPool,
        future=True,
    )


def get_etl_engine(settings: Settings | None = None) -> Engine:
    """Backward-compatible alias for target engine."""
    return get_target_engine(settings)
