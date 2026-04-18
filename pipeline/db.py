"""SQLAlchemy engine factory (NullPool — pooling handled by PgBouncer)."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from pipeline.config import Settings, get_settings


def get_etl_engine(settings: Settings | None = None) -> Engine:
    """Create a sync engine through PgBouncer without client-side pooling."""
    cfg = settings or get_settings()
    return create_engine(
        cfg.etl_database_url,
        poolclass=NullPool,
        future=True,
    )
