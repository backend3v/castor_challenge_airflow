"""Pruebas minimas de servicios del sistema."""

from __future__ import annotations

import os

from sqlalchemy.pool import NullPool

from pipeline.utils.config import get_settings
from pipeline.utils.db import get_source_engine, get_target_engine


def _set_base_env() -> None:
    os.environ.setdefault("POSTGRES_USER", "test")
    os.environ.setdefault("POSTGRES_PASSWORD", "test")
    os.environ.setdefault("POSTGRES_DB", "castor_challenge")
    os.environ.setdefault("TARGET_POSTGRES_DB", "castor_challenge_etl")


def test_services_settings_db_url() -> None:
    _set_base_env()
    os.environ["SOURCE_DB_HOST"] = "postgres"
    os.environ["SOURCE_DB_PORT"] = "5432"
    os.environ["TARGET_DB_HOST"] = "pgbouncer"
    os.environ["TARGET_DB_PORT"] = "5432"
    get_settings.cache_clear()

    s = get_settings()
    assert "postgres:5432/castor_challenge" in s.source_database_url
    assert "pgbouncer:5432/castor_challenge_etl" in s.target_database_url


def test_services_engine_usa_nullpool() -> None:
    _set_base_env()
    get_settings.cache_clear()

    source_engine = get_source_engine()
    target_engine = get_target_engine()
    assert isinstance(source_engine.pool, NullPool)
    assert isinstance(target_engine.pool, NullPool)

