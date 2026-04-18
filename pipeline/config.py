"""Central settings and hardware-aware defaults for the ETL pipeline."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Final
from urllib.parse import quote_plus

import psutil
from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEFAULT_CHUNK_SIZE: Final[int] = 5000
_DEFAULT_OVERLAP_MINUTES: Final[int] = 5


def compute_max_workers() -> int:
    """Derive a safe thread count from CPU cores and available RAM (see project plan)."""
    cpu_cores: int = os.cpu_count() or 1
    ram_gb: float = psutil.virtual_memory().available / 1e9
    return max(1, min(cpu_cores, int(ram_gb / 0.5)))


class Settings(BaseSettings):
    """Application settings loaded from environment (``.env`` when present)."""

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- PostgreSQL (shared with Airflow / seed); env: POSTGRES_* ---
    postgres_user: str
    postgres_password: str
    postgres_db: str

    # --- ETL connection (PgBouncer); env: ETL_DB_HOST, ETL_DB_PORT ---
    etl_db_host: str = "localhost"
    etl_db_port: int = 6432

    # --- Extraction / checkpoint ---
    chunk_size: int = _DEFAULT_CHUNK_SIZE
    watermark_overlap_minutes: int = _DEFAULT_OVERLAP_MINUTES

    # --- Retry (tenacity); .env uses RETRY_WAIT_MIN / RETRY_WAIT_MAX ---
    retry_max_attempts: int = 5
    retry_wait_min_seconds: float = Field(
        default=1.0,
        validation_alias=AliasChoices("RETRY_WAIT_MIN", "RETRY_WAIT_MIN_SECONDS"),
    )
    retry_wait_max_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices("RETRY_WAIT_MAX", "RETRY_WAIT_MAX_SECONDS"),
    )

    # --- Circuit breaker (pybreaker); optional env overrides ---
    circuit_breaker_fail_max: int = Field(default=3, validation_alias="CIRCUIT_FAIL_MAX")
    circuit_breaker_reset_timeout_seconds: int = Field(
        default=60,
        validation_alias="CIRCUIT_RESET_TIMEOUT_SECONDS",
    )

    # --- Parallelism: optional hard cap; otherwise psutil-based compute ---
    max_workers_override: int | None = Field(default=None, validation_alias="MAX_WORKERS")

    @field_validator("chunk_size")
    @classmethod
    def chunk_size_positive(cls, value: int) -> int:
        if value < 1:
            msg = "chunk_size must be >= 1"
            raise ValueError(msg)
        return value

    @field_validator("watermark_overlap_minutes")
    @classmethod
    def overlap_non_negative(cls, value: int) -> int:
        if value < 0:
            msg = "watermark_overlap_minutes must be >= 0"
            raise ValueError(msg)
        return value

    @field_validator("max_workers_override", mode="before")
    @classmethod
    def empty_max_workers_as_none(cls, value: object) -> int | None:
        if value is None or value == "":
            return None
        return int(value)

    def resolved_max_workers(self) -> int:
        """Return ``MAX_WORKERS`` if set, else hardware-derived parallelism."""
        if self.max_workers_override is not None:
            return max(1, self.max_workers_override)
        return compute_max_workers()

    @property
    def etl_database_url(self) -> str:
        """SQLAlchemy sync URL through PgBouncer (``NullPool`` recommended in loader)."""
        user = quote_plus(self.postgres_user)
        password = quote_plus(self.postgres_password)
        db = quote_plus(self.postgres_db)
        return (
            f"postgresql+psycopg2://{user}:{password}"
            f"@{self.etl_db_host}:{self.etl_db_port}/{db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return process-wide settings (cached)."""
    return Settings()
