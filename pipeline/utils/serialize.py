"""JSON serialization helpers for staging payloads (UUID, Decimal, datetime)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


def _json_default(value: object) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


def records_json_dumps(records: list[dict[str, Any]]) -> str:
    """Serialize a list of row dicts for ``JSONB`` columns."""
    return json.dumps(records, default=_json_default)
