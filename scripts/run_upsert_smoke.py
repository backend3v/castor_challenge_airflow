#!/usr/bin/env python3
"""Prueba de humo: bulk upsert idempotente contra ``financial_records``.

Ejecutar desde la raíz del repositorio con variables cargadas::

    set -a && source .env && set +a
    export PYTHONPATH=.
    python scripts/run_upsert_smoke.py

O: ``./scripts/smoke_upsert_flow.sh --run``
"""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

# Raíz del repo en PYTHONPATH
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv

    load_dotenv(_REPO / ".env")
except ImportError:
    pass

from sqlalchemy import text

from pipeline.db import get_etl_engine
from pipeline.loader import _row_to_bind, _upsert_chunk


def main() -> None:
    engine = get_etl_engine()
    tid = uuid4()
    aid = uuid4()
    now = datetime.now().replace(microsecond=0)

    base = {
        "transaction_id": str(tid),
        "account_id": str(aid),
        "category_id": 1,
        "amount": "100.00",
        "currency": "USD",
        "transaction_type": "debit",
        "status": "completed",
        "description": "smoke_bulk_upsert",
        "created_at": now,
        "updated_at": now,
    }
    row_v1 = _row_to_bind(base)

    print("1) Primer bulk upsert (1 fila), amount=100.00")
    _upsert_chunk(engine, [row_v1])

    with engine.connect() as conn:
        amt = conn.execute(
            text("SELECT amount FROM financial_records WHERE transaction_id = :id"),
            {"id": tid},
        ).scalar_one()
    print(f"   Leído: amount={amt}")

    base["amount"] = "200.50"
    base["updated_at"] = now
    row_v2 = _row_to_bind(base)

    print("2) Segundo bulk upsert (misma PK), amount=200.50 — debe actualizar (ON CONFLICT)")
    _upsert_chunk(engine, [row_v2])

    with engine.connect() as conn:
        amt2 = conn.execute(
            text("SELECT amount FROM financial_records WHERE transaction_id = :id"),
            {"id": tid},
        ).scalar_one()
    assert Decimal(str(amt2)) == Decimal("200.50"), (amt2, "expected 200.50 after upsert")

    print(f"   OK: amount={amt2} (idempotente / upsert)")
    print("Smoke completado.")


if __name__ == "__main__":
    main()
