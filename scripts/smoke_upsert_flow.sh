#!/usr/bin/env bash
# Flujo de prueba manual: bulk upsert + comprobación en PostgreSQL.
# Uso (desde la raíz del repo):
#   chmod +x scripts/smoke_upsert_flow.sh
#   ./scripts/smoke_upsert_flow.sh
#
# Requisitos: Docker, `.env` con POSTGRES_* y ETL_DB_* si pruebas vía PgBouncer.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== 1) Levantar Postgres (y opcionalmente PgBouncer) =="
echo "    docker compose up -d postgres pgbouncer"
echo ""

echo "== 2) Asegurar esquema aplicado (initdb solo en volumen vacío) =="
echo "    Si la BD ya existía sin las tablas nuevas, recrea el volumen o aplica migraciones."
echo ""

echo "== 3) Cargar variables y ejecutar smoke Python (mismo proceso que el ETL) =="
echo "    set -a && source .env && set +a"
echo "    export PYTHONPATH=\"\$ROOT\""
echo "    python scripts/run_upsert_smoke.py"
echo ""

echo "== 4) Verificar en SQL (opcional) =="
echo "    docker compose exec postgres psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \\"
echo "      \"SELECT transaction_id, amount, loaded_at FROM financial_records ORDER BY loaded_at DESC LIMIT 5;\""
echo ""

if [[ "${1:-}" == "--run" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.env"
  set +a
  export PYTHONPATH="${ROOT}"
  exec python "${ROOT}/scripts/run_upsert_smoke.py"
fi
