# Guia de ejecucion de pruebas

Pruebas minimas del proyecto: solo flujo ETL y servicios.

## 1) Que se prueba

- Flujo del DAG completo (`extract -> validate -> load -> finalize`).
- Servicios base de conexion (URL ETL correcta y `NullPool` en SQLAlchemy).

## 2) Archivos de prueba activos

- `tests/test_dag_pipeline_flow.py`
  - Verifica que el DAG tenga las tareas y dependencias correctas.
  - Verifica que en ejecucion se propaguen metadatos entre tareas.

- `tests/test_services.py`
  - Verifica que la URL de conexion ETL se construya correctamente con host/port.
  - Verifica que el engine use `NullPool` (pooling delegado a PgBouncer).

## 3) Como ejecutar

1. Instalar dependencias:
   - `pip install -r requirements.txt`
2. Ejecutar pruebas:
   - `pytest tests/ -q`

## 4) Prueba de servicios en entorno real (opcional)

Para validar servicios levantados:

1. `docker compose down -v && docker compose up -d postgres pgbouncer`

2. `set -a && source .env && set +a`

3. `export PYTHONPATH=.`

4. `python scripts/run_upsert_smoke.py`

Si en Airflow no aparece el DAG, revisar el mount `./dags:/opt/airflow/dags` en `docker-compose.yaml` y `TARGET_POSTGRES_DB=castor_challenge_etl` en `.env`.

## 5) Criterio de aceptacion

- `pytest tests/ -q` finaliza en verde.
- El smoke script termina sin error y confirma upsert idempotente.
