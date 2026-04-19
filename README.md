# Castor Challenge Airflow

Pipeline ETL financiero para la prueba tecnica de Senior Backend Engineer (Airflow) en Castor.
El sistema extrae datos de transacciones, valida su calidad y los carga en PostgreSQL de forma incremental, orquestado con Apache Airflow.

Arquitectura actual de datos:
- `castor_challenge` (fuente): `public.accounts`, `public.categories`, `public.transactions`
- `castor_challenge_etl` (operacion ETL + reporteria):
  - `dag.*` metadatos de Airflow
  - `etl.*` checkpoint, staging, validated, dead letter
  - `public.financial_records` destino de reporteria

## Tablas y objetivo (directo)

- `castor_challenge.public.transactions`: origen que se extrae.
- `castor_challenge_etl.public.financial_records`: destino final para reporteria.
- `castor_challenge_etl.etl.etl_checkpoints`: estado de corrida y watermark para retomar.
- `castor_challenge_etl.etl.etl_staging`: buffer temporal entre extract y validate.
- `castor_challenge_etl.etl.etl_validated`: buffer temporal entre validate y load.
- `castor_challenge_etl.etl.etl_failed_records`: registros invalidos (dead letter).
- `castor_challenge_etl.dag.*`: tablas internas de Airflow (orquestacion).

El DAG `financial_etl` ahora incluye `report_tables` para registrar en logs el uso real de `etl_staging`, `etl_validated`, `etl_failed_records` y checkpoint por `pipeline_run_id`.

## 1) Descripcion del proyecto segun Plan + Prueba

La prueba pide construir un backend ETL robusto, eficiente y con resiliencia para mover datos financieros desde una fuente legada hacia PostgreSQL para analitica.

Este proyecto lo aborda con un flujo por etapas:

- **Extraccion incremental:** toma solo registros nuevos o actualizados usando `updated_at` como watermark.
- **Validacion de esquema:** revisa cada registro antes de cargarlo.
- **Carga optimizada:** inserta en lotes para reducir carga sobre base de datos.
- **Resiliencia operativa:** aplica retry con backoff y protecciones ante fallos transitorios.
- **Orquestacion:** ejecuta todo como DAG de Airflow.

## 2) Como se abordaron los items de la prueba

### Parte 1: Desarrollo del pipeline

- **Extraccion incremental (requerido):** se calcula `extract_since` desde checkpoint (`etl_checkpoints`) y se aplica una ventana de solapamiento de 5 minutos para no perder actualizaciones cercanas al corte.
- **Chunking y paralelismo controlado (requerido):** la extraccion divide en lotes (`chunk_size`, por defecto 5000) y usa `ThreadPoolExecutor` con `Semaphore`.
- **Manejo de errores con retries y logs (requerido):** operaciones de BD usan `tenacity` con `wait_random_exponential` y logs estructurados con `structlog`.

### Parte 2: Calidad y pruebas

- **Validacion de esquema (requerido):** `Pydantic v2` valida cada fila contra el modelo de `FinancialRecord`.
- **Registros invalidos:** no detienen el pipeline; se envian a `etl_failed_records` (dead letter table) para auditoria.
- **Testing con pytest (simplificado):** pruebas minimas de flujo (DAG) y servicios (configuracion/engine), mas smoke de servicios para validar upsert.
- **Prueba de orquestacion DAG (adicional clave):** se agrega test del flujo completo del DAG para validar contrato `extract -> validate -> load -> finalize`.

### Parte 3: Versionamiento y CI/CD

- **GitFlow (requerido en la prueba):** el repositorio esta preparado para flujo con ramas `main/develop/feature/hotfix`.
- **GitHub Actions:** valida estructura critica del repo y ejecuta `pytest` en cada PR/push.
- **Azure Pipelines YAML:** valida tests y, en `main/develop`, sincroniza cambios al ambiente y despliega con `docker compose`.

## 3) Tecnologias usadas por item

- **Orquestacion ETL:** `Apache Airflow`.
- **Procesamiento de datos:** `Polars`.
- **Validacion de datos:** `Pydantic v2`.
- **Acceso a BD:** `SQLAlchemy`.
- **Resiliencia de conexion:** `tenacity` + `pybreaker`.
- **Logs estructurados:** `structlog`.
- **Infra local:** `Docker` + `docker-compose`.
- **Pruebas:** `pytest` + `pytest-mock`.

## 4) Mejoras y extras implementados

Ademas de lo minimo solicitado, el proyecto incorpora mejoras que fortalecen produccion:

- **PgBouncer como pool externo:** reduce presion sobre PostgreSQL y estabiliza conexiones concurrentes.
- **`NullPool` en SQLAlchemy:** evita doble pooling porque el pool real ya lo maneja PgBouncer.
- **Full Jitter en backoff:** cada reintento espera un tiempo aleatorio exponencial para evitar reconexiones sincronizadas (thundering herd).
- **Circuit Breaker:** si hay fallos repetidos, corta temporalmente llamadas a BD y protege el sistema.
- **Checkpoint por watermark temporal:** permite retomar ejecucion sin reprocesar todo desde cero.
- **Dead Letter Table:** guarda registros invalidos para inspeccion y correccion.
- **Upsert idempotente:** `INSERT ... ON CONFLICT DO UPDATE`, evita duplicados y permite reprocesos seguros.
- **Paralelismo adaptativo:** calcula workers segun CPU/RAM disponibles mediante `psutil`.

## 5) Componentes del sistema y su funcion

### Flujo funcional

1. `extract`: lee watermark y guarda lotes en `etl_staging`.
2. `validate`: valida y separa invalidos en `etl_failed_records`.
3. `load`: hace upsert en `financial_records` y actualiza checkpoint.
4. `finalize`: marca corrida como completada y limpia buffers temporales.

### Componentes principales

- **`dags/financial_etl_dag.py`**
  - Define el DAG `financial_etl`.
  - Orquesta tareas en secuencia `extract -> validate -> load -> finalize`.

- **`pipeline/application/services.py`**
  - Casos de uso del pipeline (`run_extraction`, `run_validation`, `run_load`, `run_finalize`).
  - Punto de entrada de aplicacion para adapters.

- **`pipeline/adapters/airflow/tasks.py`**
  - Adapter entre DAG TaskFlow y casos de uso.
  - Mantiene contrato de metadatos entre tareas.

- **`pipeline/infrastructure/persistence/`**
  - Implementacion tecnica de extract/validate/load/finalize/smoke.
  - SQL, serializacion y escritura/lectura en tablas ETL/reporteria.

- **`pipeline/infrastructure/resilience/`**
  - Checkpoint, dead letter, retry y circuit breaker.

- **`pipeline/domain/models/`**
  - Modelos de dominio Pydantic (`Account`, `Transaction`, `FinancialRecord`, etc.).

- **`pipeline/utils/`**
  - Utilitarios tecnicos compartidos: `config.py`, `db.py`, `exceptions.py`, `log.py`, `serialize.py`.

- **`docker-compose.yaml`**
  - Levanta PostgreSQL, PgBouncer y Airflow; monta `./dags` y `./pipeline` en los contenedores.

- **`docker/sql/`**
  - Scripts de schema y datos de ejemplo.

- **`tests/`**
  - Suite principal de pytest para los modulos del pipeline.

## 6) Estructura del proyecto

```text
castor_challenge_airflow/
├── dags/                        # DAG de Airflow
├── pipeline/                    # Arquitectura hexagonal ETL
│   ├── domain/models/           # Entidades y modelos de dominio
│   ├── application/             # Casos de uso
│   ├── infrastructure/          # Persistencia y resiliencia
│   ├── adapters/                # Integraciones (Airflow/config)
│   └── utils/                   # Config/db/log/excepciones/serialize
├── docker/sql/                  # Schema y seeds de BD
├── docs/                        # Guías (p. ej. pruebas)
├── tests/                       # Tests con pytest
├── .github/workflows/ci.yml     # CI: estructura + pytest + resumen DAGs en PR
├── azure-pipelines.yml          # CI + deploy a ambiente remoto
├── docker-compose.yaml
└── requirements.txt
```

## 7) Guia para levantar servicios y ejecutar el pipeline

### Requisitos previos

- Docker y Docker Compose.
- Python 3.11 (para correr pruebas locales fuera de contenedores).
- Archivo `.env` con credenciales y variables requeridas.
- Variable requerida: `TARGET_POSTGRES_DB=castor_challenge_etl`.

### Paso a paso

1. **Levantar infraestructura** (primera vez o cambio de schema: `docker compose down -v`):
   - `docker compose up -d postgres pgbouncer`

2. **Inicializar Airflow:** `docker compose up airflow-init`

3. **Airflow web + scheduler:** `docker compose up -d airflow-webserver airflow-scheduler`

4. **UI Airflow:** `http://localhost:8080` (usuario/clave en `.env`: `AIRFLOW_ADMIN_*`)

5. **Ejecutar el DAG** desde la UI (trigger manual si aplica).

### GitHub Actions y DAGs en un PR

En cada PR, el workflow corre `pytest` y luego (solo en PR) deja **visible** lo que hay en `dags/` para ese commit:

- En la pestaña **Summary** del run aparece el listado.
- En **Artifacts** puedes descargar un zip con la carpeta `dags/` de ese SHA.

Eso es lo que GitHub puede hacer **sin** un servidor propio: el admin de Airflow sigue siendo el tuyo local (`docker compose`) o un despliegue que tu configures aparte; Actions no abre el puerto 8080 en Internet.

Adicionalmente, GitHub Actions valida una lista de archivos criticos (estructura base del proyecto) para evitar que se eliminen piezas esenciales del sistema por error.

### Azure Pipelines (test + deploy)

`azure-pipelines.yml` ahora ejecuta dos etapas:

1. **Test:** instala dependencias y corre `pytest`.
2. **Deploy:** para ramas `main/develop`, sincroniza el repo al host remoto y ejecuta `docker compose up -d --build`.

Variables/secretos esperados en Azure:
- `DEPLOY_HOST`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY_B64` (llave privada SSH en base64)



### Verificaciones minimas esperadas

- La corrida completa termina en estado exitoso.
- Se insertan/actualizan registros en `castor_challenge_etl.public.financial_records`.
- El checkpoint se actualiza en `castor_challenge_etl.etl.etl_checkpoints`.
- Registros invalidos (si existen) quedan en `castor_challenge_etl.etl.etl_failed_records`.

## 8) Guia corta para ejecutar pruebas solicitadas por la prueba tecnica

La guia detallada esta en `docs/GUIA_PRUEBAS.md`. Aqui va el resumen operativo:

1. Instalar dependencias:
   - `pip install -r requirements.txt`

2. Ejecutar pruebas activas:
   - `pytest tests/ -q`

3. Ejecutar smoke de transferencia (DAG ligero):
   - `set -a && source .env && set +a`
   - `export PYTHONPATH=.`
   - ejecutar DAG `financial_etl_smoke` (manual en Airflow UI)

## 9) Entregables clave para la evaluacion

- Codigo ETL desacoplado por capas (extract/validate/load/finalize).
- Contenerizacion con variables de entorno (sin secretos hardcodeados).
- Pruebas automatizadas con pytest.
- Pipeline CI en GitHub Actions con guard de estructura.
- Pipeline Azure con validacion y despliegue por rama.
- Documentacion funcional para entender y correr el sistema.