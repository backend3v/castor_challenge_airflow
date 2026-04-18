-- =============================================================
-- Schema: Sistema financiero legado (fuente de extracción ETL)
-- Tablas: accounts, categories, transactions
-- =============================================================

CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_name    VARCHAR(100)    NOT NULL,
    account_type    VARCHAR(20)     NOT NULL CHECK (account_type IN ('savings', 'checking', 'investment')),
    owner_name      VARCHAR(100)    NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'USD',
    balance         NUMERIC(18, 2)  NOT NULL DEFAULT 0.00,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS categories (
    category_id     SERIAL PRIMARY KEY,
    name            VARCHAR(50)     NOT NULL UNIQUE,
    description     VARCHAR(200),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID            NOT NULL REFERENCES accounts(account_id),
    category_id      INTEGER         NOT NULL REFERENCES categories(category_id),
    amount           NUMERIC(18, 2)  NOT NULL CHECK (amount <> 0),
    currency         CHAR(3)         NOT NULL DEFAULT 'USD',
    transaction_type VARCHAR(10)     NOT NULL CHECK (transaction_type IN ('debit', 'credit')),
    status           VARCHAR(10)     NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    description      VARCHAR(255),
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Índice sobre updated_at — columna watermark de extracción incremental
-- Sin este índice, el WHERE updated_at > :watermark haría full scan
CREATE INDEX IF NOT EXISTS idx_transactions_updated_at ON transactions (updated_at);
CREATE INDEX IF NOT EXISTS idx_accounts_updated_at     ON accounts (updated_at);

-- =============================================================
-- Tablas de control del pipeline ETL (destino en mismo Postgres
-- para esta demo — en producción sería una BD separada)
-- =============================================================

CREATE TABLE IF NOT EXISTS etl_checkpoints (
    pipeline_run_id     TEXT            PRIMARY KEY,
    last_watermark      TIMESTAMP       NOT NULL,
    chunks_completed    INTEGER         NOT NULL DEFAULT 0,
    total_chunks        INTEGER,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed')),
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl_failed_records (
    id                  SERIAL          PRIMARY KEY,
    pipeline_run_id     TEXT            NOT NULL,
    chunk_index         INTEGER         NOT NULL,
    payload             JSONB           NOT NULL,
    error_message       TEXT            NOT NULL,
    attempt_count       INTEGER         NOT NULL DEFAULT 1,
    failed_at           TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Índice para consultas del DBA por pipeline y fecha
CREATE INDEX IF NOT EXISTS idx_failed_records_run ON etl_failed_records (pipeline_run_id, failed_at);

-- Destino analítico (warehouse) — upsert idempotente desde el ETL
CREATE TABLE IF NOT EXISTS financial_records (
    transaction_id   UUID PRIMARY KEY,
    account_id       UUID            NOT NULL,
    category_id      INTEGER         NOT NULL,
    amount           NUMERIC(18, 2)  NOT NULL,
    currency         CHAR(3)         NOT NULL,
    transaction_type VARCHAR(10)   NOT NULL,
    status           VARCHAR(10)     NOT NULL,
    description      VARCHAR(255),
    created_at       TIMESTAMP       NOT NULL,
    updated_at       TIMESTAMP       NOT NULL,
    source_updated_at TIMESTAMP      NOT NULL,
    loaded_at        TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_financial_records_source_updated
    ON financial_records (source_updated_at);

-- Buffer entre tareas del DAG (metadata en XCom; datos aquí)
CREATE TABLE IF NOT EXISTS etl_staging (
    pipeline_run_id  TEXT            NOT NULL,
    chunk_index      INTEGER         NOT NULL,
    records          JSONB           NOT NULL,
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS etl_validated (
    pipeline_run_id       TEXT            NOT NULL,
    chunk_index           INTEGER         NOT NULL,
    records               JSONB           NOT NULL,
    source_high_watermark TIMESTAMP       NOT NULL,
    created_at            TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, chunk_index)
);
