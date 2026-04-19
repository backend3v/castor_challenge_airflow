-- =============================================================
-- Database layout for the challenge
-- - castor_challenge: source business data (public)
-- - castor_challenge_etl: airflow metadata + etl controls + reporting tables
-- =============================================================

SELECT 'CREATE DATABASE castor_challenge_etl'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'castor_challenge_etl')
\gexec

\connect castor_challenge

-- =============================================================
-- Source DB (castor_challenge.public)
-- =============================================================
CREATE TABLE IF NOT EXISTS public.accounts (
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

CREATE TABLE IF NOT EXISTS public.categories (
    category_id     SERIAL PRIMARY KEY,
    name            VARCHAR(50)     NOT NULL UNIQUE,
    description     VARCHAR(200),
    updated_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.transactions (
    transaction_id   BIGSERIAL PRIMARY KEY,
    account_id       UUID            NOT NULL REFERENCES public.accounts(account_id),
    category_id      INTEGER         NOT NULL REFERENCES public.categories(category_id),
    amount           NUMERIC(18, 2)  NOT NULL CHECK (amount <> 0),
    currency         CHAR(3)         NOT NULL DEFAULT 'USD',
    transaction_type VARCHAR(10)     NOT NULL CHECK (transaction_type IN ('debit', 'credit')),
    status           VARCHAR(10)     NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    description      VARCHAR(255),
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_updated_at ON public.transactions (updated_at);
CREATE INDEX IF NOT EXISTS idx_accounts_updated_at ON public.accounts (updated_at);

-- Drop legacy mixed-mode ETL tables from source DB.
DROP TABLE IF EXISTS public.etl_validated;
DROP TABLE IF EXISTS public.etl_staging;
DROP TABLE IF EXISTS public.etl_failed_records;
DROP TABLE IF EXISTS public.etl_checkpoints;
DROP TABLE IF EXISTS public.financial_records;

\connect castor_challenge_etl

-- =============================================================
-- ETL DB (castor_challenge_etl): schemas and operational tables
-- =============================================================
CREATE SCHEMA IF NOT EXISTS dag;
CREATE SCHEMA IF NOT EXISTS etl;

-- Reporting target in public schema.
CREATE TABLE IF NOT EXISTS public.financial_records (
    transaction_id    BIGINT PRIMARY KEY,
    account_id        UUID            NOT NULL,
    category_id       INTEGER         NOT NULL,
    amount            NUMERIC(18, 2)  NOT NULL,
    currency          CHAR(3)         NOT NULL,
    transaction_type  VARCHAR(10)     NOT NULL,
    status            VARCHAR(10)     NOT NULL,
    description       VARCHAR(255),
    created_at        TIMESTAMP       NOT NULL,
    updated_at        TIMESTAMP       NOT NULL,
    source_updated_at TIMESTAMP       NOT NULL,
    loaded_at         TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_financial_records_source_updated
    ON public.financial_records (source_updated_at);

-- ETL operational state and buffers in etl schema.
CREATE TABLE IF NOT EXISTS etl.etl_checkpoints (
    pipeline_run_id     TEXT            PRIMARY KEY,
    last_watermark      TIMESTAMP       NOT NULL,
    chunks_completed    INTEGER         NOT NULL DEFAULT 0,
    total_chunks        INTEGER,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed')),
    started_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl.etl_failed_records (
    id                  SERIAL          PRIMARY KEY,
    pipeline_run_id     TEXT            NOT NULL,
    chunk_index         INTEGER         NOT NULL,
    payload             JSONB           NOT NULL,
    error_message       TEXT            NOT NULL,
    attempt_count       INTEGER         NOT NULL DEFAULT 1,
    failed_at           TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_failed_records_run
    ON etl.etl_failed_records (pipeline_run_id, failed_at);

CREATE TABLE IF NOT EXISTS etl.etl_staging (
    pipeline_run_id  TEXT            NOT NULL,
    chunk_index      INTEGER         NOT NULL,
    records          JSONB           NOT NULL,
    created_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS etl.etl_validated (
    pipeline_run_id       TEXT            NOT NULL,
    chunk_index           INTEGER         NOT NULL,
    records               JSONB           NOT NULL,
    source_high_watermark TIMESTAMP       NOT NULL,
    created_at            TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_run_id, chunk_index)
);
