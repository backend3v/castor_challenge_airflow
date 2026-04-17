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