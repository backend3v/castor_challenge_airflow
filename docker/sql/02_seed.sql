-- =============================================================
-- Seed: Inserción masiva usando generate_series() nativo de PG
-- Estrategia: generate_series() > INSERT masivo > COPY
-- porque no requiere archivos externos ni herramientas adicionales
-- y genera datos relacionalmente consistentes en una sola query.
-- Referencia: https://www.postgresql.org/docs/15/populate.html
-- =============================================================

-- Desactivar autocommit por bloque para maximizar velocidad [web:148]
BEGIN;

-- -------------------------------------------------------------
-- categories — 10 categorías fijas (catálogo pequeño)
-- -------------------------------------------------------------
INSERT INTO public.categories (name, description) VALUES
    ('salary',        'Ingreso por nómina'),
    ('rent',          'Pago de arriendo o hipoteca'),
    ('utilities',     'Servicios públicos'),
    ('groceries',     'Mercado y alimentos'),
    ('transport',     'Transporte y combustible'),
    ('healthcare',    'Salud y medicamentos'),
    ('entertainment', 'Entretenimiento y ocio'),
    ('investment',    'Depósitos y fondos'),
    ('transfer',      'Transferencias entre cuentas'),
    ('other',         'Otros movimientos')
ON CONFLICT (name) DO NOTHING;

-- -------------------------------------------------------------
-- accounts — 1,000 cuentas usando generate_series()
-- chr(65 + ...) genera letras A-Z para nombres sintéticos
-- random() * N genera valores numéricos distribuidos
-- -------------------------------------------------------------
INSERT INTO public.accounts (account_name, account_type, owner_name, currency, balance, is_active, created_at, updated_at)
SELECT
    'Account-' || LPAD(seq::TEXT, 5, '0'),
    (ARRAY['savings', 'checking', 'investment'])[1 + (seq % 3)],
    'Owner-' || chr(65 + (seq % 26)) || chr(65 + ((seq * 7) % 26)),
    (ARRAY['USD', 'EUR', 'COP'])[1 + (seq % 3)],
    ROUND((random() * 100000)::NUMERIC, 2),
    (random() > 0.05),          -- 95% activas
    NOW() - (random() * INTERVAL '2 years'),
    NOW() - (random() * INTERVAL '30 days')
FROM generate_series(1, 1000) AS seq;

-- -------------------------------------------------------------
-- transactions — 500,000 filas
-- Distribuidas en los últimos 2 años con updated_at variable
-- para simular una BD legada con histórico real
--
-- Técnica: JOIN de generate_series con subquery de account_ids
-- para asignar FK válidas sin loops ni cursores
-- -------------------------------------------------------------
INSERT INTO public.transactions (
    account_id, category_id, amount, currency,
    transaction_type, status, description, created_at, updated_at
)
SELECT
    a.account_id,
    1 + (seq % 10),                                             -- category_id 1-10
    ROUND((random() * 9900 + 100)::NUMERIC, 2),                -- amount $100 - $10,000
    (ARRAY['USD', 'EUR', 'COP'])[1 + (seq % 3)],
    (ARRAY['debit', 'credit'])[1 + (seq % 2)],
    (ARRAY['pending', 'completed', 'failed'])[
        CASE
            WHEN random() < 0.75 THEN 1   -- 75% completed
            WHEN random() < 0.90 THEN 2   -- 15% pending
            ELSE 3                         -- 10% failed
        END
    ],
    'Transaction ref-' || seq,
    NOW() - (random() * INTERVAL '2 years'),
    -- updated_at: mayoría reciente (últimos 7 días) para simular actividad incremental
    CASE
        WHEN seq % 10 < 7 THEN NOW() - (random() * INTERVAL '7 days')
        ELSE NOW() - (random() * INTERVAL '2 years')
    END
FROM
    generate_series(1, 500000) AS seq
    -- Asignar account_id válida: modulo sobre total de cuentas
    JOIN (
        SELECT account_id, ROW_NUMBER() OVER () AS rn FROM public.accounts
    ) a ON a.rn = 1 + (seq % (SELECT COUNT(*) FROM public.accounts));

COMMIT;

-- -------------------------------------------------------------
-- Verificación post-seed
-- -------------------------------------------------------------
SELECT
    'accounts'     AS tabla, COUNT(*) AS total FROM public.accounts
UNION ALL SELECT
    'categories',            COUNT(*)          FROM public.categories
UNION ALL SELECT
    'transactions',          COUNT(*)          FROM public.transactions;
