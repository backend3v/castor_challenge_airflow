-- =============================================================
-- Seed incremental: simula nuevos registros y modificaciones
-- Usar para probar el pipeline después del seed inicial
-- Ejecutar manualmente: docker compose exec postgres psql -U "$POSTGRES_USER" -d castor_challenge -f /docker-entrypoint-initdb.d/03_seed_incremental.sql
-- =============================================================

BEGIN;

-- 1,000 transacciones nuevas (updated_at = ahora)
INSERT INTO public.transactions (
    account_id, category_id, amount, currency,
    transaction_type, status, description, created_at, updated_at
)
SELECT
    a.account_id,
    1 + (seq % 10),
    ROUND((random() * 9900 + 100)::NUMERIC, 2),
    (ARRAY['USD', 'EUR', 'COP'])[1 + (seq % 3)],
    (ARRAY['debit', 'credit'])[1 + (seq % 2)],
    'completed',
    'Incremental-' || seq,
    NOW(),
    NOW()   -- updated_at = NOW() → el watermark las captura en el siguiente run
FROM
    generate_series(1, 1000) AS seq
    JOIN (
        SELECT account_id, ROW_NUMBER() OVER () AS rn FROM public.accounts
    ) a ON a.rn = 1 + (seq % (SELECT COUNT(*) FROM public.accounts));

-- Modificar 200 transacciones existentes (simulan cambio de estado)
UPDATE public.transactions
SET
    status     = 'completed',
    updated_at = NOW()      -- updated_at actualizado → watermark las recoge
WHERE transaction_id IN (
    SELECT transaction_id
    FROM public.transactions
    WHERE status = 'pending'
    ORDER BY created_at
    LIMIT 200
);

COMMIT;

SELECT 'nuevas transacciones' AS tipo, COUNT(*) FROM public.transactions WHERE updated_at >= NOW() - INTERVAL '1 minute'
UNION ALL
SELECT 'total transacciones',          COUNT(*) FROM public.transactions;
