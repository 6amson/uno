-- Link idempotency keys to payments and record when processing started,
-- so the recovery worker can reconcile stranded keys after a crash.

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS payment_id UUID REFERENCES payments(id);

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS order_id TEXT;

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_status_started
    ON idempotency_keys(status, started_at);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_order
    ON idempotency_keys(order_id);
