-- Track when a payment reached AUTHORIZED state,
-- so stale authorizations can be voided automatically.
ALTER TABLE payments
    ADD COLUMN IF NOT EXISTS authorized_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_payments_authorized_at
    ON payments(authorized_at)
    WHERE state = 'AUTHORIZED';
