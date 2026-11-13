-- payments table
CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    state TEXT NOT NULL DEFAULT 'PENDING',
    bank_reference TEXT,
    last_four TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TYPE payment_state AS ENUM ('PENDING', 'AUTHORIZED', 'CAPTURED', 'REFUNDED', 'VOIDED');

-- state history
CREATE TABLE IF NOT EXISTS state_history (
    id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(id),
    from_state payment_state,
    to_state payment_state NOT NULL,
    bank_reference TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- idempotency keys
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- in-flight marker (stuck payments)
CREATE TABLE IF NOT EXISTS inflight (
    payment_id UUID PRIMARY KEY REFERENCES payments(id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_customer ON payments(customer_id);
CREATE INDEX IF NOT EXISTS idx_payments_state ON payments(state);
