-- Convert payments.state from TEXT to the payment_state enum type,
-- so Rust's sqlx::Type mapping (type_name = "payment_state") aligns
-- with the database column at runtime.

ALTER TABLE payments ALTER COLUMN state DROP DEFAULT;
ALTER TABLE payments ALTER COLUMN state TYPE payment_state USING state::payment_state;
ALTER TABLE payments ALTER COLUMN state SET DEFAULT 'PENDING'::payment_state;
