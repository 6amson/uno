-- Add a status column to idempotency_keys so a key can be atomically reserved
-- ('processing') before work begins, then marked 'completed' with the response.
-- This prevents concurrent requests with the same idempotency key from both
-- processing and creating duplicate payments.

ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'processing';

-- Existing rows that already have a response body are completed.
UPDATE idempotency_keys
    SET status = 'completed'
    WHERE response_body IS NOT NULL AND response_body != '';

-- response_body is only meaningful once completed; allow NULL while processing.
ALTER TABLE idempotency_keys
    ALTER COLUMN response_body DROP NOT NULL;
