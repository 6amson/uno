use sqlx::PgPool;
use uuid::Uuid;
use crate::domain::models::{Payment, StateHistory, PaymentState};
use crate::domain::errors::GatewayError;
use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct Repository {
    pool: PgPool,
}

impl Repository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a brand-new payment receipt in PENDING state.
    pub async fn create_payment(&self, payment: &Payment) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            INSERT INTO payments (
                id, order_id, customer_id, amount_cents, currency,
                state, bank_reference, last_four, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(payment.id)
        .bind(&payment.order_id)
        .bind(&payment.customer_id)
        .bind(payment.amount_cents)
        .bind(&payment.currency)
        .bind(payment.state)
        .bind(&payment.bank_reference)
        .bind(&payment.last_four)
        .bind(payment.created_at)
        .bind(payment.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Fetch a single payment by its internal UUID.
    pub async fn get_payment(&self, id: Uuid) -> Result<Option<Payment>, GatewayError> {
        let row = sqlx::query_as::<_, Payment>("SELECT * FROM payments WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(row)
    }

    /// Lookup by the order service's own order ID.
    pub async fn get_by_order(&self, order_id: &str) -> Result<Option<Payment>, GatewayError> {
        let row = sqlx::query_as::<_, Payment>("SELECT * FROM payments WHERE order_id = $1")
            .bind(order_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(row)
    }

    /// All payments for a customer, newest first.
    pub async fn list_by_customer(&self, customer_id: &str) -> Result<Vec<Payment>, GatewayError> {
        let rows = sqlx::query_as::<_, Payment>(
            "SELECT * FROM payments WHERE customer_id = $1 ORDER BY created_at DESC"
        )
        .bind(customer_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(rows)
    }

    /// Advance the payment state inside a transaction, with optimistic concurrency.
    ///
    /// If `from` is `Some`, the update only applies when the current DB state matches,
    /// preventing two workers from transitioning the same payment simultaneously.
    /// Always appends a row to `state_history` for the audit trail.
    pub async fn update_state(
        &self,
        id: Uuid,
        from: Option<PaymentState>,
        to: PaymentState,
        bank_reference: Option<String>,
    ) -> Result<Payment, GatewayError> {
        let mut tx = self.pool.begin().await.map_err(|e| GatewayError::Storage(e.to_string()))?;

        let updated = match from {
            Some(from_state) => {
                sqlx::query_as::<_, Payment>(
                    r#"
                    UPDATE payments
                    SET state = $2, bank_reference = $3, updated_at = NOW()
                    WHERE id = $1 AND state = $4
                    RETURNING *
                    "#,
                )
                .bind(id)
                .bind(to)
                .bind(&bank_reference)
                .bind(from_state)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| GatewayError::Storage(e.to_string()))?
            }
            None => {
                sqlx::query_as::<_, Payment>(
                    r#"
                    UPDATE payments
                    SET state = $2, bank_reference = $3, updated_at = NOW()
                    WHERE id = $1
                    RETURNING *
                    "#,
                )
                .bind(id)
                .bind(to)
                .bind(&bank_reference)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| GatewayError::Storage(e.to_string()))?
            }
        };

        sqlx::query(
            r#"
            INSERT INTO state_history (id, payment_id, from_state, to_state, bank_reference, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(id)
        .bind(from)
        .bind(to)
        .bind(&bank_reference)
        .execute(&mut *tx)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;

        tx.commit().await.map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(updated)
    }

    /// Standalone history record (kept for callers that want to append history
    /// without a state change, e.g. recording a bank callback).
    pub async fn record_history(&self, history: &StateHistory) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            INSERT INTO state_history (id, payment_id, from_state, to_state, bank_reference, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(history.id)
        .bind(history.payment_id)
        .bind(history.from_state)
        .bind(history.to_state)
        .bind(&history.bank_reference)
        .bind(history.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Find payments mid-bank-call that may be stuck after a crash or timeout.
    pub async fn list_stuck(&self, older_than_seconds: i64) -> Result<Vec<Payment>, GatewayError> {
        let rows = sqlx::query_as::<_, Payment>(
            r#"
            SELECT p.*
            FROM payments p
            JOIN inflight i ON p.id = i.payment_id
            WHERE i.started_at < NOW() - ($1 * interval '1 second')
              AND p.state IN ('PENDING', 'AUTHORIZED')
            "#,
        )
        .bind(older_than_seconds)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(rows)
    }

    /// Atomically mark a stuck payment as under recovery so concurrent workers
    /// skip it. Returns true if this worker won the claim.
    pub async fn claim_inflight(&self, id: Uuid) -> Result<bool, GatewayError> {
        let claimed = sqlx::query(
            r#"
            UPDATE inflight
            SET attempt = attempt + 1
            WHERE payment_id = $1
            RETURNING payment_id
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?
        .is_some();
        Ok(claimed)
    }

    /// Upsert an in-flight marker before a bank call.
    pub async fn claim_inflight_upsert(&self, id: Uuid) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            INSERT INTO inflight (payment_id, started_at, attempt)
            VALUES ($1, NOW(), 1)
            ON CONFLICT (payment_id) DO UPDATE SET attempt = inflight.attempt + 1, started_at = NOW()
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Remove the in-flight marker after a bank call completes.
    pub async fn clear_inflight(&self, id: Uuid) -> Result<(), GatewayError> {
        sqlx::query(
            "DELETE FROM inflight WHERE payment_id = $1",
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Look up an idempotency key. Returns (request_hash, response_body, status)
    /// if present; response_body is None while the key is still 'processing'.
    pub async fn get_idempotency(
        &self,
        key: &str,
    ) -> Result<Option<(String, Option<String>, String)>, GatewayError> {
        let row = sqlx::query_as::<_, (String, Option<String>, String)>(
            "SELECT request_hash, response_body, status FROM idempotency_keys WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(row)
    }

    /// Atomically reserve an idempotency key. Returns true if this caller won
    /// the reservation (key was absent), false if another caller already owns it.
    pub async fn reserve_idempotency(
        &self,
        key: &str,
        request_hash: &str,
        order_id: &str,
    ) -> Result<bool, GatewayError> {
        let inserted = sqlx::query(
            r#"
            INSERT INTO idempotency_keys (key, request_hash, response_body, status, order_id)
            VALUES ($1, $2, NULL, 'processing', $3)
            ON CONFLICT (key) DO NOTHING
            "#,
        )
        .bind(key)
        .bind(request_hash)
        .bind(order_id)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?
        .rows_affected()
        > 0;
        Ok(inserted)
    }

    /// Publish the completed response for an idempotency key, transitioning it
    /// from 'processing' to 'completed'. Links the key to its payment and
    /// records when processing finished. No-op if the key was already completed.
    pub async fn complete_idempotency(
        &self,
        key: &str,
        payment_id: Option<Uuid>,
        response_body: &str,
    ) -> Result<(), GatewayError> {
        sqlx::query(
            r#"
            UPDATE idempotency_keys
            SET response_body = $2, status = 'completed', payment_id = $3
            WHERE key = $1 AND status = 'processing'
            "#,
        )
        .bind(key)
        .bind(response_body)
        .bind(payment_id)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Find idempotency keys stuck in 'processing' beyond the TTL, in batches.
    /// Returns (key, payment_id, order_id, request_hash, started_at).
    pub async fn list_stale_processing_keys(
        &self,
        older_than_seconds: i64,
        limit: i64,
    ) -> Result<Vec<(String, Option<Uuid>, Option<String>, String, DateTime<Utc>)>, GatewayError> {
        let rows = sqlx::query_as::<_, (String, Option<Uuid>, Option<String>, String, DateTime<Utc>)>(
            r#"
            SELECT key, payment_id, order_id, request_hash, started_at
            FROM idempotency_keys
            WHERE status = 'processing'
              AND started_at < NOW() - ($1 * interval '1 second')
            ORDER BY started_at ASC
            LIMIT $2
            "#,
        )
        .bind(older_than_seconds)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(rows)
    }

    /// Mark an idempotency key as completed using the given payment as the
    /// cached response body. Used by recovery when a stranded key's payment
    /// already reached a terminal state.
    pub async fn mark_idempotency_completed_from_payment(
        &self,
        key: &str,
        payment: &Payment,
    ) -> Result<(), GatewayError> {
        let response_body = serde_json::to_string(payment)
            .map_err(|e| GatewayError::Internal(format!("serialize: {}", e)))?;
        self.complete_idempotency(key, Some(payment.id), &response_body).await
    }

    /// Delete a stranded processing idempotency key so a safe retry can proceed.
    pub async fn delete_idempotency_key(&self, key: &str) -> Result<(), GatewayError> {
        sqlx::query(
            "DELETE FROM idempotency_keys WHERE key = $1 AND status = 'processing'",
        )
        .bind(key)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Set the authorized_at timestamp on a payment.
    pub async fn set_authorized_at(
        &self,
        id: Uuid,
        authorized_at: DateTime<Utc>,
    ) -> Result<(), GatewayError> {
        sqlx::query(
            "UPDATE payments SET authorized_at = $2 WHERE id = $1",
        )
        .bind(id)
        .bind(authorized_at)
        .execute(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Find AUTHORIZED payments older than the TTL, in batches.
    pub async fn list_stale_authorized(
        &self,
        older_than_seconds: i64,
        limit: i64,
    ) -> Result<Vec<Payment>, GatewayError> {
        let rows = sqlx::query_as::<_, Payment>(
            r#"
            SELECT id, order_id, customer_id, amount_cents, currency, state,
                   bank_reference, last_four, created_at, updated_at, authorized_at
            FROM payments
            WHERE state = 'AUTHORIZED'
              AND authorized_at IS NOT NULL
              AND authorized_at < NOW() - ($1 * interval '1 second')
            ORDER BY authorized_at ASC
            LIMIT $2
            "#,
        )
        .bind(older_than_seconds)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| GatewayError::Storage(e.to_string()))?;
        Ok(rows)
    }
}
