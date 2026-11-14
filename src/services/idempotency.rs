use crate::domain::errors::GatewayError;
use crate::storage::repository::Repository;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Outcome of checking/reserving an idempotency key.
#[derive(Debug)]
pub enum IdempotencyOutcome {
    /// This caller won the reservation and should proceed to process the request.
    Reserved,
    /// The key was already completed with the same request hash; replay the cached response.
    Replay(String),
}

/// A single cached completed idempotency entry with an expiry timestamp.
#[derive(Clone)]
struct CacheEntry {
    request_hash: String,
    response_body: String,
    expires_at: u64, // epoch seconds
}

#[derive(Clone)]
pub struct IdempotencyLayer {
    repo: Repository,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    max_entries: usize,
    ttl_seconds: u64,
}

impl IdempotencyLayer {
    pub fn new(repo: Repository) -> Self {
        Self {
            repo,
            cache: Arc::new(Mutex::new(HashMap::new())),
            max_entries: 100_000,
            ttl_seconds: 30 * 60, // 30 minutes
        }
    }

    /// Derive a deterministic hash from canonical request fields so that
    /// reusing an idempotency key for a different payload is detected.
    pub fn hash_request(order_id: &str, amount_cents: i64, card_last_four: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(order_id.as_bytes());
        hasher.update(b"|");
        hasher.update(amount_cents.to_string().as_bytes());
        hasher.update(b"|");
        hasher.update(card_last_four.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Called by the service before processing.
    ///
    /// Returns `Ok(IdempotencyOutcome::Reserved)` if this caller owns the key
    /// and should process the request, or `Ok(IdempotencyOutcome::Replay(body))`
    /// if the key was already completed with the same request hash.
    ///
    /// Returns `Err(IdempotencyConflict)` if the key was completed with a
    /// different request hash.
    pub async fn check_or_reserve(
        &self,
        key: &str,
        order_id: &str,
        amount_cents: i64,
        card_last_four: &str,
    ) -> Result<IdempotencyOutcome, GatewayError> {
        let request_hash = Self::hash_request(order_id, amount_cents, card_last_four);

        // 1. Memory fast path (completed entries only).
        {
            let guard = self.cache.lock().await;
            if let Some(entry) = guard.get(key) {
                if Self::now_seconds() < entry.expires_at {
                    if entry.request_hash == request_hash {
                        return Ok(IdempotencyOutcome::Replay(entry.response_body.clone()));
                    }
                    return Err(GatewayError::IdempotencyConflict);
                }
            }
        }

        // 2. Atomically reserve the key in Postgres (source of truth).
        let won = self.repo.reserve_idempotency(key, &request_hash, order_id).await?;
        if won {
            return Ok(IdempotencyOutcome::Reserved);
        }

        // 3. Lost the race: another caller owns the key. Poll until it completes.
        for _ in 0..50 { // ~5 seconds total
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some((stored_hash, response_body, status)) = self.repo.get_idempotency(key).await? {
                match status.as_str() {
                    "completed" => {
                        if stored_hash != request_hash {
                            return Err(GatewayError::IdempotencyConflict);
                        }
                        let body = response_body.ok_or_else(|| {
                            GatewayError::Internal("completed idempotency key has no response".into())
                        })?;
                        // Repopulate memory for future fast replays.
                        let entry = CacheEntry {
                            request_hash: stored_hash.clone(),
                            response_body: body.clone(),
                            expires_at: Self::now_seconds() + self.ttl_seconds,
                        };
                        self.evict_if_full().await;
                        self.cache.lock().await.insert(key.to_string(), entry);
                        return Ok(IdempotencyOutcome::Replay(body));
                    }
                    "processing" => continue, // still in flight, keep polling
                    _ => {
                        return Err(GatewayError::Internal(format!(
                            "unknown idempotency status: {}",
                            status
                        )))
                    }
                }
            } else {
                // Key disappeared; treat as conflict.
                return Err(GatewayError::IdempotencyConflict);
            }
        }

        Err(GatewayError::Internal(
            "idempotency key still processing after timeout".into(),
        ))
    }

    /// Evict the oldest entry if we are at capacity, preventing unbounded growth.
    async fn evict_if_full(&self) {
        let mut guard = self.cache.lock().await;
        if guard.len() >= self.max_entries {
            if let Some(key) = guard.keys().next().cloned() {
                guard.remove(&key);
            }
        }
    }

    /// Persist the completed response to Postgres first (source of truth), then
    /// write-through to memory. Call this after the request has been fully processed.
    pub async fn complete(
        &self,
        key: &str,
        request_hash: &str,
        payment_id: Option<Uuid>,
        response_body: &str,
    ) -> Result<(), GatewayError> {
        self.repo.complete_idempotency(key, payment_id, response_body).await?;

        let entry = CacheEntry {
            request_hash: request_hash.to_string(),
            response_body: response_body.to_string(),
            expires_at: Self::now_seconds() + self.ttl_seconds,
        };
        self.evict_if_full().await;
        self.cache.lock().await.insert(key.to_string(), entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_request_is_deterministic() {
        let h1 = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        let h2 = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_request_differs_by_amount() {
        let h1 = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        let h2 = IdempotencyLayer::hash_request("order-1", 2000, "4242");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_request_differs_by_card_last_four() {
        let h1 = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        let h2 = IdempotencyLayer::hash_request("order-1", 1000, "1234");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_request_differs_by_order_id() {
        let h1 = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        let h2 = IdempotencyLayer::hash_request("order-2", 1000, "4242");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_request_is_64_hex_chars() {
        let h = IdempotencyLayer::hash_request("order-1", 1000, "4242");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
