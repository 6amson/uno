use crate::storage::repository::Repository;
use crate::bank::client::{BankClient, BankError};
use crate::domain::models::PaymentState;
use crate::domain::errors::GatewayError;
use std::time::Duration;

pub struct RecoveryWorker {
    repo: Repository,
    bank: BankClient,
    interval: Duration,
}

impl RecoveryWorker {
    pub fn new(repo: Repository, bank: BankClient, interval: Duration) -> Self {
        Self { repo, bank, interval }
    }

    pub async fn run(self) -> ! {
        loop {
            tokio::time::sleep(self.interval).await;
            if let Err(e) = self.recover_cycle().await {
                tracing::error!("recovery cycle failed: {}", e);
            }
        }
    }

    async fn recover_cycle(&self) -> Result<(), GatewayError> {
        let stuck = self.repo.list_stuck(300).await?; // stuck for > 5 minutes

        for payment in stuck {
            let claimed = self.repo.claim_inflight(payment.id).await?;
            if !claimed {
                continue; // another worker already handling it
            }

            match payment.state {
                PaymentState::Pending => {
                    // Can't re-authorize: we don't store full card details.
                    // Leave as PENDING; requires manual intervention or re-submission.
                    tracing::warn!("payment {} stuck in PENDING, cannot re-authorize without card details", payment.id);
                    let _ = self.repo.clear_inflight(payment.id).await;
                }
                PaymentState::Authorized => {
                    // Authorization succeeded at the bank (we have bank_reference).
                    // Just clear the inflight marker; keep AUTHORIZED.
                    let _ = self.repo.clear_inflight(payment.id).await;
                }
                PaymentState::Captured | PaymentState::Refunded | PaymentState::Voided => {
                    // Already terminal; just clear the inflight marker
                    let _ = self.repo.clear_inflight(payment.id).await;
                }
            }
        }

        // Reconcile stranded idempotency keys left 'processing' after a crash.
        self.reconcile_stale_idempotency_keys().await?;

        // Sweep stale AUTHORIZED payments that were never captured/voided.
        self.reconcile_stale_authorized_payments().await?;

        Ok(())
    }

    /// Sweep AUTHORIZED payments older than the TTL (default 7 days).
    /// Attempts to void at the bank and transitions locally to VOIDED.
    async fn reconcile_stale_authorized_payments(&self) -> Result<(), GatewayError> {
        const BATCH: i64 = 100;
        const STALE_SECONDS: i64 = 604_800; // 7 days

        loop {
            let payments = self.repo.list_stale_authorized(STALE_SECONDS, BATCH).await?;
            if payments.is_empty() {
                break;
            }

            for (id, _order_id, _amount_cents, bank_ref, authorized_at) in payments {
                tracing::warn!(
                    "payment {} ({}) authorized at {:?}, age exceeds {}s; voiding at bank",
                    id, _order_id, authorized_at, STALE_SECONDS
                );

                match bank_ref {
                    Some(ref bank_ref) => {
                        match self.bank.void(
                            crate::bank::client::VoidRequest {
                                authorization_id: bank_ref.clone(),
                            },
                            None,
                        ).await {
                            Ok(_) => {
                                let _ = self.repo.update_state(
                                    id,
                                    Some(PaymentState::Authorized),
                                    PaymentState::Voided,
                                    Some(bank_ref.clone()),
                                ).await;
                                tracing::info!("payment {} voided at bank and locally", id);
                            }
                            Err(BankError::Permanent(_)) => {
                                // Bank auth expired or already voided — mark VOIDED locally
                                tracing::warn!("payment {} AUTHORIZED but bank void rejected ({}), marking VOIDED locally", id, bank_ref);
                                let _ = self.repo.update_state(
                                    id,
                                    Some(PaymentState::Authorized),
                                    PaymentState::Voided,
                                    Some(bank_ref.clone()),
                                ).await;
                            }
                            Err(_) => {
                                // Transient error (timeout, 5xx) → leave AUTHORIZED, retry next cycle
                                tracing::warn!("payment {} AUTHORIZED, bank void transient error, retrying later", id);
                            }
                        }
                    }
                    None => {
                        tracing::warn!("payment {} AUTHORIZED but no bank reference; marking VOIDED locally", id);
                        let _ = self.repo.update_state(
                            id,
                            Some(PaymentState::Authorized),
                            PaymentState::Voided,
                            None,
                        ).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Process stranded idempotency keys in batches. For each key stuck in
    /// 'processing' beyond the TTL, look up its payment and either mark the
    /// key completed (work already done) or delete it (nothing happened,
    /// safe to retry).
    async fn reconcile_stale_idempotency_keys(&self) -> Result<(), GatewayError> {
        const BATCH: i64 = 100;
        const STALE_SECONDS: i64 = 300; // 5 minutes

        loop {
            let keys = self.repo.list_stale_processing_keys(STALE_SECONDS, BATCH).await?;
            if keys.is_empty() {
                break;
            }

            for (key, payment_id, order_id, _request_hash, _started_at) in keys {
                // Find the payment this key belongs to.
                let payment = match payment_id {
                    Some(id) => self.repo.get_payment(id).await?,
                    None => {
                        match order_id {
                            Some(ref oid) => self.repo.get_by_order(oid).await?,
                            None => None,
                        }
                    }
                };

                match payment {
                    Some(p) if p.state != PaymentState::Pending => {
                        // Work reached the bank and a state transition happened.
                        // Mark the key completed so retries replay this payment.
                        if let Err(e) = self.repo.mark_idempotency_completed_from_payment(&key, &p).await {
                            tracing::warn!("failed to complete idempotency key {}: {}", key, e);
                        }
                    }
                    Some(_) => {
                        // Payment still PENDING: no bank charge happened. Safe to
                        // drop the key so a retry can proceed without duplication.
                        if let Err(e) = self.repo.delete_idempotency_key(&key).await {
                            tracing::warn!("failed to delete idempotency key {}: {}", key, e);
                        }
                    }
                    None => {
                        // No payment found: orphaned key. Drop it.
                        if let Err(e) = self.repo.delete_idempotency_key(&key).await {
                            tracing::warn!("failed to delete orphaned idempotency key {}: {}", key, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
