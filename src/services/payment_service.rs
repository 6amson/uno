use crate::domain::models::{Payment, CardToken, PaymentState};
use crate::domain::errors::GatewayError;
use crate::storage::repository::Repository;
use crate::bank::client::{BankClient, AuthorizeRequest, CaptureRequest, VoidRequest, RefundRequest};
use crate::services::idempotency::{IdempotencyLayer, IdempotencyOutcome};
use uuid::Uuid;
use chrono::Utc;

#[derive(Clone)]
pub struct PaymentService {
    repo: Repository,
    bank: BankClient,
    idempotency: IdempotencyLayer,
}

impl PaymentService {
    pub fn new(repo: Repository, bank: BankClient, idempotency: IdempotencyLayer) -> Self {
        Self { repo, bank, idempotency }
    }

    /// Authorize a payment: validate idempotency, write PENDING receipt,
    /// call bank, transition PENDING -> AUTHORIZED, store bank reference.
    pub async fn authorize(
        &self,
        order_id: String,
        customer_id: String,
        amount_cents: i64,
        card_token: CardToken,
        idempotency_key: Option<String>,
    ) -> Result<Payment, GatewayError> {
        // Idempotency check/reserve (replay if already processed, reserve if fresh)
        if let Some(key) = &idempotency_key {
            match self.idempotency.check_or_reserve(
                key, &order_id, amount_cents, &card_token.last_four
            ).await? {
                IdempotencyOutcome::Replay(response_body) => {
                    // Replay: parse the stored response to return the same payment
                    let replay: Payment = serde_json::from_str(&response_body)
                        .map_err(|e| GatewayError::Internal(format!("bad cached response: {}", e)))?;
                    return Ok(replay);
                }
                IdempotencyOutcome::Reserved => {} // proceed to process
            }
        }

        // Write PENDING receipt (source of truth)
        let id = Uuid::new_v4();
        let now = Utc::now();
        let payment = Payment {
            id,
            order_id: order_id.clone(),
            customer_id: customer_id.clone(),
            amount_cents,
            currency: "USD".to_string(),
            state: PaymentState::Pending,
            bank_reference: None,
            last_four: Some(card_token.last_four.clone()),
            created_at: now,
            updated_at: now,
        };
        self.repo.create_payment(&payment).await?;

        // Mark in-flight before bank call
        self.repo.claim_inflight_upsert(id).await?;

        // Call bank
        let bank_resp = self.bank.authorize(AuthorizeRequest {
            card_number: card_token.card_number.clone(),
            cvv: card_token.cvv.clone(),
            expiry_month: card_token.expiry_month,
            expiry_year: card_token.expiry_year,
            amount: amount_cents,
        }, idempotency_key.as_deref()).await;

        let bank_resp = match bank_resp {
            Ok(r) => r,
            Err(e) => {
                // Bank call failed; payment stays PENDING. Recovery worker will resolve.
                return Err(GatewayError::Bank(e.to_string()));
            }
        };

        // Transition PENDING -> AUTHORIZED
        let updated = self.repo.update_state(
            id,
            Some(PaymentState::Pending),
            PaymentState::Authorized,
            Some(bank_resp.authorization_id.clone()),
        ).await?;

        // Store idempotency response (before clearing inflight, so a crash
        // between the two leaves the inflight marker for recovery to find).
        if let Some(key) = &idempotency_key {
            let response_body = serde_json::to_string(&updated)
                .map_err(|e| GatewayError::Internal(format!("serialize: {}", e)))?;
            let request_hash = IdempotencyLayer::hash_request(
                &order_id, amount_cents, &card_token.last_four
            );
            self.idempotency.complete(key, &request_hash, Some(id), &response_body).await?;
        }

        // Clear inflight marker
        let _ = self.repo.clear_inflight(id).await;

        Ok(updated)
    }

    /// Capture: transition AUTHORIZED -> CAPTURED, call bank capture.
    pub async fn capture(
        &self,
        payment_id: Uuid,
        idempotency_key: Option<String>,
    ) -> Result<Payment, GatewayError> {
        // Idempotency check/reserve; fetch payment to reconstruct the request hash
        let payment_for_hash = if idempotency_key.is_some() {
            self.repo.get_payment(payment_id).await?
        } else {
            None
        };
        if let Some(p) = payment_for_hash.as_ref() {
            if let Some(key) = idempotency_key.as_ref() {
                match self.idempotency.check_or_reserve(
                    key, &p.order_id, p.amount_cents, p.last_four.as_deref().unwrap_or("")
                ).await? {
                    IdempotencyOutcome::Replay(response_body) => {
                        let replay: Payment = serde_json::from_str(&response_body)
                            .map_err(|e| GatewayError::Internal(format!("bad cached response: {}", e)))?;
                        return Ok(replay);
                    }
                    IdempotencyOutcome::Reserved => {}
                }
            }
        }

        let payment = self.repo.get_payment(payment_id).await?
            .ok_or_else(|| GatewayError::NotFound(payment_id.to_string()))?;

        // State machine guard
        if payment.state != PaymentState::Authorized {
            return Err(GatewayError::InvalidTransition {
                from: format!("{:?}", payment.state),
                to: "Captured".to_string(),
            });
        }

        let bank_ref = payment.bank_reference.ok_or_else(|| GatewayError::Internal(
            "no bank reference for authorized payment".into()
        ))?;

        // Mark in-flight
        self.repo.claim_inflight_upsert(payment_id).await?;

        // Call bank
        let bank_resp = self.bank.capture(CaptureRequest {
            authorization_id: bank_ref.clone(),
            amount: payment.amount_cents,
        }, idempotency_key.as_deref()).await;

        let bank_resp = match bank_resp {
            Ok(r) => r,
            Err(e) => return Err(GatewayError::Bank(e.to_string())),
        };

        // Transition AUTHORIZED -> CAPTURED
        let updated = self.repo.update_state(
            payment_id,
            Some(PaymentState::Authorized),
            PaymentState::Captured,
            Some(bank_resp.capture_id.clone()),
        ).await?;

        // Store idempotency response (before clearing inflight, so a crash
        // between the two leaves the inflight marker for recovery to find).
        if let Some(key) = &idempotency_key {
            let response_body = serde_json::to_string(&updated)
                .map_err(|e| GatewayError::Internal(format!("serialize: {}", e)))?;
            let request_hash = IdempotencyLayer::hash_request(
                &payment.order_id, payment.amount_cents, &payment.last_four.unwrap_or_default()
            );
            self.idempotency.complete(key, &request_hash, Some(payment_id), &response_body).await?;
        }

        let _ = self.repo.clear_inflight(payment_id).await;

        Ok(updated)
    }

    /// Void: transition AUTHORIZED -> VOIDED, call bank void.
    pub async fn void(
        &self,
        payment_id: Uuid,
        idempotency_key: Option<String>,
    ) -> Result<Payment, GatewayError> {
        let payment_for_hash = if idempotency_key.is_some() {
            self.repo.get_payment(payment_id).await?
        } else {
            None
        };
        if let Some(p) = payment_for_hash.as_ref() {
            if let Some(key) = idempotency_key.as_ref() {
                match self.idempotency.check_or_reserve(
                    key, &p.order_id, p.amount_cents, p.last_four.as_deref().unwrap_or("")
                ).await? {
                    IdempotencyOutcome::Replay(response_body) => {
                        let replay: Payment = serde_json::from_str(&response_body)
                            .map_err(|e| GatewayError::Internal(format!("bad cached response: {}", e)))?;
                        return Ok(replay);
                    }
                    IdempotencyOutcome::Reserved => {}
                }
            }
        }

        let payment = self.repo.get_payment(payment_id).await?
            .ok_or_else(|| GatewayError::NotFound(payment_id.to_string()))?;

        if payment.state != PaymentState::Authorized {
            return Err(GatewayError::InvalidTransition {
                from: format!("{:?}", payment.state),
                to: "Voided".to_string(),
            });
        }

        let bank_ref = payment.bank_reference.ok_or_else(|| GatewayError::Internal(
            "no bank reference for authorized payment".into()
        ))?;

        self.repo.claim_inflight_upsert(payment_id).await?;

        let bank_resp = self.bank.void(VoidRequest { authorization_id: bank_ref.clone() }, idempotency_key.as_deref()).await;
        let bank_resp = match bank_resp {
            Ok(r) => r,
            Err(e) => return Err(GatewayError::Bank(e.to_string())),
        };

        let updated = self.repo.update_state(
            payment_id,
            Some(PaymentState::Authorized),
            PaymentState::Voided,
            Some(bank_resp.void_id.clone()),
        ).await?;

        // Store idempotency response (before clearing inflight, so a crash
        // between the two leaves the inflight marker for recovery to find).
        if let Some(key) = &idempotency_key {
            let response_body = serde_json::to_string(&updated)
                .map_err(|e| GatewayError::Internal(format!("serialize: {}", e)))?;
            let request_hash = IdempotencyLayer::hash_request(
                &payment.order_id, payment.amount_cents, &payment.last_four.unwrap_or_default()
            );
            self.idempotency.complete(key, &request_hash, Some(payment_id), &response_body).await?;
        }

        let _ = self.repo.clear_inflight(payment_id).await;

        Ok(updated)
    }

    /// Refund: transition CAPTURED -> REFUNDED, call bank refund.
    pub async fn refund(
        &self,
        payment_id: Uuid,
        idempotency_key: Option<String>,
    ) -> Result<Payment, GatewayError> {
        let payment_for_hash = if idempotency_key.is_some() {
            self.repo.get_payment(payment_id).await?
        } else {
            None
        };
        if let Some(p) = payment_for_hash.as_ref() {
            if let Some(key) = idempotency_key.as_ref() {
                match self.idempotency.check_or_reserve(
                    key, &p.order_id, p.amount_cents, p.last_four.as_deref().unwrap_or("")
                ).await? {
                    IdempotencyOutcome::Replay(response_body) => {
                        let replay: Payment = serde_json::from_str(&response_body)
                            .map_err(|e| GatewayError::Internal(format!("bad cached response: {}", e)))?;
                        return Ok(replay);
                    }
                    IdempotencyOutcome::Reserved => {}
                }
            }
        }

        let payment = self.repo.get_payment(payment_id).await?
            .ok_or_else(|| GatewayError::NotFound(payment_id.to_string()))?;

        if payment.state != PaymentState::Captured {
            return Err(GatewayError::InvalidTransition {
                from: format!("{:?}", payment.state),
                to: "Refunded".to_string(),
            });
        }

        let bank_ref = payment.bank_reference.ok_or_else(|| GatewayError::Internal(
            "no bank reference for captured payment".into()
        ))?;

        self.repo.claim_inflight_upsert(payment_id).await?;

        let bank_resp = self.bank.refund(RefundRequest {
            capture_id: bank_ref.clone(),
            amount: payment.amount_cents,
        }, idempotency_key.as_deref()).await;

        let bank_resp = match bank_resp {
            Ok(r) => r,
            Err(e) => return Err(GatewayError::Bank(e.to_string())),
        };

        let updated = self.repo.update_state(
            payment_id,
            Some(PaymentState::Captured),
            PaymentState::Refunded,
            Some(bank_resp.refund_id.clone()),
        ).await?;

        // Store idempotency response (before clearing inflight, so a crash
        // between the two leaves the inflight marker for recovery to find).
        if let Some(key) = &idempotency_key {
            let response_body = serde_json::to_string(&updated)
                .map_err(|e| GatewayError::Internal(format!("serialize: {}", e)))?;
            let request_hash = IdempotencyLayer::hash_request(
                &payment.order_id, payment.amount_cents, &payment.last_four.unwrap_or_default()
            );
            self.idempotency.complete(key, &request_hash, Some(payment_id), &response_body).await?;
        }

        let _ = self.repo.clear_inflight(payment_id).await;

        Ok(updated)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Payment>, GatewayError> {
        self.repo.get_payment(id).await
    }

    pub async fn get_by_order(&self, order_id: &str) -> Result<Option<Payment>, GatewayError> {
        self.repo.get_by_order(order_id).await
    }

    pub async fn list_by_customer(&self, customer_id: &str) -> Result<Vec<Payment>, GatewayError> {
        self.repo.list_by_customer(customer_id).await
    }
}
