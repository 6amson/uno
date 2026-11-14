use axum::{Json, extract::{State, Path}, http::{HeaderMap, StatusCode}};
use serde::{Deserialize, Serialize};
use crate::api::router::AppState;
use crate::domain::models::CardToken;
use uuid::Uuid;
use tracing::error;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Deserialize)]
pub struct AuthorizeRequest {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub card_number: String,
    pub cvv: String,
    pub card_last_four: String,
    pub card_expiry_month: u8,
    pub card_expiry_year: u16,
    pub idempotency_key: Option<String>,
}

#[derive(Serialize)]
pub struct AuthorizeResponse {
    pub payment_id: String,
    pub state: String,
}

#[derive(Serialize)]
pub struct PaymentSummary {
    pub payment_id: String,
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
    pub state: String,
    pub bank_reference: Option<String>,
    pub last_four: Option<String>,
}

pub async fn authorize(
    State(state): State<AppState>,
    Json(req): Json<AuthorizeRequest>,
) -> Result<Json<AuthorizeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let card_token = CardToken {
        card_number: req.card_number,
        cvv: req.cvv,
        last_four: req.card_last_four,
        expiry_month: req.card_expiry_month,
        expiry_year: req.card_expiry_year,
    };

    let payment = state.service.authorize(
        req.order_id,
        req.customer_id,
        req.amount_cents,
        card_token,
        req.idempotency_key,
    ).await.map_err(|e| {
        error!("authorize failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    Ok(Json(AuthorizeResponse {
        payment_id: payment.id.to_string(),
        state: format!("{:?}", payment.state),
    }))
}

fn idempotency_key_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

pub async fn capture(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.capture(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("capture failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn void_payment(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.void(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("void failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn refund(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.refund(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("refund failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn get_payment(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
) -> Result<Json<PaymentSummary>, (StatusCode, Json<ErrorResponse>)> {
    let payment = state.service.get(payment_id).await
        .map_err(|e| {
            error!("get_payment failed for {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?
        .ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() })))?;

    Ok(Json(PaymentSummary {
        payment_id: payment.id.to_string(),
        order_id: payment.order_id,
        customer_id: payment.customer_id,
        amount_cents: payment.amount_cents,
        currency: payment.currency,
        state: format!("{:?}", payment.state),
        bank_reference: payment.bank_reference,
        last_four: payment.last_four,
    }))
}

pub async fn get_by_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<Json<PaymentSummary>, (StatusCode, Json<ErrorResponse>)> {
    let payment = state.service.get_by_order(&order_id).await
        .map_err(|e| {
            error!("get_by_order failed for {}: {e}", order_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?
        .ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() })))?;

    Ok(Json(PaymentSummary {
        payment_id: payment.id.to_string(),
        order_id: payment.order_id,
        customer_id: payment.customer_id,
        amount_cents: payment.amount_cents,
        currency: payment.currency,
        state: format!("{:?}", payment.state),
        bank_reference: payment.bank_reference,
        last_four: payment.last_four,
    }))
}

pub async fn list_by_customer(
    State(state): State<AppState>,
    Path(customer_id): Path<String>,
) -> Result<Json<Vec<PaymentSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let payments = state.service.list_by_customer(&customer_id).await
        .map_err(|e| {
            error!("list_by_customer failed for {}: {e}", customer_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?;

    Ok(Json(payments.into_iter().map(|p| PaymentSummary {
        payment_id: p.id.to_string(),
        order_id: p.order_id,
        customer_id: p.customer_id,
        amount_cents: p.amount_cents,
        currency: p.currency,
        state: format!("{:?}", p.state),
        bank_reference: p.bank_reference,
        last_four: p.last_four,
    }).collect()))
}
