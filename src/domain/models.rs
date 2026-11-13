use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "payment_state", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaymentState {
    Pending,
    Authorized,
    Captured,
    Refunded,
    Voided,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Payment {
    pub id: Uuid,
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
    pub state: PaymentState,
    pub bank_reference: Option<String>,
    pub last_four: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct StateHistory {
    pub id: Uuid,
    pub payment_id: Uuid,
    pub from_state: Option<PaymentState>,
    pub to_state: PaymentState,
    pub bank_reference: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardToken {
    pub card_number: String,
    pub cvv: String,
    pub last_four: String,
    pub expiry_month: u8,
    pub expiry_year: u16,
}
