use thiserror::Error;

#[derive(Error, Debug)]
pub enum GatewayError {
    #[error("invalid state transition: {from:?} -> {to:?}")]
    InvalidTransition { from: String, to: String },
    #[error("payment not found: {0}")]
    NotFound(String),
    #[error("idempotency key already used with different request")]
    IdempotencyConflict,
    #[error("bank error: {0}")]
    Bank(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("internal error: {0}")]
    Internal(String),
}
