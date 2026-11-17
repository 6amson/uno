use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use thiserror::Error;
use std::time::Duration;

#[derive(Error, Debug)]
pub enum BankError {
    #[error("transient: {0}")]
    Transient(String),
    #[error("permanent: {0}")]
    Permanent(String),
    #[error("timeout")]
    Timeout,
    #[error("client: {0}")]
    Client(#[from] reqwest::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeRequest {
    pub card_number: String,
    pub cvv: String,
    pub expiry_month: u8,
    pub expiry_year: u16,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeResponse {
    pub authorization_id: String,
    pub status: String,
    pub amount: i64,
    pub currency: String,
    pub expires_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureRequest {
    pub authorization_id: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureResponse {
    pub capture_id: String,
    pub authorization_id: String,
    pub status: String,
    pub amount: i64,
    pub currency: String,
    pub captured_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoidRequest {
    pub authorization_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoidResponse {
    pub void_id: String,
    pub authorization_id: String,
    pub status: String,
    pub voided_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundRequest {
    pub capture_id: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundResponse {
    pub refund_id: String,
    pub capture_id: String,
    pub status: String,
    pub amount: i64,
    pub currency: String,
    pub refunded_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BankErrorResponse {
    pub error: String,
    pub message: String,
}

#[derive(Clone)]
pub struct BankClient {
    client: Client,
    base_url: String,
}

impl BankClient {
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");
        Self { client, base_url }
    }

    pub async fn authorize(
        &self,
        req: AuthorizeRequest,
        idempotency_key: Option<&str>,
    ) -> Result<AuthorizeResponse, BankError> {
        let url = format!("{}/api/v1/authorizations", self.base_url);
        let resp = self.retry(move || {
            let req_clone = req.clone();
            let mut request = self.client.post(&url).json(&req_clone);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            async move { request.send().await }
        }).await?;

        resp.json::<AuthorizeResponse>().await.map_err(BankError::Client)
    }

    pub async fn capture(
        &self,
        req: CaptureRequest,
        idempotency_key: Option<&str>,
    ) -> Result<CaptureResponse, BankError> {
        let url = format!("{}/api/v1/captures", self.base_url);
        let resp = self.retry(move || {
            let req_clone = req.clone();
            let mut request = self.client.post(&url).json(&req_clone);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            async move { request.send().await }
        }).await?;

        resp.json::<CaptureResponse>().await.map_err(BankError::Client)
    }

    pub async fn void(
        &self,
        req: VoidRequest,
        idempotency_key: Option<&str>,
    ) -> Result<VoidResponse, BankError> {
        let url = format!("{}/api/v1/voids", self.base_url);
        let resp = self.retry(move || {
            let req_clone = req.clone();
            let mut request = self.client.post(&url).json(&req_clone);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            async move { request.send().await }
        }).await?;

        resp.json::<VoidResponse>().await.map_err(BankError::Client)
    }

    pub async fn refund(
        &self,
        req: RefundRequest,
        idempotency_key: Option<&str>,
    ) -> Result<RefundResponse, BankError> {
        let url = format!("{}/api/v1/refunds", self.base_url);
        let resp = self.retry(move || {
            let req_clone = req.clone();
            let mut request = self.client.post(&url).json(&req_clone);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            async move { request.send().await }
        }).await?;

        resp.json::<RefundResponse>().await.map_err(BankError::Client)
    }

    /// Retry transient failures (HTTP 5xx, 408, timeouts, connection errors).
    /// Permanent failures (4xx) surface as BankError::Permanent without retrying.
    async fn retry<F, Fut>(&self, req: F) -> Result<reqwest::Response, BankError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut last_err: Option<BankError> = None;
        for attempt in 0..=3 {
            match req().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_server_error() || status.as_u16() == 408 {
                        last_err = Some(BankError::Transient(format!("HTTP {}", status)));
                        let backoff = Duration::from_millis(100 * (2u64.pow(attempt)));
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    if status.is_client_error() {
                        let status_code = status.as_u16();
                        let body_text = resp.text().await.unwrap_or_else(|_| String::new());
                        let error_body: BankErrorResponse = serde_json::from_str(&body_text).unwrap_or_default();
                        let error_detail = if error_body.error.is_empty() && error_body.message.is_empty() {
                            body_text
                        } else {
                            format!("{}: {}", error_body.error, error_body.message)
                        };
                        return Err(BankError::Permanent(format!(
                            "HTTP {}: {}",
                            status_code, error_detail
                        )));
                    }
                    return Ok(resp);
                }
                Err(e) if e.is_timeout() => {
                    last_err = Some(BankError::Timeout);
                    let backoff = Duration::from_millis(100 * (2u64.pow(attempt)));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) if e.is_connect() => {
                    last_err = Some(BankError::Transient(format!("connect: {}", e)));
                    let backoff = Duration::from_millis(100 * (2u64.pow(attempt)));
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => {
                    return Err(BankError::Client(e));
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| BankError::Transient("exhausted retries".into())))
    }

    // pub fn is_permanent(status: u16) -> bool {
    //     (400..=499).contains(&status)
    // }
}
