use axum::{Router, routing::{get, post}};
use crate::services::payment_service::PaymentService;
use crate::services::recovery::RecoveryWorker;
use crate::bank::client::BankClient;
use crate::storage::repository::Repository;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub service: PaymentService,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/payments/{id}/capture", post(crate::api::handlers::capture))
        .route("/payments/{id}/void", post(crate::api::handlers::void_payment))
        .route("/payments/{id}/refund", post(crate::api::handlers::refund))
        .route("/payments/{id}", get(crate::api::handlers::get_payment))
        .route("/orders/{order_id}/payments", get(crate::api::handlers::get_by_order))
        .route("/customers/{customer_id}/payments", get(crate::api::handlers::list_by_customer))
        .route("/openapi.json", get(crate::api::handlers::openapi_spec))
        .route("/docs", get(crate::api::handlers::swagger_ui))
        .with_state(state)
}

pub async fn run(addr: SocketAddr, state: AppState) {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

pub fn spawn_recovery(repo: Repository, bank: BankClient, interval: Duration) -> tokio::task::JoinHandle<()> {
    let worker = RecoveryWorker::new(repo, bank, interval);
    tokio::spawn(async move { worker.run().await })
}
