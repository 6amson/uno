mod domain;
mod bank;
mod storage;
mod services;
mod api;

use sqlx::PgPool;
use crate::api::router::{AppState, router, run, spawn_recovery};
use crate::services::payment_service::PaymentService;
use crate::services::recovery::RecoveryWorker;
use crate::bank::client::BankClient;
use crate::storage::repository::Repository;
use crate::services::idempotency::IdempotencyLayer;
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let bank_url = std::env::var("BANK_URL")
        .unwrap_or_else(|_| "http://localhost:8787".to_string());
    let addr: SocketAddr = std::env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("LISTEN_ADDR must be a valid socket address");

    let pool = PgPool::connect(&database_url).await.expect("failed to connect to Postgres");

    // Run migrations
    sqlx::migrate!("./src/storage/migrations")
        .run(&pool)
        .await.expect("failed to run migrations");

    let repo = Repository::new(pool.clone());
    let bank = BankClient::new(bank_url);
    let idempotency = IdempotencyLayer::new(repo.clone());
    let service = PaymentService::new(repo, bank.clone(), idempotency);

    // Start recovery worker in background
    spawn_recovery(Repository::new(pool), bank, Duration::from_secs(30));

    let state = AppState { service };

    println!("Gateway listening on {}", addr);
    run(addr, state).await;
}
