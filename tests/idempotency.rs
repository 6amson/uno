use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrations = "./src/storage/migrations")]
async fn idempotency_reserve_and_replay(pool: PgPool) {
    let repo = crate::storage::repository::Repository::new(pool.clone());
    let idempotency = crate::services::idempotency::IdempotencyLayer::new(repo.clone());

    let key = "test-key-1";
    let order_id = "order-1";
    let amount = 1000;
    let card_last_four = "4242";
    let request_hash = crate::services::idempotency::IdempotencyLayer::hash_request(order_id, amount, card_last_four);

    // First caller reserves the key
    let outcome = idempotency.check_or_reserve(key, order_id, amount, card_last_four).await.unwrap();
    assert!(matches!(outcome, crate::services::idempotency::IdempotencyOutcome::Reserved));

    // Complete the key
    let response_body = r#"{"payment_id":"abc","state":"Authorized"}"#;
    idempotency.complete(key, &request_hash, None, response_body).await.unwrap();

    // Second caller with same key replays
    let outcome2 = idempotency.check_or_reserve(key, order_id, amount, card_last_four).await.unwrap();
    match outcome2 {
        crate::services::idempotency::IdempotencyOutcome::Replay(body) => {
            assert_eq!(body, response_body);
        }
        _ => panic!("expected Replay, got {:?}", outcome2),
    }
}

#[sqlx::test(migrations = "./src/storage/migrations")]
async fn idempotency_conflict_on_different_payload(pool: PgPool) {
    let repo = crate::storage::repository::Repository::new(pool.clone());
    let idempotency = crate::services::idempotency::IdempotencyLayer::new(repo.clone());

    let key = "test-key-conflict";
    let request_hash1 = crate::services::idempotency::IdempotencyLayer::hash_request("order-1", 1000, "4242");
    let request_hash2 = crate::services::idempotency::IdempotencyLayer::hash_request("order-1", 2000, "4242");

    // First caller reserves and completes
    let outcome = idempotency.check_or_reserve(key, "order-1", 1000, "4242").await.unwrap();
    assert!(matches!(outcome, crate::services::idempotency::IdempotencyOutcome::Reserved));

    idempotency.complete(key, &request_hash1, None, r#"{"payment_id":"abc"}"#).await.unwrap();

    // Second caller with same key but different payload
    let outcome2 = idempotency.check_or_reserve(key, "order-1", 2000, "4242").await;
    assert!(matches!(outcome2, Err(crate::domain::errors::GatewayError::IdempotencyConflict)));
}

#[sqlx::test(migrations = "./src/storage/migrations")]
async fn recovery_reconciles_stranded_processing_key(pool: PgPool) {
    let repo = crate::storage::repository::Repository::new(pool.clone());

    // Insert a payment directly
    let payment_id = Uuid::new_v4();
    let now = chrono::Utc::now();
    sqlx::query(
        r#"
        INSERT INTO payments (id, order_id, customer_id, amount_cents, currency, state, bank_reference, last_four, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(payment_id)
    .bind("order-1")
    .bind("customer-1")
    .bind(1000i64)
    .bind("USD")
    .bind("Authorized")
    .bind("auth_ref_123")
    .bind("4242")
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();

    // Insert a stranded processing idempotency key linked to the payment
    let key = "stranded-key-1";
    let request_hash = crate::services::idempotency::IdempotencyLayer::hash_request("order-1", 1000, "4242");
    let five_minutes_ago = now - chrono::Duration::minutes(6);

    sqlx::query(
        r#"
        INSERT INTO idempotency_keys (key, request_hash, response_body, status, payment_id, order_id, started_at)
        VALUES ($1, $2, NULL, 'processing', $3, $4, $5)
        "#,
    )
    .bind(key)
    .bind(&request_hash)
    .bind(payment_id)
    .bind("order-1")
    .bind(five_minutes_ago)
    .execute(&pool)
    .await
    .unwrap();

    // Find stale processing keys
    let stale_keys = repo.list_stale_processing_keys(300, 100).await.unwrap();
    assert_eq!(stale_keys.len(), 1);
    assert_eq!(stale_keys[0].0, key);

    // Mark it completed from payment (simulating recovery reconciliation)
    let payment = repo.get_payment(payment_id).await.unwrap().unwrap();
    repo.mark_idempotency_completed_from_payment(key, &payment).await.unwrap();

    // Key should now be completed with response body
    let row = repo.get_idempotency(key).await.unwrap().unwrap();
    assert_eq!(row.2, "completed"); // status
    assert!(row.1.is_some()); // response_body present
}
