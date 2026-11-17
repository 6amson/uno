# Uno — Payment Gateway

A Rust payment gateway with idempotency, state machine transitions, in-flight markers, and crash recovery.

**Repository:** https://github.com/6amson/uno

**Mock Bank API:** Provided externally at `localhost:8787` (part of the "Backend Engineer Path" project by benx421). This is external infrastructure your gateway talks to — not part of this repository.

## Documentation

- [Architecture & Design Decisions](ARCHITECTURE.md) — system design, data flow, module boundaries, database schema, HTTP API
- [Features](FEATURES.md) — all implemented features, what's not implemented, testing, configuration

## Stack

- Rust 2024, Axum, SQLx (Postgres), Tokio, Serde, SHA-2
- Mock bank API on `localhost:8787` (external dependency)

## Run

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/uno \
BANK_URL=http://localhost:8787 \
LISTEN_ADDR=0.0.0.0:8080 \
cargo run
```

Migrations run automatically on startup.

## Endpoints

| Method | Path | Body / Headers | Description |
|---|---|---|---|
| POST | `/payments/authorize` | `order_id`, `customer_id`, `amount_cents`, `card_number`, `cvv`, `card_last_four`, `card_expiry_month`, `card_expiry_year`, `idempotency_key?` | Authorize a payment |
| POST | `/payments/:id/capture` | `Idempotency-Key?` header | Capture an authorized payment |
| POST | `/payments/:id/void` | `Idempotency-Key?` header | Void an authorized payment |
| POST | `/payments/:id/refund` | `Idempotency-Key?` header | Refund a captured payment |
| GET | `/payments/:id` | | Get a payment |
| GET | `/orders/:order_id/payments` | | Get payment by order |
| GET | `/customers/:customer_id/payments` | | List payments by customer |

## Idempotency

Pass an `Idempotency-Key` (header for capture/void/refund, body field for authorize). The same key with the same payload replays the cached response; a different payload returns `IdempotencyConflict`.

## State Machine

`PENDING → AUTHORIZED → CAPTURED → REFUNDED` and `AUTHORIZED → VOIDED`. All other transitions are rejected.

## Recovery

A background worker scans for stuck payments (in-flight markers older than 5 minutes) every 30 seconds and reconciles stranded idempotency keys.

## Test

```bash
cargo test --bin Uno
```

Integration tests require a Postgres database via `DATABASE_URL` and the mock bank running at `localhost:8787`.

## Tradeoffs

See `TRADEOFFS.md` for design decisions around idempotency, recovery consistency, and polling intervals.

---

## Related Documents

- [Architecture & Design Decisions](ARCHITECTURE.md) — system design, data flow, module boundaries, database schema, HTTP API
- [Features](FEATURES.md) — all implemented features, what's not implemented, testing, configuration
