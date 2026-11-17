# Architecture — Uno Payment Gateway

This document describes the internal design, component boundaries, data flow, and key
engineering decisions made for the Uno payment gateway.

**Repository:** https://github.com/6amson/uno

## Related Documents

- [README](../README.md) — run instructions, endpoints, quick start
- [Features](FEATURES.md) — all implemented features, what's not implemented, testing, configuration

## 1. High-Level Overview

Uno is a Rust (Axum + SQLx + Tokio) payment gateway that sits between a merchant
front-end and a mock bank API (`localhost:8787`). It handles the full card payment
lifecycle: authorize → capture → void → refund, with idempotency, optimistic state
transitions, in-flight crash recovery, and authorization TTL sweeps.

The service is stateless except for Postgres, which is the source of truth for
payments, idempotency keys, in-flight markers, and state history.

### External Dependencies

| Component | Location | Notes |
|-----------|----------|-------|
| Uno Gateway | https://github.com/6amson/uno | This repository |
| Mock Bank API | https://github.com/benx421/payment-gateway/tree/main/bank | External dependency |
| Postgres | `DATABASE_URL` | Required, configured via env var |

The mock bank API is **external infrastructure** your gateway talks to over HTTP.
It is **not** part of this repository and is **not** versioned here. It provides:
- `POST /api/v1/authorizations`, `/api/v1/captures`, `/api/v1/voids`, `/api/v1/refunds`
- `GET` endpoints for each resource
- `Idempotency-Key` header requirement on all POSTs
- Chaos failures (~5% random 500s, 100-2000ms latency)
- 7-day authorization expiry

See `FEATURES.md` for full bank API endpoint details and test card numbers.

## 2. Module Structure

```
src/
├── main.rs             — Entry point. Wire-up, migrations, service construction,
│                         recovery worker spawn, HTTP server start.
├── domain/
│   ├── models.rs       — Payment, StateHistory, CardToken (SQLx FromRow types)
│   ├── errors.rs       — GatewayError enum (InvalidTransition, NotFound,
│   │                   IdempotencyConflict, Bank, Storage, Validation, Internal)
│   ├── state.rs        — Pure state machine (Pending → Authorized → Captured → Refunded,
│   │                   Authorized → Voided). No I/O.
│   └── mod.rs
├── bank/
│   ├── client.rs       — BankClient. HTTP client to mock bank. Retry logic,
│   │                     Idempotency-Key header forwarding, 4xx→Permanent,
│   │                     5xx/408→Transient with backoff.
│   └── mod.rs
├── storage/
│   ├── mod.rs
│   ├── repository.rs   — Repository. All SQL queries: payments, idempotency_keys,
│   │                   inflight, state_history. Batch queries for recovery sweeps.
│   └── migrations/
│       ├── 0001_init.sql            — payments, state_history, idempotency_keys,
│       │                            inflight tables
│       ├── 0002_idempotency_status.sql — status column on idempotency_keys
│       ├── 0003_idempotency_payment_link.sql — payment_id, order_id, started_at
│       │                            on idempotency_keys
│       └── 0005_authorized_at.sql   — authorized_at column on payments
└── services/
    ├── mod.rs
    ├── payment_service.rs   — Core business logic. Authorize, capture, void, refund.
    │                          Idempotency check/reserve/complete flow.
    ├── idempotency.rs       — IdempotencyLayer. Atomic key reservation via
    │                          INSERT ... ON CONFLICT DO NOTHING. Poll-for-complete
    │                          for concurrent retries. Memory cache (LRU-ish eviction)
    │                          with Postgres as source of truth.
    └── recovery.rs          — RecoveryWorker. Background loop. Recovers stuck
    │                          inflight payments, stranded processing idempotency
    │                          keys, and stale authorized payments (TTL void sweep).
    │                          Runs every N seconds (default 30s).
└── api/
    ├── handlers.rs        — Axum route handlers. JSON request/response.
    │                        Error bodies returned as JSON.
    ├── router.rs          — Route registration + Swagger UI (/docs) + OpenAPI spec
    │                        (/openapi.json).
    └── mod.rs
```

## 3. Data Flow

### Authorize

```
Client → POST /payments/authorize (JSON body + idempotency_key)
  │
  ▼
PaymentService::authorize
  │
  ├─► IdempotencyLayer::check_or_reserve(key, order_id, amount, last_four)
  │     ├─ Memory cache hit (completed, not expired) → Replay cached response
  │     ├─ Memory cache miss → Postgres INSERT ... ON CONFLICT DO NOTHING
  │     │     ├─ Won race → Reserved → proceed
  │     │     └─ Lost race → Poll Postgres every 100ms (max 5s) until completed
  │     │           or conflict (different request hash)
  │     └─ Return Reserved or Replay(body)
  │
  ├─ If Reserved:
  │     ├─ INSERT payments (state=PENDING, bank_reference=NULL)
  │     ├─ claim_inflight_upsert(id) — mark inflight before bank call
  │     ├─ BankClient::authorize(...) → bank
  │     │     ├─ Success → update_state(PENDING → AUTHORIZED, bank_reference)
  │     │     │     ├─ set_authorized_at(id, now) — TTL tracking
  │     │     │     ├─ complete_idempotency(key, payment_id, response_body)
  │     │     │     ├─ clear_inflight(id)
  │     │     │     └─ Return Payment(Authorized)
  │     │     └─ BankError → delete_idempotency_key(key) + clear_inflight(id)
  │     │           → Return GatewayError::Bank (500 to client)
  │     │
  │     └─ Clear inflight
  │
  └─ Return JSON response to client
```

### Capture / Void / Refund

Same pattern: idempotency check/reserve → state machine guard → bank call → state
transition → complete idempotency → clear inflight.

**Capture** guard: `AUTHORIZED → CAPTURED`, plus authorization TTL check (7 days).
**Void** guard: `AUTHORIZED → VOIDED`.
**Refund** guard: `CAPTURED → REFUNDED`.

## 4. Idempotency Design

### The Problem
Retries (client-side, load balancer, or network) can hit the gateway multiple
times for the same logical payment. Without idempotency, each retry creates a new
payment and debits the customer multiple times.

### Solution: Atomic Key Reservation

`idempotency_keys` table:
```sql
key TEXT PRIMARY KEY,
request_hash TEXT NOT NULL,
response_body TEXT NULL,       -- NULL while processing
status TEXT NOT NULL DEFAULT 'processing',  -- 'processing' | 'completed'
payment_id UUID REFERENCES payments(id),
order_id TEXT,
started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

`check_or_reserve(key, order_id, amount_cents, card_last_four)`:

1. **Memory fast path**: Check `HashMap` in memory for completed entries (TTL 30 min).
2. **Atomic reserve**: `INSERT ... ON CONFLICT DO NOTHING`. If 1 row inserted → caller
   owns the key (`Reserved`). If 0 rows → another caller owns it.
3. **Loser poll**: Poll Postgres every 100ms (max ~5s). When status becomes `completed`,
   return cached `response_body`. If stored `request_hash` ≠ current hash →
   `IdempotencyConflict`.

### Why a `status` column?

Without `processing`/`completed` status, a crashed worker that had already called
the bank but not yet written the response body would leave the key in limbo.
Retries would either block forever or create duplicate payments. The `status`
column lets the recovery worker distinguish "in progress" from "done".

### Race Condition Handling

Two concurrent requests with the same key:
- Winner: creates payment, calls bank, completes key.
- Loser: polls until winner completes, then replays cached response.
- No duplicate bank calls, no duplicate payments.

### Crash Windows

| Crash point | State | Recovery |
|-------------|-------|----------|
| After reserve, before bank call | `processing`, payment `PENDING`, inflight set | Recovery voids/invalidates, key stays `processing` → retry creates new payment |
| After bank call, before `complete_idempotency` | `processing`, payment `AUTHORIZED`, inflight cleared | Key stranded `processing`. Recovery `reconcile_stale_idempotency_keys` sees terminal payment, marks key `completed` with cached response. Retry replays. |
| After `complete_idempotency`, before `clear_inflight` | `completed`, payment `AUTHORIZED`, inflight set | Recovery sees inflight + `AUTHORIZED`, voids at bank, transitions to `VOIDED`. |

## 5. State Machine

Defined in `src/domain/state.rs`. Pure, no I/O.

```
PENDING  → AUTHORIZED   (authorize)
AUTHORIZED → CAPTURED   (capture)
CAPTURED  → REFUNDED    (refund)
AUTHORIZED → VOIDED     (void)
```

All other transitions return `GatewayError::InvalidTransition`.

### Why a pure state machine?

- Testable without a database (unit tests in `state.rs`)
- Easy to reason about legal transitions
- Guards against double-capture, refund-before-capture, etc.

## 6. In-Flight Markers & Crash Recovery

`inflight` table:
```sql
payment_id UUID PRIMARY KEY REFERENCES payments(id),
started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
attempt INT NOT NULL DEFAULT 1
```

Set via `claim_inflight_upsert(id)` before every bank call. Cleared via
`clear_inflight(id)` after the bank call completes (success or failure).

`RecoveryWorker` runs every 30 seconds (configurable via `Duration`):

1. `list_stuck(300)` — payments with inflight marker older than 5 minutes,
   state `PENDING` or `AUTHORIZED`.
2. Atomically claim each via `claim_inflight(id)` (UPDATE attempt counter).
3. Per payment:
   - `PENDING` → can't re-authorize (no full card details stored). Leave for
     manual intervention. Clear inflight.
   - `AUTHORIZED` → try `bank.void(...)`. If success → `VOIDED`. If bank error,
     leave `AUTHORIZED` (bank may not have the auth either). Clear inflight.
   - Terminal states → clear inflight.
4. `reconcile_stale_idempotency_keys()` — find `processing` idempotency keys
   older than 5 minutes, in batches of 100. If linked payment is terminal,
   mark key `completed` with serialized payment. If `PENDING` or missing,
   delete the key (safe retry).
5. `reconcile_stale_authorized_payments()` — find `AUTHORIZED` payments older
   than 7 days (`authorized_at < NOW() - 7d`), in batches of 100. Attempt bank
   void. On success → `VOIDED`. On bank error → log and leave for next sweep.

## 7. Authorization Expiry

Card network authorizations typically expire after 7 days. Uno enforces this at
the gateway level:

- `authorized_at` column on `payments` (set during authorize).
- Capture rejects with `BankError` if `authorized_at < NOW() - 7 days`.
- Recovery worker sweeps stale `AUTHORIZED` payments every cycle and voids them
  at the bank.

This prevents stale authorizations from being captured days or weeks later when
the bank would have already released the hold.

## 8. Bank API Integration

`BankClient` in `src/bank/client.rs`:

- Base URL configurable via `BANK_URL` env (default `http://localhost:8787`).
- Endpoints: `/api/v1/authorizations`, `/api/v1/captures`, `/api/v1/voids`,
  `/api/v1/refunds`.
- All POST endpoints require `Idempotency-Key` header (forwarded from gateway).
- Retry logic: HTTP 5xx or 408 → transient, retry with exponential backoff
  (100ms, 200ms, 400ms, up to 4 attempts). HTTP 4xx → permanent, no retry.
- Bank responses are parsed into typed structs (`AuthorizeResponse`,
  `CaptureResponse`, `VoidResponse`, `RefundResponse`).
- Bank errors (4xx with `{"error": "...", "message": "..."}` body) are surfaced
  as `BankError::Permanent` with HTTP status and body text included.

## 9. Database Schema

### `payments`
```sql
id UUID PRIMARY KEY,
order_id TEXT NOT NULL,
customer_id TEXT NOT NULL,
amount_cents BIGINT NOT NULL,
currency TEXT NOT NULL DEFAULT 'USD',
state TEXT NOT NULL DEFAULT 'PENDING',  -- payment_state enum
bank_reference TEXT,
last_four TEXT,
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
authorized_at TIMESTAMPTZ            -- set on authorize, NULL otherwise
```

### `state_history`
Audit trail. One row per state transition. Includes `from_state`, `to_state`,
`bank_reference`, `created_at`.

### `idempotency_keys`
```sql
key TEXT PRIMARY KEY,
request_hash TEXT NOT NULL,
response_body TEXT NULL,
status TEXT NOT NULL DEFAULT 'processing',  -- 'processing' | 'completed'
payment_id UUID REFERENCES payments(id),
order_id TEXT,
started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

### `inflight`
```sql
payment_id UUID PRIMARY KEY REFERENCES payments(id),
started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
attempt INT NOT NULL DEFAULT 1
```

### Indexes
- `payments(order_id)`, `payments(customer_id)`, `payments(state)`
- `payments(authorized_at) WHERE state = 'AUTHORIZED'` (partial, for sweep)
- `idempotency_keys(status, started_at)` (for stale key sweep)
- `idempotency_keys(order_id)` (for recovery lookup by order)

## 10. HTTP API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/payments/authorize` | Create authorization hold |
| POST | `/payments/{id}/capture` | Capture authorized funds |
| POST | `/payments/{id}/void` | Void authorization |
| POST | `/payments/{id}/refund` | Refund captured payment |
| GET | `/payments/{id}` | Get payment details |
| GET | `/orders/{order_id}/payments` | Get payment by order ID |
| GET | `/customers/{customer_id}/payments` | List payments by customer |
| GET | `/docs` | Swagger UI |
| GET | `/openapi.json` | OpenAPI 3.0.3 spec |

Authorize accepts `idempotency_key` in the JSON body. Capture/void/refund accept
`Idempotency-Key` as an HTTP header.

All error responses return JSON: `{"error": "<message>"}`.

## 11. Configuration

Environment variables:
- `DATABASE_URL` — Postgres connection string (required)
- `BANK_URL` — Mock bank base URL (default: `http://localhost:8787`)
- `LISTEN_ADDR` — HTTP listen address (default: `0.0.0.0:8080`)

`dotenv` is loaded at startup, so a `.env` file in the project root works.

## 12. Testing

- Unit tests: `cargo test --bin Uno` (state machine, idempotency hash)
- Integration tests: `sqlx::test` against a real Postgres database
  (`DATABASE_URL` must point to a test database). Tests cover full flows
  (authorize → capture → refund), idempotency replay/conflict, capture
  rejection on expired authorization, and recovery voiding of stale
  authorized payments.

Tests run against the live mock bank at `localhost:8787`.

## 13. Tradeoffs

See `TRADEOFFS.md` for detailed tradeoff analysis on idempotency, recovery
consistency, sweep intervals, and state machine design.

Key tradeoffs made:
- **Recovery is eventually consistent**: Stuck payments are resolved on the
  next sweep (default 30s), not immediately.
- **Idempotency key per operation**: Different keys for authorize/capture/void/
  refund. Reusing one key across operations would cause replay of the wrong
  response.
- **Authorization TTL = 7 days**: Matches typical card network limits. Can be
  adjusted via `AUTH_TTL_SECONDS` constant in `payment_service.rs`.
- **Memory cache for idempotency**: Per-instance, 30-minute TTL, 100k entry cap.
  Postgres is the source of truth; cache is a fast path only.
