# Features — Uno Payment Gateway

This document lists every implemented feature in the Uno payment gateway, grouped
by functional area, with notes on how each works and where to find the code.

## 1. Payment Lifecycle

### Authorize (POST /payments/authorize)
- Creates a new payment in `PENDING` state
- Calls the bank to place an authorization hold on funds
- On success: transitions `PENDING → AUTHORIZED`, stores the bank `authorization_id`
- On bank decline/failure: payment stays `PENDING`, returns error
- Requires: `order_id`, `customer_id`, `amount_cents`, `card_number`, `cvv`,
  `card_last_four`, `card_expiry_month`, `card_expiry_year`, optional
  `idempotency_key`

### Capture (POST /payments/{id}/capture)
- Transitions `AUTHORIZED → CAPTURED`
- Calls the bank to collect the reserved funds
- Requires `Idempotency-Key` header
- Rejects if payment is not `AUTHORIZED` (e.g., already captured, voided, or refunded)
- Rejects if authorization has expired (7-day TTL)

### Void (POST /payments/{id}/void)
- Transitions `AUTHORIZED → VOIDED`
- Calls the bank to release the authorization hold
- Requires `Idempotency-Key` header
- Rejects if payment is not `AUTHORIZED`

### Refund (POST /payments/{id}/refund)
- Transitions `CAPTURED → REFUNDED`
- Calls the bank to refund captured funds
- Requires `Idempotency-Key` header
- Rejects if payment is not `CAPTURED`

### Payment Lookup
- `GET /payments/{id}` — get a single payment by UUID
- `GET /orders/{order_id}/payments` — get payment by merchant order ID
- `GET /customers/{customer_id}/payments` — list all payments for a customer

## 2. Idempotency

### Per-Operation Keys
Each operation (authorize, capture, void, refund) requires its own idempotency
key. Authorize takes the key in the JSON body; capture/void/refund take it as an
`Idempotency-Key` HTTP header.

### Guarantees
- **Same key + same payload** → returns the cached response from the first call
  (no duplicate bank call, no duplicate payment)
- **Same key + different payload** → `IdempotencyConflict` error
- **Different key** → treated as a new request

### Implementation
- `idempotency_keys` table with `key` (primary key), `request_hash`, `response_body`,
  `status` (`processing`/`completed`), `payment_id`, `order_id`, `started_at`
- Atomic reservation via `INSERT ... ON CONFLICT DO NOTHING`
- Losers of the reservation race poll Postgres every 100ms (max ~5s) until the
  winner completes
- Memory cache (per-instance, 30-minute TTL, 100k entry cap) for fast replays
- Postgres is the source of truth

### Crash Safety
If a worker crashes after the bank call but before marking the key `completed`,
the key stays `processing`. The recovery worker reconciles stranded keys:
- If the linked payment is terminal → mark key `completed` with cached response
- If the payment is still `PENDING` → delete the key (safe retry)

## 3. State Machine

Pure, testable, no I/O. Defined in `src/domain/state.rs`.

```
PENDING  → AUTHORIZED   (authorize)
AUTHORIZED → CAPTURED   (capture)
CAPTURED  → REFUNDED    (refund)
AUTHORIZED → VOIDED     (void)
```

All other transitions are rejected with `GatewayError::InvalidTransition`.

### Tested Transitions
- Happy path: PENDING → AUTHORIZED → CAPTURED → REFUNDED
- Reject: capture before authorize, refund before capture, capture after void,
  refund after void, etc.
- Terminal states (REFUNDED, VOIDED) have no outgoing transitions

## 4. Crash Recovery

`RecoveryWorker` runs in the background every 30 seconds (configurable).

### Stuck Inflight Payments
Payments with an `inflight` marker older than 5 minutes, in state `PENDING` or
`AUTHORIZED`:
- `PENDING` → cannot re-authorize (no full card details stored). Left for manual
  intervention. Inflight marker cleared.
- `AUTHORIZED` → attempt bank `void()`. On success → `VOIDED`. On bank error,
  leave `AUTHORIZED`. Inflight marker cleared.
- Terminal → inflight marker cleared.

### Stranded Idempotency Keys
Keys stuck in `processing` status older than 5 minutes, processed in batches of 100:
- Linked payment is terminal → mark key `completed` with serialized payment as
  cached response (retries replay correctly)
- Linked payment is `PENDING` or missing → delete the key (safe retry)

### Stale Authorized Payments
`AUTHORIZED` payments older than 7 days (based on `authorized_at` timestamp),
processed in batches of 100:
- Attempt bank `void()` to release the hold
- On success → transition locally to `VOIDED`
- On bank error → log warning, leave for next sweep

## 5. Authorization Expiry

Card network authorizations typically expire after 7 days. Uno enforces this:

- `authorized_at` column on `payments`, set when a payment transitions to
  `AUTHORIZED`
- Capture rejects with `BankError` if `authorized_at` is older than 7 days
- Recovery worker voids stale `AUTHORIZED` payments every cycle

## 6. Bank API Integration

Mock bank at `localhost:8787` (configurable via `BANK_URL`).

### Endpoints
- `POST /api/v1/authorizations` — place authorization hold
- `POST /api/v1/captures` — capture authorized hold
- `POST /api/v1/voids` — cancel authorization hold
- `POST /api/v1/refunds` — refund captured payment

### Requirements
- All POST endpoints require `Idempotency-Key` header (forwarded from gateway)
- Bank errors (4xx with `{"error": "...", "message": "..."}` body) are surfaced
  as `BankError::Permanent` with HTTP status and body text

### Retry Logic
- HTTP 5xx or 408 → transient, retry with exponential backoff
  (100ms, 200ms, 400ms, up to 4 attempts)
- HTTP 4xx → permanent, no retry
- Network timeouts/connect errors → transient, retry

## 7. Database

Managed via SQLx migrations in `src/storage/migrations/`:

| Migration | Description |
|-----------|-------------|
| `0001_init.sql` | `payments`, `state_history`, `idempotency_keys`, `inflight` tables |
| `0002_idempotency_status.sql` | Add `status` column to `idempotency_keys` |
| `0003_idempotency_payment_link.sql` | Add `payment_id`, `order_id`, `started_at` to `idempotency_keys` |
| `0005_authorized_at.sql` | Add `authorized_at` to `payments` |

### Tables
- `payments` — core payment records with state, bank reference, timestamps
- `state_history` — audit trail of every state transition
- `idempotency_keys` — idempotency key coordination
- `inflight` — in-flight markers for crash recovery

### Indexes
- `payments(order_id)`, `payments(customer_id)`, `payments(state)`
- `payments(authorized_at) WHERE state = 'AUTHORIZED'` (partial, for stale sweep)
- `idempotency_keys(status, started_at)` (for stale key sweep)
- `idempotency_keys(order_id)` (for recovery lookup)

## 8. HTTP API

| Method | Path | Body | Headers | Description |
|--------|------|------|---------|-------------|
| POST | `/payments/authorize` | JSON (see below) | — | Authorize payment |
| POST | `/payments/{id}/capture` | — | `Idempotency-Key` | Capture funds |
| POST | `/payments/{id}/void` | — | `Idempotency-Key` | Void authorization |
| POST | `/payments/{id}/refund` | — | `Idempotency-Key` | Refund payment |
| GET | `/payments/{id}` | — | — | Get payment |
| GET | `/orders/{order_id}/payments` | — | — | Get by order ID |
| GET | `/customers/{customer_id}/payments` | — | — | List by customer |
| GET | `/docs` | — | — | Swagger UI |
| GET | `/openapi.json` | — | — | OpenAPI spec |

### Authorize Request Body
```json
{
  "order_id": "string",
  "customer_id": "string",
  "amount_cents": 1000,
  "card_number": "4111111111111111",
  "cvv": "123",
  "card_last_four": "1111",
  "card_expiry_month": 12,
  "card_expiry_year": 2026,
  "idempotency_key": "optional-string"
}
```

### Error Response
```json
{ "error": "human-readable message" }
```

## 8. Testing

### Unit Tests (`cargo test --bin Uno`)
- State machine: happy path, invalid transitions, terminal states
- Idempotency hash: deterministic, differs by amount/card/order, 64 hex chars

### Integration Tests (`cargo test --bin Uno`, requires `DATABASE_URL` and bank at `localhost:8787`)
- Full flow: authorize → capture → refund
- Capture → refund flow
- Idempotency reserve → replay (same key) → conflict (different payload)
- Capture rejected when authorization expired (7-day TTL)
- Recovery voids stale authorized payment

## 9. Configuration

| Env Var | Description | Default |
|---------|-------------|---------|
| `DATABASE_URL` | Postgres connection string | (required) |
| `BANK_URL` | Mock bank base URL | `http://localhost:8787` |
| `LISTEN_ADDR` | HTTP listen address | `0.0.0.0:8080` |

`.env` file in project root is loaded automatically via `dotenv`.

## 10. What Is NOT Implemented

- Multiple currency support (hardcoded USD)
- Card number Luhn validation (delegated to bank)
- Full card number storage (only `last_four` stored)
- Partial captures (capture amount must match authorization)
- Partial refunds (refund amount must match capture)
- Webhooks to merchant systems
- Authentication/authorization on gateway endpoints
- Rate limiting
- Multi-tenant isolation (single-tenant by design)

## 11. Related Documents

- `ARCHITECTURE.md` — system design, data flow, module boundaries
- `TRADEOFFS.md` — engineering tradeoff analysis
- `src/domain/state.rs` — pure state machine with unit tests
- `src/services/idempotency.rs` — idempotency reserve/replay/complete logic
- `src/services/recovery.rs` — background recovery worker
