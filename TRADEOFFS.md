# Tradeoffs

## Overview

This payment gateway sits between an order service and a mock bank API on port 8787. It authorizes, captures, voids, and refunds card payments, storing every receipt in Postgres as the source of truth. The bank randomly fails and lags, so the design favors recoverability and consistency over raw latency. Full card numbers and CVVs are never stored; only a card token carrying last-four and expiry is retained.

## State machine vs. bank-driven state

The gateway enforces its own state machine (`PENDING → AUTHORIZED → CAPTURED → REFUNDED`, plus `AUTHORIZED → VOIDED`) in pure Rust (`domain/state.rs`), independent of what the bank returns. This is the central tradeoff: we reject any transition the gateway deems illegal even if the bank would allow it. The bank is treated as an untrusted external system that may retry, duplicate, or partially succeed requests. By making the gateway's state machine the source of truth, a buggy or compromised bank cannot corrupt our ledger. The cost is that legitimate bank behavior we haven't anticipated (for example, a bank allowing recapture of a voided payment) is rejected by us, which can cause mismatches requiring manual reconciliation.

## Idempotency keys

Every mutating API call accepts an optional idempotency key. On receipt, the gateway checks whether the key has already been used; if so, it replays the stored response without touching the bank. This makes duplicate retries safe after network timeouts. The tradeoff is storage and lookup overhead for every request, plus the risk of key collision if callers reuse keys for genuinely different requests. We mitigate the latter by hashing the request body and rejecting a key reuse with a different hash (`IdempotencyConflict`). Keys are persisted in Postgres, so idempotency survives restarts, at the cost of an extra row write per mutation.

## In-flight records and recovery

Before each bank call, the gateway writes an "in-flight" marker (payment id, attempt count, started-at) to Postgres. If the process crashes or a bank call times out, the marker remains. A background recovery worker (`services/recovery.rs`) polls for markers older than a threshold and attempts to resolve each stuck payment by re-reading the current state and re-issuing the bank call idempotently. Tradeoff: recovery is eventually consistent, not immediate. A payment can sit in an intermediate state (for example, authorized at the bank but still `PENDING` in our DB) until the next recovery sweep. Sweep interval trades latency of recovery against DB load. We chose a short default interval because payment correctness matters more than polling cost.

## Retry vs. stop

The bank client retries transient failures (HTTP 500, timeouts, connection errors) with exponential backoff, but stops immediately on permanent failures (declined card, insufficient funds, 4xx). This is the right split for a payment system: retrying a declined card is pointless and can annoy customers or trigger bank fraud rules; retrying a transient blip is usually safe because the gateway's idempotency keys and in-flight markers prevent double-charging. The residual risk is a "timeout after success" case: the bank charged the card but our request timed out, so we retry and potentially double-charge. Idempotency keys on the bank side (if the bank supports them) plus our own idempotency layer mitigate this, but not fully. We accept this small risk because the alternative (never retrying) leaves far more payments stuck.

## Data model and storage

Postgres is the source of truth, with `payments`, `state_history`, `idempotency_keys`, and `inflight` tables. `state_history` appends one row per transition, giving an immutable audit trail. Tradeoff: history grows unbounded and updates require a transaction touching both `payments` and `state_history`. We accept the write cost because auditability and the ability to reconstruct state are non-negotiable for payments. We use SQLx with the `migrate` feature to run the schema on startup; migrations are versioned SQL files.

The `payments.state` column is a Postgres enum (`payment_state`), mirroring the Rust enum. Tradeoff: enums are strict and altering them requires a migration, but they prevent invalid states at the DB level, a valuable defense-in-depth layer.

## What we intentionally do not store

We store only a card token: last-four digits, expiry month/year. No PAN, no CVV. This reduces PCI scope and limits blast radius if the DB is leaked. The tradeoff is we cannot re-present the full card to the bank on recovery; we rely on the bank reference ID returned at authorization for all subsequent capture/void/refund calls. If the bank loses its own record of a reference, we cannot reconstruct the payment from our stored data alone — another argument for treating the bank as authoritative only within our state machine's bounds.

## Out-of-scope decisions

We explicitly do not support partial captures or partial refunds, multiple currencies (USD only), or building the bank mock. These simplify the state machine and the retry/recovery logic substantially. Adding them later would require revising `state.rs` (new states like `PARTIALLY_CAPTURED`), idempotency key granularity (per partial operation), and the recovery worker's reconciliation logic.

## Reconciliation

The gateway treats its own `payments` table as the source of truth and does not perform automated reconciliation against the bank's records. There is no double-entry ledger, no nightly match of gateway state to bank authorizations/captures, and no automated alert when our record diverges from the bank's (for example, a payment marked `AUTHORIZED` here that the bank never recorded, or vice versa). Drift between systems would require manual detection and correction. We accept this gap because building reconciliation against an external, sometimes-unreliable bank API is substantial engineering work; for now, correctness relies on the idempotency layer, recovery worker, and optimistic state machine rather than external verification. This should be added before production use.

## Operational notes

Because Postgres is required at runtime but unavailable in this environment, the code is structured to compile and the pure state machine is unit-testable without a DB. Integration tests against a real Postgres and the bank mock should be added before production use. The recovery worker runs as a spawned background task alongside the Axum server; in a multi-replica deployment, leader election or a distributed lock would be needed to avoid two workers resolving the same stuck payment simultaneously — currently out of scope.

