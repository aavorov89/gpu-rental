# GPU rental money paths

A small Go module that implements two money paths against Postgres, using
native pgx (`pgxpool`) and raw SQL:

- **Prepaid hold and settle** for customers (`internal/ledger`).
- **Ambiguous external payouts** for hosts (`internal/payouts`).

This is a made-up system. The payment provider is an in-memory mock.

## Running the tests

You need Go 1.27+ and Docker.

```bash
docker compose up -d --wait
go test -race ./...
```

Each test package creates its own randomly named database
(`gpu_rental_test_*`), applies every migration, and drops it afterwards. The
development tables are not touched.

To use another Postgres server, point `TEST_DATABASE_URL` at it. Its role must
be allowed to create databases:

```bash
TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable' go test -race ./...
```

To apply the migrations to the development database:

```bash
GOOSE_DRIVER=postgres \
GOOSE_DBSTRING='postgres://gpu_rental:gpu_rental@127.0.0.1:5432/gpu_rental?sslmode=disable' \
GOOSE_MIGRATION_DIR=./migrations \
go tool -modfile=tools.mod goose up
```

The goose CLI is pinned in `tools.mod`, because it pulls in drivers for every
database it supports. This keeps them out of the main `go.mod`.

## Layout

| Path | Purpose |
| --- | --- |
| `migrations/` | Goose SQL migrations |
| `internal/ledger` | `Hold` and `Settle` |
| `internal/payouts` | `RequestPayout`, `ProcessOne`, `RunWorker` |
| `internal/payouts/payoutstest` | Mock provider: success, hard failure, two kinds of timeout |
| `internal/testdb` | Temporary migrated database for tests |
| `tools.mod`, `tools.sum` | Separate module file for the goose CLI |

## Design notes

### Money

All amounts are `BIGINT` minor units. There is one implicit currency.

Available balance is `users.balance - users.held`. The database enforces
`balance >= 0`, `held >= 0` and `held <= balance`, so a code bug cannot commit
an overdraft.

### Hold and settle transactions

Both operations run in one `READ COMMITTED` transaction.

`Hold`:

1. Lock the customer row with `FOR UPDATE`.
2. Lock the reservation and check that the customer owns it.
3. If a hold exists, return it when the amount matches, otherwise return a conflict.
4. Reject the request if `balance - held < amount`.
5. Insert the hold and increase `users.held`.

`Settle`:

1. Read the customer and held amount without a lock. A hold never changes after insert.
2. Lock the customer row with `FOR UPDATE`.
3. Read the settlement. This must come after the lock: in `READ COMMITTED` each
   statement gets a new snapshot, so it sees a settlement committed by a
   concurrent call that held the lock first.
4. If a settlement exists, return it when the amount matches, otherwise return a conflict.
5. Reject `used_amount > held`.
6. Insert the settlement, then run
   `balance = balance - used, held = held - hold_amount`.

Step 6 releases the leftover: `held` drops by the full hold, while `balance`
drops only by actual usage.

The customer row is the single serialization point for that customer's money,
and both operations take it first, so they share a lock order.

`holds` and `settlements` are insert-only. A hold is settled exactly when its
settlement row exists, so there is no status column to drift. `users.held` is a
deliberate denormalization of the sum of unsettled holds, and the tests assert
that the two agree.

### Payout state machine

```text
             provider says paid
  pending ───────────────────────▶ paid
     │ ▲
     │ └── ambiguous, not found, or still processing: retry later
     │
     └───────────────────────────▶ failed
             provider definitively rejected
```

`paid` and `failed` are terminal. Only the worker moves a payout out of
`pending`, and only after a provider status lookup or a definitive rejection.
A timeout never changes the status.

### Payout flow

`RequestPayout` commits the `pending` row **before** the first provider call,
so money can never move without a local record. If the process dies between the
commit and the call, the worker picks the row up.

The first worker attempt is delayed by 30 seconds, longer than the 3-second
provider timeout, so the worker does not race the initial call.

`ProcessOne`:

1. Claim one due payout with `FOR UPDATE SKIP LOCKED`.
2. Look up the transfer by its idempotency key.
3. If the provider has no such transfer, call `CreateTransfer` with the **same
   key**, then look it up again.
4. Store `paid` or `failed`, or keep `pending` and schedule another attempt.

### Idempotency

- A hold is unique per `reservation_id`, and a settlement is unique per `reservation_id`.
- `payout_id` is the primary key, and a repeat with other parameters is a conflict.
- The provider key is always `payout:<payout_id>`. It is derived once, stored
  with a unique constraint, and read back on every retry.

### Concurrency

The worker keeps its row lock during provider calls. A second worker skips the
locked row, so two workers cannot process one payout at once. If a worker
crashes, Postgres releases the lock and another worker resumes from the lookup,
which is safe because the key does not change.

## What would break in production

**Double-payment risks**

- Safety depends on the provider honoring idempotency keys. If keys expire, a
  payout stuck past that window could get `not found` and be paid again. There
  is no maximum payout age that would stop automatic retries and send it to a human.
- `not found` is trusted. A provider with eventually consistent reads could
  report it for a transfer that exists, leaving the key as the only protection.
- With asynchronous Postgres replication, a failover can lose a committed payout
  row after the provider was called.

**Reconciliation**

- There is no job comparing local payouts with provider statements.
- Terminal states are never revisited, so a provider reversal or late failure
  after `paid` goes unnoticed. There are no webhooks.
- Nothing periodically verifies that `users.held` matches the unsettled holds
  in production. Only the tests check it.

**Retries, backoff and timeouts**

- Retry delay is a fixed 5 seconds, with no exponential backoff, jitter,
  attempt counter, dead-letter state or circuit breaker. A provider outage
  causes constant retries, and a payout can stay `pending` forever.
- Timeouts and delays are hard-coded instead of configured.
- Every attempt uses one shared 3-second deadline for up to three provider calls.

**Capacity**

- The worker holds a database transaction and connection during network calls.
  A slow provider can exhaust the pool and keep transactions open, which delays vacuum.
  A lease column (`claimed_until`) would avoid this at the cost of more states.
- `RunWorker` handles one payout per tick, so throughput is capped by the interval.
- All money operations for one customer serialize on one row, which limits very
  active accounts.

**Observability**

- There are no metrics, traces, structured logs or alerts. Queue depth, oldest
  pending age, provider error rate and stuck payouts are invisible.
- Status changes leave no history, so an incident cannot be reconstructed from the database.

**Business gaps**

- Payouts are not connected to the ledger: settlement does not credit host
  earnings, and `RequestPayout` does not check that a host has earned the amount.
- There is no double-entry journal, top-up flow, refund, or multi-currency support.
- Abandoned holds never expire, and there is no explicit cancel. `Settle(id, 0)`
  releases a hold but records it as a settlement.
- There is no authentication, authorization, rate limiting or audit trail.

**Testing**

- The mock is not a real provider, and nothing tests a provider sandbox.
- Tests do not inject process crashes or database failures during commit.
