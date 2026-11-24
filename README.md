# GPU rental payments

I use Postgres for two parts of GPU rental. For the customer, I reserve money
from the prepaid balance. For the host, I send a payout through a payment
provider. In this exercise the provider keeps its transfers in memory. The Go
code uses pgx with raw SQL for all queries.

## Run the tests

You need Go 1.26 or newer and Docker with Compose. Run these commands from the
project directory.

```bash
docker compose up -d --wait
go test -race -count=1 ./...
```

Compose exposes Postgres on `127.0.0.1:55432`. This leaves the default port
free for a local Postgres instance. I use `-count=1` to disable the Go test
cache. Otherwise Go can report a previous success without connecting to
Postgres.

The ledger tests and the payout tests each create their own database with a
name like `gpu_rental_test_*`. They apply the SQL migrations to it and drop it
when the tests finish.

To use an existing Postgres instance, set `TEST_DATABASE_URL`. The user in
this URL must have permission to create databases.

```bash
TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable' go test -race -count=1 ./...
```

The default pool size for tests is 20 connections. You can set
`pool_max_conns` in the URL. Use at least 2 connections because the test with
two workers needs two open transactions at the same time.

For the local development database, apply the same migrations with this command.

```bash
GOOSE_DRIVER=postgres \
GOOSE_DBSTRING='postgres://gpu_rental:gpu_rental@127.0.0.1:55432/gpu_rental?sslmode=disable' \
GOOSE_MIGRATION_DIR=./migrations \
go tool -modfile=tools.mod goose up
```

I keep the SQL files in `migrations/`. For the tests, `embed.go` in the
project root embeds these files. The Goose command has a separate `tools.mod`
and `tools.sum` because it also needs drivers for other databases. The code
for test database setup is in `internal/testdb`.

### What the tests cover

| Requirement | Tests |
| --- | --- |
| Settlement releases the unused part of the hold | `TestHoldThenSettleReleasesLeftover` |
| A hold with insufficient funds leaves no partial changes | `TestHoldRejectsShortBalanceWithoutPartialState` |
| Concurrent holds stay within the available balance | `TestConcurrentHoldsCannotOverdraw` |
| Repeating a hold for one reservation gives the same hold | `TestHoldIsIdempotent`, `TestConcurrentHoldsOnSameReservationHoldOnce` |
| The reservation keeps the same owner once it has a hold | `TestHoldPinsReservationOwner` |
| The database rejects edits and deletions of holds | `TestDatabaseRejectsHoldChanges` |
| The database rejects edits and deletions of settlements | `TestDatabaseRejectsSettlementChanges` |
| A settlement retry needs the same amount and returns the same settlement | `TestSettleIsIdempotent`, `TestSettleRejectsConflictingAmount` |
| Usage must fit within the hold | `TestSettleRejectsMoreThanHeld`, `TestDatabaseEnforcesSettledWithinHold` |
| The first provider call starts after the `pending` payout is saved | `TestWorkerDoesNotClaimPayoutDuringFirstProviderCall` |
| Concurrent retries return the same payout | `TestConcurrentRequestPayoutIsIdempotent` |
| A payout stays pending until the provider gives a definite status | `TestAmbiguousResponseLeavesPayoutPending`, `TestWorkerKeepsPendingWhileTransferInProgress`, `TestWorkerKeepsPendingOnUnknownProviderStatus` |
| The worker checks transfer status by the original key and reuses it on retry | `TestWorkerResolvesByLookupWithoutNewTransfer`, `TestWorkerRetriesLostTransferWithSameKey` |
| The worker rejects a transfer if the host or amount differs | `TestWorkerRefusesTransferWithOtherParameters`, `TestRequestPayoutKeyConflictIsNotConfirmedByLookup` |
| The database allows only valid payout state changes | `TestDatabaseEnforcesPayoutStateMachine` |
| The database checks amounts, balances and statuses | `TestDatabaseRejectsInvalidAmounts`, `TestDatabaseRejectsInvalidPayoutValues` |
| Foreign key checks can run while the customer row is locked | `TestHoldAndSettleDoNotBlockForeignKeyChecks` |
| Two workers cannot process the same payout at the same time | `TestSecondWorkerSkipsLockedPayout`, `TestManyWorkersNeverDoublePay` |
| A worker continues after an error | `TestRunWorkerContinuesAfterError` |

## Design notes

### Customer balance

The code is in `internal/ledger`. I use one currency and count money in minor
units. All amounts are integers and Postgres stores them as `BIGINT`. The
available amount is `users.balance - users.held`. The database checks that
both values are zero or positive and that `held <= balance`.

Each call to `Hold` or `Settle` uses one `READ COMMITTED` transaction. Before
changing the amounts, both operations lock the customer row with
`FOR NO KEY UPDATE`. For the same customer the requests go one after the
other, even for different rentals. Unrelated inserts can still check their
foreign keys without waiting for this lock.

**Hold.** Lock the customer row first, then the reservation. Check who owns
the reservation. Look up the existing hold before checking available funds.
The first request may already have reserved the full balance. If the amount
is the same, the retry returns the original hold. A different amount gives a
conflict. For a new hold, check available funds, insert the hold and add its
amount to `users.held`. If funds are insufficient, the transaction rolls back.
There is no hold row for this reservation.

**Settle.** Take the customer ID and the amount from the hold. A trigger
rejects every `UPDATE` and `DELETE` on `holds`. A composite foreign key keeps
the reservation with the same owner while the hold exists. This way the
charge goes to the customer whose `held` increased. Lock the customer row
before reading the settlement. Under `READ COMMITTED` this read sees a
settlement that another request committed while this one waited for the lock.
A retry with the same usage returns the saved settlement. Different usage
gives a conflict.

For a new settlement, usage can be from zero up to the hold amount, both
included. Insert the settlement. Deduct actual usage from `balance` and the
full hold amount from `held`. For example, a customer has 1000 cents and
reserves 300. This leaves 700 available. If the rental costs 120, the final
balance is 880 cents, all available.

Postgres also checks the limit on usage. The settlement stores the hold
amount in `held_amount`. A composite foreign key makes sure this copy matches
the amount on the hold. A `CHECK` enforces `used_amount <= held_amount`. Even
if the Go code has a bug, the database rejects a settlement above the hold.

There can be at most one hold and one settlement per reservation. The
settlement row marks the hold as closed and gives the result for retries.
For this reason a trigger also rejects every `UPDATE` and `DELETE` on
`settlements`. If someone could delete the row, a retry could charge the
customer a second time. The tests also check that `users.held` equals the sum
of the customer's open holds.

### Host payouts

The service and its worker are in `internal/payouts`. The caller provides the
payment provider. The mock in `internal/payouts/payoutstest` can pay the
host, reject a transfer, leave it in progress or return a timeout. For a
timeout it can lose the request before payment or lose the reply after
payment. Both cases give the caller the same error.

For the ID, `RequestPayout` accepts up to 128 ASCII letters, digits, `_` or
`-`. This makes the ID safe to write in logs and short enough for the
provider key. The service first commits a `pending` row with the key
`payout:<payout_id>`. The first `CreateTransfer` call uses this key. If the
process stops just after commit, the worker has both the row and the key. A
retry with the same ID, host and amount returns the saved payout. Reusing
the ID with another host or amount gives a conflict. A call can return a
`nil` error while payment is still pending. Only `paid` confirms payment.

The first worker check is due 30 seconds after the insert transaction
starts. The first provider call has a timeout of 3 seconds and usually
finishes before the worker gets the row. If the insert waits for a lock, the
row can already be due at commit. Both calls use the same key, so the
provider can recognise a transfer it already made.

`ProcessOne` handles one due payout in one transaction.

1. Select a `pending` row with `FOR UPDATE SKIP LOCKED`. Hold the row lock
   during the provider calls. Another worker can take a different row.
2. Look up the transfer at the provider with the saved key. If lookup times
   out, leave the payout pending.
3. If the provider has no transfer for this key, call `CreateTransfer` with
   the same key, host and amount. On success or definite rejection, look up
   the transfer again.
4. Check the host and amount from lookup against the payout. If either is
   different, the key belongs to another transfer. Its status cannot confirm
   this payout. The worker treats the mismatch as an error.
5. Move from `pending` to `paid` or `failed` only when lookup confirms that
   status. Both statuses are final. On error, unknown status or transfer
   still in progress, keep `pending` and retry in 5 seconds.

If the worker stops before commit, Postgres releases the lock. The next
worker starts with a lookup, even if the previous one already sent the
transfer.

A database trigger checks the state changes. Every new payout must start as
`pending`. Only a pending row can change, and only its status and retry time
can change. With a different key the worker could send a second real
transfer. The trigger also rejects `DELETE`.

## What would break in production

- **Provider keys.** Once the provider forgets a key, lookup reports a
  missing transfer. The worker could pay it again. Older payouts need manual
  review. Each environment needs its own provider account because the keys
  have no environment prefix.
- **Retries and timeouts.** Retry delays are fixed. There is no backoff,
  jitter, attempt limit or circuit breaker. Up to three provider calls use
  the same 3-second timeout. A key mismatch leaves the payout `pending`
  forever. The same happens if lookup never confirms a rejection. There is
  no queue for manual review. A provider client can block the queue if it
  panics or ignores its context.
- **Connections and locks.** The worker keeps a Postgres transaction open
  during provider calls. A slow provider can exhaust the pool and delay
  vacuum. A long lock on a customer row can do the same. If the worker
  loses its connection during a call, another worker can take this payout.
  Then only the idempotency key prevents a second transfer. Each worker
  handles one row per tick.
- **Reconciliation.** At runtime there is no comparison of payouts with
  provider statements, or of open holds with `users.held`. Later reversals
  go undetected. The payout has no provider transfer ID or confirmation
  time. With asynchronous failover the database can lose the payout after
  the money has moved.
- **Observability.** The worker logs errors as plain text. There are no
  metrics or alerts, and no history of status changes or traces.
- **Accounting and access.** There is no model for host earnings, so
  `RequestPayout` trusts the host and amount from the caller. There is no
  authentication, rate limiting, top-up, refund, double-entry journal or
  second currency. An abandoned hold stays open until someone settles it
  with zero usage.
- **Database access.** Triggers protect against accidental `UPDATE` and
  `DELETE`. A table owner or superuser can still use `TRUNCATE` or disable
  the triggers. Production needs separate roles for the app and migrations.
  The app can still write to `users`, so only a journal and reconciliation
  would catch an incorrect balance. Compose is for local tests only. It
  uses a superuser, a default password and an unpinned image. TLS is off.
- **Failure tests.** I use a mock provider. None of the tests kill the
  process or the database connection during commit.
