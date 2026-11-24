# GPU rental payments

I use Postgres for two jobs here. A customer reserves part of a prepaid balance
for a rental. A host gets paid through a payment provider. For this exercise
the provider keeps transfers in memory. All queries in the Go code use pgx and
raw SQL.

## Run the tests

You need Go 1.26 or newer and Docker with Compose. From the project directory run

```bash
docker compose up -d --wait
go test -race -count=1 ./...
```

Compose publishes Postgres on `127.0.0.1:55432`, so it does not clash with a
local Postgres on the default port. `-count=1` turns off the Go test cache,
which could otherwise report a pass without touching the database.

The ledger and payout tests each create a database named `gpu_rental_test_*`.
They apply the SQL migrations there and drop the database when the tests finish.

If you already have Postgres running, set `TEST_DATABASE_URL`. The database
user needs permission to create databases.

```bash
TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable' go test -race -count=1 ./...
```

The test pool uses 20 connections unless the URL sets `pool_max_conns`. Use at
least 2, because the two-worker test keeps two transactions open at once.

To apply the same migrations to the local development database, run

```bash
GOOSE_DRIVER=postgres \
GOOSE_DBSTRING='postgres://gpu_rental:gpu_rental@127.0.0.1:55432/gpu_rental?sslmode=disable' \
GOOSE_MIGRATION_DIR=./migrations \
go tool -modfile=tools.mod goose up
```

The SQL files are in `migrations/`, and `embed.go` in the project root embeds
them for the tests. I keep the Goose command in `tools.mod` with its own
`tools.sum` because the command also needs drivers for other databases.
The test database setup is in `internal/testdb`.

### What the tests cover

| Requirement | Tests |
| --- | --- |
| Hold and settle release the leftover | `TestHoldThenSettleReleasesLeftover` |
| A short balance leaves no partial state | `TestHoldRejectsShortBalanceWithoutPartialState` |
| Concurrent holds cannot overdraw | `TestConcurrentHoldsCannotOverdraw` |
| Hold is idempotent by reservation | `TestHoldIsIdempotent`, `TestConcurrentHoldsOnSameReservationHoldOnce` |
| A held reservation cannot change owner | `TestHoldPinsReservationOwner` |
| A hold cannot be edited or deleted | `TestDatabaseRejectsHoldChanges` |
| A settlement cannot be edited or deleted | `TestDatabaseRejectsSettlementChanges` |
| Settle is idempotent, another amount is an error | `TestSettleIsIdempotent`, `TestSettleRejectsConflictingAmount` |
| Settled never exceeds held | `TestSettleRejectsMoreThanHeld`, `TestDatabaseEnforcesSettledWithinHold` |
| Concurrent payout retries all get the same payout | `TestConcurrentRequestPayoutIsIdempotent` |
| An unclear provider answer leaves the payout pending | `TestAmbiguousResponseLeavesPayoutPending`, `TestWorkerKeepsPendingWhileTransferInProgress`, `TestWorkerKeepsPendingOnUnknownProviderStatus` |
| The worker resolves by lookup with the same key | `TestWorkerResolvesByLookupWithoutNewTransfer`, `TestWorkerRetriesLostTransferWithSameKey` |
| The worker refuses a transfer for another host or amount | `TestWorkerRefusesTransferWithOtherParameters`, `TestRequestPayoutKeyConflictIsNotConfirmedByLookup` |
| The database enforces the payout state machine | `TestDatabaseEnforcesPayoutStateMachine` |
| The database rejects invalid amounts, balances and statuses | `TestDatabaseRejectsInvalidAmounts`, `TestDatabaseRejectsInvalidPayoutValues` |
| Customer locks do not block foreign key checks | `TestHoldAndSettleDoNotBlockForeignKeyChecks` |
| Two workers cannot process one payout | `TestSecondWorkerSkipsLockedPayout`, `TestManyWorkersNeverDoublePay` |
| The worker keeps running after an error | `TestRunWorkerContinuesAfterError` |

## Design notes

### Customer balance

The code is in `internal/ledger`. All amounts are integers in minor units of
one currency. Postgres stores them as `BIGINT`. A customer can spend
`users.balance - users.held`. The database checks that both numbers are at
least zero and that `held <= balance`.

`Hold` and `Settle` each use one `READ COMMITTED` transaction. Both lock the
same customer row with `FOR NO KEY UPDATE` before changing money. Two requests
for one customer take turns even if they are for different rentals, while
foreign key checks from unrelated inserts do not wait.

**Hold.** First lock the customer row, then lock the reservation and check its
owner. Look for an existing hold before checking available money. This order matters if the first call reserved the last cent.
A retry with the same amount gets the original hold. A different amount
gives a conflict. For a new hold, check the available balance, insert the
hold and increase `users.held`. If the money is short, the transaction rolls
back and the reservation has no hold row.

**Settle.** Read the customer ID and the amount from the hold. A trigger
rejects every `UPDATE` and `DELETE` on `holds`, and a composite foreign key
stops the reservation from changing owner while a hold exists, so the customer
whose `held` grew is the one charged. Lock the customer row, then read the
settlement. With `READ COMMITTED`, this read sees a settlement
that another request committed while we waited for the lock. A retry with the
same usage gets that settlement back. Different usage gives a conflict.

For a new settlement, usage must be between zero and the hold amount,
inclusive. Insert the settlement, subtract actual usage from `balance` and
subtract the whole hold from `held`. For example, with 1000 cents on the
account, a hold of 300 leaves 700 available. If the rental costs 120, the
final balance is 880 and all 880 are available.

The database enforces the same limit. A settlement keeps a copy of the hold
amount in `held_amount`. A composite foreign key ties that copy to the hold,
and a `CHECK` keeps `used_amount <= held_amount`. Even a bug in the Go code
cannot settle more than was held.

Each reservation can have one hold and one settlement. The settlement row tells
us the hold is closed and answers retries, so a trigger rejects every `UPDATE`
and `DELETE` on `settlements` too. Without it, deleting the row would let a
retry charge the customer again. The tests also check that `users.held` equals
the sum of all open holds for the customer.

### Host payouts

The service and worker are in `internal/payouts`. The caller supplies the
provider. The mock in `internal/payouts/payoutstest` can pay, reject a transfer,
keep it in progress or return a timeout. It can lose the request before moving
money or lose the reply after moving money. The caller sees the same error in
both timeout cases.

`RequestPayout` accepts an ID of up to 128 ASCII letters, digits, `_` or `-`,
which keeps it safe in logs and short enough for a provider key. It first
commits a `pending` row. It stores the key `payout:<payout_id>` there and uses
it for the first `CreateTransfer` call. If the process stops just after commit,
the worker still has the row and the key. A second request with the same ID,
host and amount returns the saved payout. Changing the host or amount for that
ID gives a conflict. A `nil` error does not mean the money moved. Only the
`paid` status says that.

The first worker check is due 30 seconds from the start of the insert
transaction. The first provider call has a 3-second timeout. Usually it
finishes before the worker starts. If the insert waits on a lock, the worker
may find the row due as soon as it commits. Both calls use the same key, so
the provider can recognise a transfer it already made.

`ProcessOne` handles one due payout per transaction.

1. Pick a `pending` row with `FOR UPDATE SKIP LOCKED`. Keep the row locked
   during the provider calls. A second worker can pick another row.
2. Ask the provider for the transfer status using the saved key. If the
   lookup times out, keep the payout pending.
3. If the provider reports that the transfer is missing, call `CreateTransfer`
   with the same key, host and amount. After success or a definite rejection,
   ask for the status again.
4. Compare the host and amount from the lookup with the payout. A difference
   means the key belongs to another transfer, so its status says nothing about
   this payout and the worker treats it as an error.
5. Change `pending` to `paid` or `failed` only when the lookup returns that
   status. These two states are final. For an error, unknown status or a
   transfer still in progress, keep `pending` and try again in 5 seconds.

If the worker stops before commit, Postgres releases its lock. The next worker
starts with a lookup, even if the previous worker already sent the transfer.

A trigger enforces the state machine in the database. A new row must be
`pending`. Only a `pending` row can change, and only its status and retry time,
because a changed key would make the worker send a second real transfer.
`DELETE` is rejected too.

## What would break in production

- **Provider keys.** If the provider forgets an old key, a lookup says the
  transfer is missing and the worker can pay again. Older payouts need manual
  review. Keys have no environment prefix, so each environment needs its own
  provider account.
- **Retries and timeouts.** Delays are fixed, with no backoff, jitter, attempt
  limit or circuit breaker. Up to three provider calls share one 3-second
  timeout. A key mismatch, or a rejection that lookup never shows, keeps a
  payout `pending` forever, with no review queue. A provider client that panics
  or ignores its context can block the queue.
- **Connections and locks.** A claim holds a Postgres transaction during
  provider calls, so a slow provider can fill the pool and delay vacuum. A long
  lock on one customer can do the same. If a worker loses its connection during
  a call, another worker can take the same payout, and only the idempotency key
  prevents a second transfer. Each worker handles one row per tick.
- **Reconciliation.** At runtime nothing compares payouts with provider
  statements or open holds with `users.held`. Later reversals go unnoticed. A
  payout stores no provider transfer ID or confirmation time. An asynchronous
  failover can lose a payout after the money moved.
- **Observability.** Errors go to a plain-text log. There are no metrics,
  alerts, status history or traces.
- **Accounting and access.** `RequestPayout` trusts the caller's host and amount
  because host earnings are not modeled. There is no authentication, rate limit,
  top-up, refund, double-entry journal or second currency. An abandoned hold
  stays open until someone settles it for zero.
- **Database access.** Triggers stop accidental `UPDATE` and `DELETE`, but a
  table owner or superuser can `TRUNCATE` or disable them. Production needs
  separate app and migration roles. Even then the app can write to `users`, so
  only a journal and reconciliation would catch a bad balance. Compose is for
  local tests only, with a superuser, a default password, no TLS and an unpinned
  image.
- **Failure tests.** The provider is a mock, and no test kills the process or
  the database connection during commit.
