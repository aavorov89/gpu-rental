# GPU rental payments

I use Postgres for two jobs here. A customer reserves part of a prepaid balance
for a rental. A host gets paid through a payment provider. For this exercise
the provider keeps transfers in memory. All queries in the Go code use pgx and
raw SQL.

## Run the tests

You need Go 1.27.1 or newer and Docker with Compose. From the project directory run

```bash
docker compose up -d --wait
go test -race ./...
```

The ledger and payout tests each create a database named `gpu_rental_test_*`.
They apply the SQL migrations there and drop the database when the tests finish.

If you already have Postgres running, set `TEST_DATABASE_URL`. The database
user needs permission to create databases.

```bash
TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable' go test -race ./...
```

To apply the same migrations to the local development database, run

```bash
GOOSE_DRIVER=postgres \
GOOSE_DBSTRING='postgres://gpu_rental:gpu_rental@127.0.0.1:5432/gpu_rental?sslmode=disable' \
GOOSE_MIGRATION_DIR=./migrations \
go tool -modfile=tools.mod goose up
```

The SQL files are in `migrations/`. I keep the Goose command in `tools.mod`
with its own `tools.sum` because the command also needs drivers for other databases.
The test database setup is in `internal/testdb`.

## Customer balance

The code is in `internal/ledger`. All amounts are integers in minor units of
one currency. Postgres stores them as `BIGINT`. A customer can spend
`users.balance - users.held`. The database checks that both numbers are at
least zero and that `held <= balance`.

`Hold` and `Settle` each use one `READ COMMITTED` transaction. Both lock the
same customer row before changing money. Two requests for one customer take
turns even if they are for different rentals.

**Hold.** First lock the customer row with `FOR UPDATE`, then lock the
reservation and check its owner. Look for an existing hold before checking
available money. This order matters if the first call reserved the last cent.
A retry with the same amount gets the original hold. A different amount
gives a conflict. For a new hold, check the available balance, insert the
hold and increase `users.held`. If the money is short, the transaction rolls
back and the reservation has no hold row.

**Settle.** Read the customer ID and the amount from the reservation and its
hold. The service never edits a hold after creating it. Lock the customer row,
then read the settlement. With `READ COMMITTED`, this read sees a settlement
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
cannot settle more than was held or change the amount of a settled hold.

Each reservation can have one hold and one settlement. The settlement row tells
us the hold is closed. The tests also check that `users.held` equals the sum
of all open holds for the customer.

## Host payouts

The service and worker are in `internal/payouts`. The caller supplies the
provider. The mock in `internal/payouts/payoutstest` can pay, reject a transfer
or return a timeout. It can lose the request before moving money or lose the
reply after moving money. The caller sees the same error in both timeout cases.

`RequestPayout` first commits a `pending` row. It stores the key
`payout:<payout_id>` there and uses it for the first `CreateTransfer` call.
If the process stops just after commit, the worker still has the row and the
key. A second request with the same ID, host and amount returns the saved
payout. Changing the host or amount for that ID gives a conflict.

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
4. Change `pending` to `paid` or `failed` only when the lookup returns that
   status. These two states are final. For an error, unknown status or a
   transfer still in progress, keep `pending` and try again in 5 seconds.

If the worker stops before commit, Postgres releases its lock. The next worker
starts with a lookup, even if the previous worker already sent the transfer.

## What would break in production

- **Provider keys.** A provider may remember a key for a limited time. An old
  payout could get paid twice if the key expires and a lookup says the transfer
  is missing. A lookup can also miss a recent payment if the provider is slow
  to update its records. I would check the provider's time limit and send old
  payouts for manual review.
- **Retries and timeouts.** The delays are fixed in code. During an outage the
  worker keeps trying every 5 seconds, with no backoff, jitter, attempt limit
  or circuit breaker. Up to three provider calls share one 3-second timeout.
  If the provider rejects a transfer but never shows it in lookup, that payout
  can stay pending forever. Someone needs a way to review it.
- **Connections and locks.** A slow provider keeps a Postgres connection and
  transaction busy. Enough slow calls can fill the pool and delay vacuum. A
  worker could claim the row for a limited time and release the connection
  before calling the provider. Today each worker handles one row per tick.
  Operations on one busy customer also wait for the same row lock.
- **Reconciliation.** I would compare payouts with provider statements and
  check open holds against `users.held` regularly. Today only the tests check
  the holds. There are no webhooks or later checks for reversals of paid
  transfers. With asynchronous replication, a Postgres failover can also lose
  a payout row after the provider has moved the money.
- **Observability.** The worker logs errors. For a live service I would record
  queue size, age of the oldest pending payout and provider error rate, and
  alert on stuck payouts. Status history, structured logs and traces would
  help explain what happened to one payment.
- **Accounting and access.** Settlement charges the customer. Paying a host
  still needs a record of what the host earned and how much they can withdraw.
  Other missing parts are top-ups, refunds, a double-entry journal, other
  currencies, authentication, access checks, rate limits and an audit trail.
  An abandoned hold stays open until someone settles it. Settling for zero
  releases the money and records a settlement. There is no separate cancel.
- **Failure tests.** The tests use real Postgres and the in-memory provider.
  A provider sandbox test and a test that kills the process or the database
  connection during commit would cover failures the mock cannot show.
