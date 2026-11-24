package payouts_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-rental/internal/payouts"
	"gpu-rental/internal/payouts/payoutstest"
	"gpu-rental/internal/testdb"
)

var testDB *pgxpool.Pool

func TestMain(m *testing.M) {
	db, cleanup, err := testdb.Open(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	testDB = db

	code := m.Run()
	cleanup()
	os.Exit(code)
}

// setup clears the queue so the worker only sees payouts from this test.
func setup(t *testing.T) (*payouts.Service, *payoutstest.Provider, int64) {
	t.Helper()

	if _, err := testDB.Exec(context.Background(), `TRUNCATE payouts`); err != nil {
		t.Fatalf("truncate payouts: %v", err)
	}

	hostID := seedHost(t)
	provider := payoutstest.New()

	return payouts.New(testDB, provider), provider, hostID
}

func seedHost(t *testing.T) int64 {
	t.Helper()

	var hostID int64
	if err := testDB.QueryRow(context.Background(), `INSERT INTO users (balance) VALUES (0) RETURNING id`).Scan(&hostID); err != nil {
		t.Fatalf("seed host: %v", err)
	}

	return hostID
}

func keyFor(id string) string {
	return "payout:" + id
}

// insertDuePayout leaves the row a worker would find if the app stopped
// before its first provider call. The row is ready to process now.
func insertDuePayout(t *testing.T, id string, hostID, amount int64) {
	t.Helper()

	_, err := testDB.Exec(context.Background(), `
		INSERT INTO payouts (id, host_id, amount, idempotency_key)
		VALUES ($1, $2, $3, $4)
	`, id, hostID, amount, keyFor(id))
	if err != nil {
		t.Fatalf("insert payout: %v", err)
	}
}

func makeDue(t *testing.T) {
	t.Helper()

	_, err := testDB.Exec(context.Background(), `
		UPDATE payouts
		SET next_attempt_at = NOW() - INTERVAL '1 second'
		WHERE status = 'pending'
	`)
	if err != nil {
		t.Fatalf("make payouts due: %v", err)
	}
}

func statusOf(t *testing.T, id string) payouts.Status {
	t.Helper()

	var status payouts.Status
	if err := testDB.QueryRow(context.Background(), `SELECT status FROM payouts WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}

	return status
}

func assertStatus(t *testing.T, id string, want payouts.Status) {
	t.Helper()

	if got := statusOf(t, id); got != want {
		t.Fatalf("payout %s status = %s, want %s", id, got, want)
	}
}

func processOne(t *testing.T, s *payouts.Service) {
	t.Helper()

	processed, err := s.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !processed {
		t.Fatal("process: no payout claimed")
	}
}

func assertAllKeys(t *testing.T, p *payoutstest.Provider, want string) {
	t.Helper()

	for _, call := range p.CreateCalls() {
		if call.Key != want {
			t.Fatalf("create used key %q, want %q", call.Key, want)
		}
	}
	for _, key := range p.LookupCalls() {
		if key != want {
			t.Fatalf("lookup used key %q, want %q", key, want)
		}
	}
}

func waitFor(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for provider call")
	}
}

func TestRequestPayoutPersistsAndUsesDerivedKey(t *testing.T) {
	s, p, host := setup(t)

	got, err := s.RequestPayout(context.Background(), "p1", host, 500)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	if got.ID != "p1" || got.HostID != host || got.Amount != 500 || got.IdempotencyKey != keyFor("p1") || got.Status != payouts.StatusPending {
		t.Fatalf("payout = %+v", got)
	}

	calls := p.CreateCalls()
	if len(calls) != 1 || calls[0] != (payoutstest.CreateCall{Key: keyFor("p1"), HostID: host, Amount: 500}) {
		t.Fatalf("create calls = %+v", calls)
	}

	// The provider accepted the call. The worker still has to check the status.
	assertStatus(t, "p1", payouts.StatusPending)

	makeDue(t)
	processOne(t, s)
	assertStatus(t, "p1", payouts.StatusPaid)
	if n := p.Applied(keyFor("p1")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
}

func TestRequestPayoutRejectsInvalidInput(t *testing.T) {
	s, p, host := setup(t)

	tests := []struct {
		id     string
		host   int64
		amount int64
	}{
		{id: "", host: host, amount: 1},
		{id: strings.Repeat("a", 129), host: host, amount: 1},
		{id: "p\nforged log line", host: host, amount: 1},
		{id: "p\n", host: host, amount: 1},
		{id: "p 1", host: host, amount: 1},
		{id: "p", host: 0, amount: 1},
		{id: "p", host: host, amount: 0},
		{id: "p", host: host, amount: -1},
	}

	for _, tt := range tests {
		_, err := s.RequestPayout(context.Background(), tt.id, tt.host, tt.amount)
		if !errors.Is(err, payouts.ErrInvalidRequest) {
			t.Fatalf("%+v: err = %v", tt, err)
		}
	}

	if calls := p.CreateCalls(); len(calls) != 0 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestRequestPayoutAcceptsValidIDs(t *testing.T) {
	s, _, host := setup(t)

	for _, id := range []string{
		strings.Repeat("a", 128),
		"3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b",
		"payout_42",
	} {
		if _, err := s.RequestPayout(context.Background(), id, host, 100); err != nil {
			t.Fatalf("id %q: %v", id, err)
		}
	}
}

func TestRequestPayoutDoesNotCallProviderWhenInsertFails(t *testing.T) {
	s, p, _ := setup(t)

	_, err := s.RequestPayout(context.Background(), "p-no-host", 999_999_999, 100)
	if !errors.Is(err, payouts.ErrHostNotFound) {
		t.Fatalf("err = %v, want ErrHostNotFound", err)
	}

	if calls := p.CreateCalls(); len(calls) != 0 {
		t.Fatalf("provider called without a persisted payout: %+v", calls)
	}
}

func TestRequestPayoutIsIdempotent(t *testing.T) {
	s, p, host := setup(t)

	first, err := s.RequestPayout(context.Background(), "p-idem", host, 100)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	second, err := s.RequestPayout(context.Background(), "p-idem", host, 100)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}

	if first.ID != second.ID || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("retry returned another payout: %+v vs %+v", first, second)
	}
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestConcurrentRequestPayoutIsIdempotent(t *testing.T) {
	s, p, host := setup(t)

	const payoutCount, callers = 30, 16

	for i := range payoutCount {
		id := fmt.Sprintf("p-race-%d", i)
		start := make(chan struct{})
		errs := make([]error, callers)

		var wg sync.WaitGroup
		for c := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, errs[c] = s.RequestPayout(context.Background(), id, host, 100)
			}()
		}

		close(start)
		wg.Wait()

		for _, err := range errs {
			if err != nil {
				t.Fatalf("request %s: %v", id, err)
			}
		}
		if applied := p.Applied(keyFor(id)); applied != 1 {
			t.Fatalf("applied %s = %d, want 1", id, applied)
		}
	}

	if calls := p.CreateCalls(); len(calls) != payoutCount {
		t.Fatalf("create calls = %d, want %d", len(calls), payoutCount)
	}
}

func TestRequestPayoutRejectsConflictingParameters(t *testing.T) {
	s, p, host := setup(t)

	if _, err := s.RequestPayout(context.Background(), "p-conflict", host, 100); err != nil {
		t.Fatalf("request: %v", err)
	}

	_, err := s.RequestPayout(context.Background(), "p-conflict", host, 200)
	if !errors.Is(err, payouts.ErrPayoutConflict) {
		t.Fatalf("other amount: err = %v, want ErrPayoutConflict", err)
	}

	_, err = s.RequestPayout(context.Background(), "p-conflict", host+1, 100)
	if !errors.Is(err, payouts.ErrPayoutConflict) {
		t.Fatalf("other host: err = %v, want ErrPayoutConflict", err)
	}
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestRequestPayoutRejectsKeyOfAnotherPayout(t *testing.T) {
	s, p, host := setup(t)

	_, err := testDB.Exec(context.Background(), `
		INSERT INTO payouts (id, host_id, amount, idempotency_key)
		VALUES ('p-imported', $1, 100, $2)
	`, host, keyFor("p-new"))
	if err != nil {
		t.Fatalf("insert payout: %v", err)
	}

	_, err = s.RequestPayout(context.Background(), "p-new", host, 100)
	if !errors.Is(err, payouts.ErrPayoutConflict) {
		t.Fatalf("err = %v, want ErrPayoutConflict", err)
	}
	if calls := p.CreateCalls(); len(calls) != 0 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestAmbiguousResponseLeavesPayoutPending(t *testing.T) {
	tests := []struct {
		name    string
		outcome payoutstest.Outcome
		applied int
	}{
		{name: "transfer lost", outcome: payoutstest.TimeoutNotApplied, applied: 0},
		{name: "response lost", outcome: payoutstest.TimeoutApplied, applied: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, p, host := setup(t)
			p.QueueCreate(tt.outcome)

			got, err := s.RequestPayout(context.Background(), "p-amb", host, 100)
			if !errors.Is(err, payoutstest.ErrAmbiguous) {
				t.Fatalf("err = %v, want ErrAmbiguous", err)
			}
			if got.Status != payouts.StatusPending {
				t.Fatalf("returned status = %s", got.Status)
			}

			assertStatus(t, "p-amb", payouts.StatusPending)
			if n := p.Applied(keyFor("p-amb")); n != tt.applied {
				t.Fatalf("applied = %d, want %d", n, tt.applied)
			}
		})
	}
}

func TestWorkerResolvesByLookupWithoutNewTransfer(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutApplied)

	_, _ = s.RequestPayout(context.Background(), "p-lookup", host, 100)
	makeDue(t)
	processOne(t, s)

	assertStatus(t, "p-lookup", payouts.StatusPaid)
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("worker created another transfer: %+v", calls)
	}
	if lookups := p.LookupCalls(); len(lookups) != 1 {
		t.Fatalf("lookups = %v, want one", lookups)
	}
	if n := p.Applied(keyFor("p-lookup")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	assertAllKeys(t, p, keyFor("p-lookup"))
}

func TestWorkerRetriesLostTransferWithSameKey(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied)

	_, _ = s.RequestPayout(context.Background(), "p-lost", host, 100)
	makeDue(t)
	processOne(t, s)

	assertStatus(t, "p-lost", payouts.StatusPaid)
	if calls := p.CreateCalls(); len(calls) != 2 {
		t.Fatalf("create calls = %+v, want 2", calls)
	}
	if n := p.Applied(keyFor("p-lost")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	assertAllKeys(t, p, keyFor("p-lost"))
}

func TestWorkerRecoversAfterCrashBeforeFirstProviderCall(t *testing.T) {
	s, p, host := setup(t)
	insertDuePayout(t, "p-crash", host, 100)

	processOne(t, s)

	assertStatus(t, "p-crash", payouts.StatusPaid)
	if n := p.Applied(keyFor("p-crash")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
}

func TestWorkerKeepsPendingThroughRepeatedAmbiguity(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.TimeoutApplied)

	_, _ = s.RequestPayout(context.Background(), "p-retry", host, 100)
	makeDue(t)

	processed, err := s.ProcessOne(context.Background())
	if !processed || !errors.Is(err, payoutstest.ErrAmbiguous) {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}
	assertStatus(t, "p-retry", payouts.StatusPending)

	makeDue(t)
	processOne(t, s)

	assertStatus(t, "p-retry", payouts.StatusPaid)
	if n := p.Applied(keyFor("p-retry")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	assertAllKeys(t, p, keyFor("p-retry"))
}

func TestWorkerLookupTimeoutDoesNotCreateTransfer(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutApplied)

	_, _ = s.RequestPayout(context.Background(), "p-lookup-timeout", host, 100)
	makeDue(t)
	p.QueueLookupError(payoutstest.ErrAmbiguous)

	processed, err := s.ProcessOne(context.Background())
	if !processed || !errors.Is(err, payoutstest.ErrAmbiguous) {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}

	assertStatus(t, "p-lookup-timeout", payouts.StatusPending)
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("create calls = %+v, want 1", calls)
	}

	var rescheduled bool
	err = testDB.QueryRow(context.Background(), `
		SELECT next_attempt_at > NOW()
		FROM payouts
		WHERE id = 'p-lookup-timeout'
	`).Scan(&rescheduled)
	if err != nil {
		t.Fatalf("read schedule: %v", err)
	}
	if !rescheduled {
		t.Fatal("next_attempt_at was not moved forward")
	}
}

func TestWorkerMarksHardFailureFailed(t *testing.T) {
	t.Run("rejected on first request", func(t *testing.T) {
		s, p, host := setup(t)
		p.QueueCreate(payoutstest.HardFailure)

		_, err := s.RequestPayout(context.Background(), "p-rejected", host, 100)
		if !errors.Is(err, payouts.ErrTransferRejected) {
			t.Fatalf("err = %v, want ErrTransferRejected", err)
		}
		assertStatus(t, "p-rejected", payouts.StatusPending)

		makeDue(t)
		processOne(t, s)

		assertStatus(t, "p-rejected", payouts.StatusFailed)
		if n := p.Applied(keyFor("p-rejected")); n != 0 {
			t.Fatalf("applied = %d, want 0", n)
		}
	})

	t.Run("rejected on worker retry", func(t *testing.T) {
		s, p, host := setup(t)
		p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.HardFailure)

		_, _ = s.RequestPayout(context.Background(), "p-rejected-retry", host, 100)
		makeDue(t)
		processOne(t, s)

		assertStatus(t, "p-rejected-retry", payouts.StatusFailed)
		if n := p.Applied(keyFor("p-rejected-retry")); n != 0 {
			t.Fatalf("applied = %d, want 0", n)
		}
	})
}

func TestRejectedRetryIsConfirmedByLookup(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.HardFailure)

	_, _ = s.RequestPayout(context.Background(), "p-confirm", host, 100)
	makeDue(t)
	processOne(t, s)

	assertStatus(t, "p-confirm", payouts.StatusFailed)
	if n := len(p.LookupCalls()); n != 2 {
		t.Fatalf("lookups = %d, want 2", n)
	}
	assertAllKeys(t, p, keyFor("p-confirm"))
}

func TestRejectedRetryWithAmbiguousLookupStaysPending(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.HardFailure)
	p.QueueLookupError(nil, payoutstest.ErrAmbiguous)

	_, _ = s.RequestPayout(context.Background(), "p-amb-reject", host, 100)
	makeDue(t)

	processed, err := s.ProcessOne(context.Background())
	if !processed || !errors.Is(err, payoutstest.ErrAmbiguous) {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}
	assertStatus(t, "p-amb-reject", payouts.StatusPending)

	makeDue(t)
	processOne(t, s)
	assertStatus(t, "p-amb-reject", payouts.StatusFailed)
}

func TestRejectionInvisibleToLookupStaysPending(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.HardFailure)
	p.QueueLookupError(nil, payouts.ErrTransferNotFound)

	_, _ = s.RequestPayout(context.Background(), "p-invisible", host, 100)
	makeDue(t)

	processed, err := s.ProcessOne(context.Background())
	if !processed || !errors.Is(err, payouts.ErrTransferNotFound) {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}
	assertStatus(t, "p-invisible", payouts.StatusPending)
}

func TestWorkerKeepsPendingWhileTransferInProgress(t *testing.T) {
	s, p, host := setup(t)
	p.QueueCreate(payoutstest.TimeoutNotApplied, payoutstest.InProgress)

	_, _ = s.RequestPayout(context.Background(), "p-progress", host, 100)
	makeDue(t)

	// The lookup misses, the retry is accepted and the next lookup says in progress.
	processOne(t, s)
	assertStatus(t, "p-progress", payouts.StatusPending)

	makeDue(t)
	processOne(t, s)
	assertStatus(t, "p-progress", payouts.StatusPending)
	if n := p.Applied(keyFor("p-progress")); n != 0 {
		t.Fatalf("applied = %d, want 0 while in progress", n)
	}

	p.SetStatus(keyFor("p-progress"), payouts.StatusPaid)
	makeDue(t)
	processOne(t, s)

	assertStatus(t, "p-progress", payouts.StatusPaid)
	if calls := p.CreateCalls(); len(calls) != 2 {
		t.Fatalf("create calls = %+v, want 2", calls)
	}
	if n := p.Applied(keyFor("p-progress")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	assertAllKeys(t, p, keyFor("p-progress"))
}

func TestWorkerKeepsPendingOnUnknownProviderStatus(t *testing.T) {
	s, p, host := setup(t)

	if _, err := s.RequestPayout(context.Background(), "p-unknown", host, 100); err != nil {
		t.Fatalf("request: %v", err)
	}
	p.SetStatus(keyFor("p-unknown"), payouts.Status("reversed"))
	makeDue(t)

	processed, err := s.ProcessOne(context.Background())
	if !processed || err == nil {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}
	assertStatus(t, "p-unknown", payouts.StatusPending)
}

func TestWorkerRefusesTransferWithOtherParameters(t *testing.T) {
	tests := []struct {
		name      string
		otherHost bool
		amount    int64
		outcome   payoutstest.Outcome
	}{
		{name: "paid to another host", otherHost: true, amount: 100, outcome: payoutstest.Success},
		{name: "paid another amount", amount: 250, outcome: payoutstest.Success},
		{name: "failed for another host", otherHost: true, amount: 100, outcome: payoutstest.HardFailure},
		{name: "in progress for another host", otherHost: true, amount: 100, outcome: payoutstest.InProgress},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, p, host := setup(t)

			transferHost := host
			if tt.otherHost {
				transferHost = seedHost(t)
			}

			// The key already belongs to another transfer. The app stopped before
			// its first call, so the worker never sees a key conflict from the provider.
			p.QueueCreate(tt.outcome)
			err := p.CreateTransfer(context.Background(), keyFor("p-foreign"), transferHost, tt.amount)
			if err != nil && !errors.Is(err, payouts.ErrTransferRejected) {
				t.Fatalf("seed transfer: %v", err)
			}
			insertDuePayout(t, "p-foreign", host, 100)

			processed, err := s.ProcessOne(context.Background())
			if !processed || !errors.Is(err, payouts.ErrTransferMismatch) {
				t.Fatalf("processed = %v, err = %v, want ErrTransferMismatch", processed, err)
			}

			assertStatus(t, "p-foreign", payouts.StatusPending)
			if calls := p.CreateCalls(); len(calls) != 1 {
				t.Fatalf("worker created another transfer: %+v", calls)
			}

			// The payout waits for its next attempt.
			processed, err = s.ProcessOne(context.Background())
			if processed || err != nil {
				t.Fatalf("second pass: processed = %v, err = %v", processed, err)
			}
		})
	}
}

func TestRequestPayoutKeyConflictIsNotConfirmedByLookup(t *testing.T) {
	s, p, host := setup(t)
	otherHost := seedHost(t)

	if err := p.CreateTransfer(context.Background(), keyFor("p-reused"), otherHost, 500); err != nil {
		t.Fatalf("seed transfer: %v", err)
	}

	_, err := s.RequestPayout(context.Background(), "p-reused", host, 100)
	if !errors.Is(err, payoutstest.ErrKeyReused) {
		t.Fatalf("err = %v, want ErrKeyReused", err)
	}
	assertStatus(t, "p-reused", payouts.StatusPending)

	makeDue(t)

	processed, err := s.ProcessOne(context.Background())
	if !processed || !errors.Is(err, payouts.ErrTransferMismatch) {
		t.Fatalf("processed = %v, err = %v, want ErrTransferMismatch", processed, err)
	}

	assertStatus(t, "p-reused", payouts.StatusPending)
	if n := p.Applied(keyFor("p-reused")); n != 1 {
		t.Fatalf("applied = %d, want only the original transfer", n)
	}
}

func TestDatabaseEnforcesPayoutStateMachine(t *testing.T) {
	s, p, host := setup(t)
	otherHost := seedHost(t)

	insertDuePayout(t, "p-paid", host, 100)
	processOne(t, s)

	p.QueueCreate(payoutstest.HardFailure)
	insertDuePayout(t, "p-failed", host, 100)
	processOne(t, s)

	insertDuePayout(t, "p-open", host, 100)

	tests := []struct {
		name  string
		query string
		args  []any
	}{
		{name: "insert as paid", query: `INSERT INTO payouts (id, host_id, amount, status, idempotency_key) VALUES ('p-direct', $1, 100, 'paid', 'payout:p-direct')`, args: []any{host}},
		{name: "pending key", query: `UPDATE payouts SET idempotency_key = 'payout:other' WHERE id = 'p-open'`},
		{name: "pending host", query: `UPDATE payouts SET host_id = $1 WHERE id = 'p-open'`, args: []any{otherHost}},
		{name: "pending amount", query: `UPDATE payouts SET amount = 250 WHERE id = 'p-open'`},
		{name: "delete pending", query: `DELETE FROM payouts WHERE id = 'p-open'`},
		{name: "reopen paid", query: `UPDATE payouts SET status = 'pending' WHERE id = 'p-paid'`},
		{name: "paid to failed", query: `UPDATE payouts SET status = 'failed' WHERE id = 'p-paid'`},
		{name: "reopen failed", query: `UPDATE payouts SET status = 'pending' WHERE id = 'p-failed'`},
		{name: "failed to paid", query: `UPDATE payouts SET status = 'paid' WHERE id = 'p-failed'`},
		{name: "reschedule paid", query: `UPDATE payouts SET next_attempt_at = NOW() WHERE id = 'p-paid'`},
		{name: "delete paid", query: `DELETE FROM payouts WHERE id = 'p-paid'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testDB.Exec(context.Background(), tt.query, tt.args...)

			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.ConstraintName != "payouts_state_machine" {
				t.Fatalf("err = %v, want violation of payouts_state_machine", err)
			}
		})
	}

	assertStatus(t, "p-paid", payouts.StatusPaid)
	assertStatus(t, "p-failed", payouts.StatusFailed)

	// A pending payout can still reach a final state.
	processOne(t, s)
	assertStatus(t, "p-open", payouts.StatusPaid)
}

func TestDatabaseRejectsInvalidPayoutValues(t *testing.T) {
	_, _, host := setup(t)
	insertDuePayout(t, "p-values", host, 100)

	tests := []struct {
		name       string
		query      string
		args       []any
		constraint string
	}{
		{
			name:       "zero amount",
			query:      `INSERT INTO payouts (id, host_id, amount, idempotency_key) VALUES ('p-zero', $1, 0, 'payout:p-zero')`,
			args:       []any{host},
			constraint: "payouts_amount_check",
		},
		{
			name:       "unknown status",
			query:      `UPDATE payouts SET status = 'reversed' WHERE id = 'p-values'`,
			constraint: "payouts_status_check",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := testDB.Exec(context.Background(), tt.query, tt.args...)

			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.ConstraintName != tt.constraint {
				t.Fatalf("err = %v, want violation of %s", err, tt.constraint)
			}
		})
	}

	assertStatus(t, "p-values", payouts.StatusPending)
}

func TestWorkerIgnoresEmptyQueueAndFinishedPayouts(t *testing.T) {
	s, p, host := setup(t)

	processed, err := s.ProcessOne(context.Background())
	if err != nil || processed {
		t.Fatalf("empty queue: processed = %v, err = %v", processed, err)
	}

	insertDuePayout(t, "p-done", host, 100)
	processOne(t, s)
	lookups := len(p.LookupCalls())

	processed, err = s.ProcessOne(context.Background())
	if err != nil || processed {
		t.Fatalf("finished payout: processed = %v, err = %v", processed, err)
	}
	if got := len(p.LookupCalls()); got != lookups {
		t.Fatalf("finished payout was looked up again: %d -> %d", lookups, got)
	}
}

func TestWorkerDoesNotClaimPayoutDuringFirstProviderCall(t *testing.T) {
	s, p, host := setup(t)
	entered, release := p.BlockNextCreate()
	defer release()

	done := make(chan error, 1)
	go func() {
		_, err := s.RequestPayout(context.Background(), "p-inflight", host, 100)
		done <- err
	}()

	waitFor(t, entered)

	processed, err := s.ProcessOne(context.Background())
	if err != nil || processed {
		t.Fatalf("processed = %v, err = %v", processed, err)
	}
	if lookups := p.LookupCalls(); len(lookups) != 0 {
		t.Fatalf("lookups = %v", lookups)
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("request: %v", err)
	}
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestSecondWorkerSkipsLockedPayout(t *testing.T) {
	s, p, host := setup(t)
	insertDuePayout(t, "p-locked", host, 100)

	entered, release := p.BlockNextLookup()
	defer release()

	type result struct {
		processed bool
		err       error
	}

	first := make(chan result, 1)
	go func() {
		processed, err := s.ProcessOne(context.Background())
		first <- result{processed, err}
	}()

	waitFor(t, entered)

	// The first worker still holds the lock. The second should skip this row.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	processed, err := s.ProcessOne(ctx)
	if err != nil || processed {
		t.Fatalf("second worker: processed = %v, err = %v", processed, err)
	}

	release()

	got := <-first
	if got.err != nil || !got.processed {
		t.Fatalf("first worker: %+v", got)
	}

	assertStatus(t, "p-locked", payouts.StatusPaid)
	if n := p.Applied(keyFor("p-locked")); n != 1 {
		t.Fatalf("applied = %d, want 1", n)
	}
	if calls := p.CreateCalls(); len(calls) != 1 {
		t.Fatalf("create calls = %+v", calls)
	}
}

func TestManyWorkersNeverDoublePay(t *testing.T) {
	s, p, host := setup(t)
	p.SetLookupDelay(3 * time.Millisecond)

	const payoutCount = 40
	for i := 0; i < payoutCount; i++ {
		insertDuePayout(t, fmt.Sprintf("p-%d", i), host, 100)
	}

	errs := make(chan error, 8)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				processed, err := s.ProcessOne(context.Background())
				if err != nil {
					errs <- err
					return
				}
				if !processed {
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("worker: %v", err)
	}

	if p.LookupOverlapped() {
		t.Fatal("two workers processed one payout at the same time")
	}

	creates := make(map[string]int)
	for _, call := range p.CreateCalls() {
		creates[call.Key]++
	}

	for i := 0; i < payoutCount; i++ {
		id := fmt.Sprintf("p-%d", i)
		assertStatus(t, id, payouts.StatusPaid)
		if creates[keyFor(id)] != 1 || p.Applied(keyFor(id)) != 1 {
			t.Fatalf("%s: creates = %d, applied = %d", id, creates[keyFor(id)], p.Applied(keyFor(id)))
		}
	}
}

func TestRunWorkerContinuesAfterError(t *testing.T) {
	s, p, host := setup(t)
	insertDuePayout(t, "p-run-first", host, 100)
	insertDuePayout(t, "p-run-second", host, 100)
	p.QueueLookupError(payoutstest.ErrAmbiguous)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.RunWorker(ctx, 5*time.Millisecond)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for statusOf(t, "p-run-second") != payouts.StatusPaid {
		select {
		case err := <-done:
			t.Fatalf("worker stopped after an error: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not reach the next payout")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestRunWorkerProcessesAndStopsOnCancel(t *testing.T) {
	s, _, host := setup(t)
	insertDuePayout(t, "p-run", host, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.RunWorker(ctx, 5*time.Millisecond)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for statusOf(t, "p-run") != payouts.StatusPaid {
		if time.Now().After(deadline) {
			t.Fatal("worker did not process the payout")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
