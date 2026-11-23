package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-rental/internal/ledger"
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

func seedCustomer(t *testing.T, balance int64) int64 {
	t.Helper()

	var id int64
	err := testDB.QueryRow(context.Background(), `INSERT INTO users (balance) VALUES ($1) RETURNING id`, balance).Scan(&id)
	if err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	return id
}

func seedReservation(t *testing.T, customerID int64) int64 {
	t.Helper()

	hostID := seedCustomer(t, 0)

	var id int64
	err := testDB.QueryRow(context.Background(), `
		INSERT INTO reservations (customer_id, host_id)
		VALUES ($1, $2)
		RETURNING id
	`, customerID, hostID).Scan(&id)
	if err != nil {
		t.Fatalf("seed reservation: %v", err)
	}

	return id
}

func seedReservations(t *testing.T, customerID int64, n int) []int64 {
	t.Helper()

	ids := make([]int64, n)
	for i := range ids {
		ids[i] = seedReservation(t, customerID)
	}

	return ids
}

func mustHold(t *testing.T, s *ledger.Service, customerID, reservationID, amount int64) ledger.Hold {
	t.Helper()

	hold, err := s.Hold(context.Background(), customerID, reservationID, amount)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}

	return hold
}

// assertAccount checks the customer's balance and adds up the open holds.
// It also checks that every settlement fits inside its hold.
func assertAccount(t *testing.T, customerID, wantBalance, wantHeld int64) {
	t.Helper()

	var balance, held, openHolds int64
	err := testDB.QueryRow(context.Background(), `
		SELECT u.balance, u.held, COALESCE((
			SELECT SUM(h.amount)
			FROM holds h
			JOIN reservations r ON r.id = h.reservation_id
			LEFT JOIN settlements s ON s.reservation_id = h.reservation_id
			WHERE r.customer_id = u.id AND s.reservation_id IS NULL
		), 0)::BIGINT
		FROM users u
		WHERE u.id = $1
	`, customerID).Scan(&balance, &held, &openHolds)
	if err != nil {
		t.Fatalf("read account: %v", err)
	}

	if balance != wantBalance || held != wantHeld {
		t.Fatalf("account: balance=%d held=%d, want balance=%d held=%d", balance, held, wantBalance, wantHeld)
	}

	if held != openHolds {
		t.Fatalf("users.held=%d but unsettled holds sum to %d", held, openHolds)
	}

	var overSettled int
	err = testDB.QueryRow(context.Background(), `
		SELECT COUNT(*)
		FROM settlements s
		JOIN holds h ON h.reservation_id = s.reservation_id
		WHERE s.used_amount > h.amount
	`).Scan(&overSettled)
	if err != nil {
		t.Fatalf("read settlements: %v", err)
	}

	if overSettled != 0 {
		t.Fatalf("%d settlements exceed their hold", overSettled)
	}
}

func countRows(t *testing.T, table string, reservationID int64) int {
	t.Helper()

	var n int
	err := testDB.QueryRow(context.Background(), `SELECT COUNT(*) FROM `+table+` WHERE reservation_id = $1`, reservationID).Scan(&n)
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}

	return n
}

func runConcurrently(n int, fn func(i int) error) []error {
	start := make(chan struct{})
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = fn(i)
		}()
	}

	close(start)
	wg.Wait()

	return errs
}

func TestHoldThenSettleReleasesLeftover(t *testing.T) {
	tests := []struct {
		name        string
		used        int64
		wantBalance int64
	}{
		{name: "partial usage", used: 120, wantBalance: 880},
		{name: "zero usage", used: 0, wantBalance: 1000},
		{name: "full usage", used: 300, wantBalance: 700},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := ledger.New(testDB)
			customer := seedCustomer(t, 1000)
			reservation := seedReservation(t, customer)

			hold := mustHold(t, s, customer, reservation, 300)
			if hold.ReservationID != reservation || hold.Amount != 300 || hold.CreatedAt.IsZero() {
				t.Fatalf("hold = %+v", hold)
			}
			assertAccount(t, customer, 1000, 300)

			settlement, err := s.Settle(context.Background(), reservation, tt.used)
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if settlement.ReservationID != reservation || settlement.UsedAmount != tt.used || settlement.CreatedAt.IsZero() {
				t.Fatalf("settlement = %+v", settlement)
			}
			if settlement.UsedAmount > hold.Amount {
				t.Fatalf("settled %d > held %d", settlement.UsedAmount, hold.Amount)
			}

			assertAccount(t, customer, tt.wantBalance, 0)
		})
	}
}

func TestHoldRejectsShortBalanceWithoutPartialState(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 100)
	reservation := seedReservation(t, customer)

	_, err := s.Hold(context.Background(), customer, reservation, 101)
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}

	assertAccount(t, customer, 100, 0)
	if n := countRows(t, "holds", reservation); n != 0 {
		t.Fatalf("holds rows = %d, want 0", n)
	}
}

func TestHoldCountsExistingHoldsAgainstAvailableBalance(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 100)
	ids := seedReservations(t, customer, 3)

	mustHold(t, s, customer, ids[0], 60)

	_, err := s.Hold(context.Background(), customer, ids[1], 41)
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	assertAccount(t, customer, 100, 60)

	mustHold(t, s, customer, ids[2], 40)
	assertAccount(t, customer, 100, 100)
}

func TestHoldRejectsInvalidAmount(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 100)
	reservation := seedReservation(t, customer)

	for _, amount := range []int64{0, -1} {
		_, err := s.Hold(context.Background(), customer, reservation, amount)
		if !errors.Is(err, ledger.ErrInvalidAmount) {
			t.Fatalf("amount %d: err = %v, want ErrInvalidAmount", amount, err)
		}
	}

	assertAccount(t, customer, 100, 0)
}

func TestHoldRejectsUnknownOrForeignReferences(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 100)
	other := seedCustomer(t, 100)
	reservation := seedReservation(t, customer)

	_, err := s.Hold(context.Background(), 999_999_999, reservation, 10)
	if !errors.Is(err, ledger.ErrCustomerNotFound) {
		t.Fatalf("unknown customer: err = %v", err)
	}

	_, err = s.Hold(context.Background(), customer, 999_999_999, 10)
	if !errors.Is(err, ledger.ErrReservationNotFound) {
		t.Fatalf("unknown reservation: err = %v", err)
	}

	_, err = s.Hold(context.Background(), other, reservation, 10)
	if !errors.Is(err, ledger.ErrReservationOwnerMismatch) {
		t.Fatalf("foreign reservation: err = %v", err)
	}

	assertAccount(t, customer, 100, 0)
	assertAccount(t, other, 100, 0)
}

func TestHoldIsIdempotent(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 100)
	reservation := seedReservation(t, customer)

	first := mustHold(t, s, customer, reservation, 100)

	// The first call reserved all 100. A retry should still get the same hold.
	second := mustHold(t, s, customer, reservation, 100)

	if first.ReservationID != second.ReservationID || first.Amount != second.Amount || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("retry returned another hold: %+v vs %+v", first, second)
	}

	assertAccount(t, customer, 100, 100)
	if n := countRows(t, "holds", reservation); n != 1 {
		t.Fatalf("holds rows = %d, want 1", n)
	}
}

func TestHoldRejectsConflictingAmount(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	mustHold(t, s, customer, reservation, 100)

	_, err := s.Hold(context.Background(), customer, reservation, 200)
	if !errors.Is(err, ledger.ErrHoldConflict) {
		t.Fatalf("err = %v, want ErrHoldConflict", err)
	}

	assertAccount(t, customer, 1000, 100)
}

func TestHoldRetryAfterSettleDoesNotHoldAgain(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	mustHold(t, s, customer, reservation, 100)
	if _, err := s.Settle(context.Background(), reservation, 60); err != nil {
		t.Fatalf("settle: %v", err)
	}

	mustHold(t, s, customer, reservation, 100)
	assertAccount(t, customer, 940, 0)
}

func TestConcurrentHoldsOnSameReservationHoldOnce(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	errs := runConcurrently(32, func(int) error {
		_, err := s.Hold(context.Background(), customer, reservation, 100)
		return err
	})

	for _, err := range errs {
		if err != nil {
			t.Fatalf("hold: %v", err)
		}
	}

	assertAccount(t, customer, 1000, 100)
	if n := countRows(t, "holds", reservation); n != 1 {
		t.Fatalf("holds rows = %d, want 1", n)
	}
}

func TestConcurrentHoldsCannotOverdraw(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	ids := seedReservations(t, customer, 30)

	errs := runConcurrently(len(ids), func(i int) error {
		_, err := s.Hold(context.Background(), customer, ids[i], 100)
		return err
	})

	var accepted int
	for i, err := range errs {
		switch {
		case err == nil:
			accepted++
			if n := countRows(t, "holds", ids[i]); n != 1 {
				t.Fatalf("accepted hold has %d rows", n)
			}
		case errors.Is(err, ledger.ErrInsufficientFunds):
			if n := countRows(t, "holds", ids[i]); n != 0 {
				t.Fatalf("rejected hold left %d rows", n)
			}
		default:
			t.Fatalf("hold: %v", err)
		}
	}

	if accepted != 10 {
		t.Fatalf("accepted = %d, want 10", accepted)
	}

	assertAccount(t, customer, 1000, 1000)
}

func TestSettleIsIdempotent(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	ids := seedReservations(t, customer, 2)

	mustHold(t, s, customer, ids[0], 100)
	mustHold(t, s, customer, ids[1], 200)

	first, err := s.Settle(context.Background(), ids[0], 60)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}

	second, err := s.Settle(context.Background(), ids[0], 60)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}

	if first.UsedAmount != second.UsedAmount || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("retry returned another settlement: %+v vs %+v", first, second)
	}

	assertAccount(t, customer, 940, 200)
}

func TestSettleRejectsConflictingAmount(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	mustHold(t, s, customer, reservation, 100)
	if _, err := s.Settle(context.Background(), reservation, 60); err != nil {
		t.Fatalf("settle: %v", err)
	}

	_, err := s.Settle(context.Background(), reservation, 70)
	if !errors.Is(err, ledger.ErrSettlementConflict) {
		t.Fatalf("err = %v, want ErrSettlementConflict", err)
	}

	assertAccount(t, customer, 940, 0)
}

func TestSettleRejectsMoreThanHeld(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	mustHold(t, s, customer, reservation, 100)

	_, err := s.Settle(context.Background(), reservation, 101)
	if !errors.Is(err, ledger.ErrUsedExceedsHold) {
		t.Fatalf("err = %v, want ErrUsedExceedsHold", err)
	}

	assertAccount(t, customer, 1000, 100)
	if n := countRows(t, "settlements", reservation); n != 0 {
		t.Fatalf("settlement rows = %d, want 0", n)
	}
}

func TestDatabaseEnforcesSettledWithinHold(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	open := seedReservation(t, customer)
	settled := seedReservation(t, customer)

	mustHold(t, s, customer, open, 100)
	mustHold(t, s, customer, settled, 100)

	if _, err := s.Settle(context.Background(), settled, 60); err != nil {
		t.Fatalf("settle: %v", err)
	}

	tests := []struct {
		name       string
		query      string
		args       []any
		constraint string
	}{
		{
			name:       "used above held",
			query:      `INSERT INTO settlements (reservation_id, held_amount, used_amount) VALUES ($1, $2, $3)`,
			args:       []any{open, 100, 101},
			constraint: "settlements_used_lte_held",
		},
		{
			name:       "forged held amount",
			query:      `INSERT INTO settlements (reservation_id, held_amount, used_amount) VALUES ($1, $2, $3)`,
			args:       []any{open, 101, 101},
			constraint: "settlements_hold_fkey",
		},
		{
			name:       "changed amount of settled hold",
			query:      `UPDATE holds SET amount = 50 WHERE reservation_id = $1`,
			args:       []any{settled},
			constraint: "settlements_hold_fkey",
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

	assertAccount(t, customer, 940, 100)
}

func TestSettleRejectsNegativeAmount(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	mustHold(t, s, customer, reservation, 100)

	_, err := s.Settle(context.Background(), reservation, -500)
	if !errors.Is(err, ledger.ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}

	assertAccount(t, customer, 1000, 100)
	if n := countRows(t, "settlements", reservation); n != 0 {
		t.Fatalf("settlement rows = %d, want 0", n)
	}
}

func TestSettleWithoutHold(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)

	for _, id := range []int64{reservation, 999_999_999} {
		_, err := s.Settle(context.Background(), id, 1)
		if !errors.Is(err, ledger.ErrReservationNotFound) {
			t.Fatalf("reservation %d: err = %v, want ErrReservationNotFound", id, err)
		}
	}

	assertAccount(t, customer, 1000, 0)
}

func TestConcurrentSettleSameAmountChargesOnce(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)
	mustHold(t, s, customer, reservation, 100)

	errs := runConcurrently(32, func(int) error {
		_, err := s.Settle(context.Background(), reservation, 60)
		return err
	})

	for _, err := range errs {
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
	}

	assertAccount(t, customer, 940, 0)
}

func TestConcurrentSettleDifferentAmountsHasOneWinner(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 1000)
	reservation := seedReservation(t, customer)
	mustHold(t, s, customer, reservation, 100)

	amount := func(i int) int64 { return int64(10 + i) }

	errs := runConcurrently(20, func(i int) error {
		_, err := s.Settle(context.Background(), reservation, amount(i))
		return err
	})

	var winners []int64
	for i, err := range errs {
		switch {
		case err == nil:
			winners = append(winners, amount(i))
		case errors.Is(err, ledger.ErrSettlementConflict):
		default:
			t.Fatalf("settle: %v", err)
		}
	}

	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}

	assertAccount(t, customer, 1000-winners[0], 0)
}

func TestConcurrentHoldAndSettleKeepInvariants(t *testing.T) {
	s := ledger.New(testDB)
	customer := seedCustomer(t, 10_000)
	toSettle := seedReservations(t, customer, 20)
	toHold := seedReservations(t, customer, 20)

	for _, id := range toSettle {
		mustHold(t, s, customer, id, 100)
	}

	errs := runConcurrently(40, func(i int) error {
		if i < 20 {
			_, err := s.Settle(context.Background(), toSettle[i], 40)
			return err
		}

		_, err := s.Hold(context.Background(), customer, toHold[i-20], 50)
		return err
	})

	for _, err := range errs {
		if err != nil {
			t.Fatalf("operation: %v", err)
		}
	}

	assertAccount(t, customer, 10_000-20*40, 20*50)
}
