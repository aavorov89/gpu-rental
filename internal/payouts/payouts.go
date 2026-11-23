package payouts

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Status string

const (
	StatusPending Status = "pending"
	StatusPaid    Status = "paid"
	StatusFailed  Status = "failed"
)

type Payout struct {
	ID             string
	HostID         int64
	Amount         int64
	Status         Status
	IdempotencyKey string
	CreatedAt      time.Time
}

// Transfer is what the provider has on record for an idempotency key.
type Transfer struct {
	HostID int64
	Amount int64
	Status Status
}

var (
	ErrInvalidRequest   = errors.New("invalid request")
	ErrHostNotFound     = errors.New("host not found")
	ErrPayoutConflict   = errors.New("payout already exists with other parameters")
	ErrTransferNotFound = errors.New("transfer not found at provider")
	ErrTransferRejected = errors.New("transfer rejected by provider")
	ErrTransferMismatch = errors.New("transfer at provider has another host or amount")
)

// payoutIDPattern keeps IDs safe in logs and short enough for a provider key.
var payoutIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type Provider interface {
	CreateTransfer(ctx context.Context, key string, hostID int64, amount int64) error
	LookupTransfer(ctx context.Context, key string) (Transfer, error)
}

type Service struct {
	db       *pgxpool.Pool
	provider Provider
}

func New(db *pgxpool.Pool, provider Provider) *Service {
	return &Service{
		db:       db,
		provider: provider,
	}
}

func (s *Service) RequestPayout(ctx context.Context, payoutID string, hostID int64, amount int64) (Payout, error) {

	if !payoutIDPattern.MatchString(payoutID) || hostID <= 0 || amount <= 0 {
		return Payout{}, ErrInvalidRequest
	}

	key := "payout:" + payoutID

	var payout Payout

	// ON CONFLICT has no target on purpose. With (id), concurrent retries of one
	// payout would fail on the unique idempotency key instead of doing nothing.
	err := s.db.QueryRow(ctx, `
		INSERT INTO payouts (id, host_id, amount, idempotency_key, next_attempt_at)
		VALUES ($1, $2, $3, $4, NOW() + INTERVAL '30 seconds')
		ON CONFLICT DO NOTHING
		RETURNING id, host_id, amount, status, idempotency_key, created_at
		`, payoutID, hostID, amount, key).Scan(
		&payout.ID, &payout.HostID, &payout.Amount, &payout.Status, &payout.IdempotencyKey, &payout.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		err = s.db.QueryRow(ctx, `
					SELECT id, host_id, amount, status, idempotency_key, created_at
					FROM payouts
					WHERE id = $1
				`, payoutID).Scan(
			&payout.ID, &payout.HostID, &payout.Amount, &payout.Status, &payout.IdempotencyKey, &payout.CreatedAt,
		)

		if errors.Is(err, pgx.ErrNoRows) {
			return Payout{}, fmt.Errorf("%w: idempotency key %q belongs to another payout", ErrPayoutConflict, key)
		}

		if err != nil {
			return Payout{}, err
		}

		if payout.HostID != hostID || payout.Amount != amount {
			return Payout{}, ErrPayoutConflict
		}

		return payout, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" && pgErr.ConstraintName == "payouts_host_id_fkey" {
		return Payout{}, ErrHostNotFound
	}

	if err != nil {
		return Payout{}, err
	}

	providerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err = s.provider.CreateTransfer(providerCtx, key, hostID, amount)
	if err != nil {
		return payout, fmt.Errorf("create transfer: %w", err)
	}

	return payout, nil
}
