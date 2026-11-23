package payouts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
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

var (
	ErrInvalidRequest   = errors.New("invalid request")
	ErrPayoutConflict   = errors.New("payout already exists with other parameters")
	ErrTransferNotFound = errors.New("transfer not found at provider")
	ErrTransferRejected = errors.New("transfer rejected by provider")
)

type Provider interface {
	CreateTransfer(ctx context.Context, key string, hostID int64, amount int64) error
	LookupTransfer(ctx context.Context, key string) (Status, error)
}

type Service struct {
	db       *sql.DB
	provider Provider
}

func New(db *sql.DB, provider Provider) *Service {
	return &Service{
		db:       db,
		provider: provider,
	}
}

func (s *Service) RequestPayout(ctx context.Context, payoutID string, hostID int64, amount int64) (Payout, error) {

	if payoutID == "" || hostID <= 0 || amount <= 0 {
		return Payout{}, ErrInvalidRequest
	}

	key := "payout:" + payoutID

	var payout Payout

	err := s.db.QueryRowContext(ctx, `
		INSERT INTO payouts (id, host_id, amount, idempotency_key, next_attempt_at)
		VALUES ($1, $2, $3, $4, NOW() + INTERVAL '30 seconds')
		ON CONFLICT (id) DO NOTHING
		RETURNING id, host_id, amount, status, idempotency_key, created_at
		`, payoutID, hostID, amount, key).Scan(
		&payout.ID, &payout.HostID, &payout.Amount, &payout.Status, &payout.IdempotencyKey, &payout.CreatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		err = s.db.QueryRowContext(ctx, `
					SELECT id, host_id, amount, status, idempotency_key, created_at
					FROM payouts
					WHERE id = $1
				`, payoutID).Scan(
			&payout.ID, &payout.HostID, &payout.Amount, &payout.Status, &payout.IdempotencyKey, &payout.CreatedAt,
		)

		if err != nil {
			return Payout{}, err
		}

		if payout.HostID != hostID || payout.Amount != amount {
			return Payout{}, ErrPayoutConflict
		}

		return payout, nil
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
