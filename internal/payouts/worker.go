package payouts

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Service) ProcessOne(ctx context.Context) (bool, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})

	if err != nil {
		return false, err
	}

	defer tx.Rollback(ctx)

	var payout Payout

	err = tx.QueryRow(ctx, `
		SELECT id, host_id, amount, status, idempotency_key, created_at
		FROM payouts
		WHERE status = 'pending'
		AND next_attempt_at <= NOW()
		ORDER BY next_attempt_at, created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(
		&payout.ID, &payout.HostID, &payout.Amount, &payout.Status, &payout.IdempotencyKey, &payout.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	providerCtx, cancel := context.WithTimeout(ctx, 3*time.Second)

	transfer, providerErr := s.provider.LookupTransfer(providerCtx, payout.IdempotencyKey)

	if errors.Is(providerErr, ErrTransferNotFound) {
		providerErr = s.provider.CreateTransfer(providerCtx, payout.IdempotencyKey, payout.HostID, payout.Amount)

		if providerErr == nil || errors.Is(providerErr, ErrTransferRejected) {
			transfer, providerErr = s.provider.LookupTransfer(providerCtx, payout.IdempotencyKey)
		}
	}

	cancel()

	if providerErr == nil && (transfer.HostID != payout.HostID || transfer.Amount != payout.Amount) {
		providerErr = fmt.Errorf("%w: provider has host %d amount %d, payout has host %d amount %d",
			ErrTransferMismatch, transfer.HostID, transfer.Amount, payout.HostID, payout.Amount)
	}

	status := transfer.Status

	if providerErr != nil {
		status = StatusPending
	}

	switch status {
	case StatusPending, StatusPaid, StatusFailed:
	default:
		providerErr = fmt.Errorf("unexpected provider status: %q", status)
		status = StatusPending
	}

	if status == StatusPending {
		_, err = tx.Exec(ctx, `
			UPDATE payouts
			SET next_attempt_at = clock_timestamp() + INTERVAL '5 seconds'
			WHERE id = $1 AND status = 'pending'
		`, payout.ID)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE payouts
			SET status = $2
			WHERE id = $1 AND status = 'pending'
		`, payout.ID, status)
	}

	if err != nil {
		return false, fmt.Errorf("update payout: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit payout: %w", err)
	}

	if providerErr != nil {
		return true, fmt.Errorf("reconcile payout %s: %w", payout.ID, providerErr)
	}

	return true, nil
}

func (s *Service) RunWorker(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("worker interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := s.ProcessOne(attemptCtx)
		cancel()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			log.Printf("payout worker: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
