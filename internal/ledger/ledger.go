package ledger

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) Hold(ctx context.Context, customerID int64, reservationID int64, amount int64) (Hold, error) {
	if amount <= 0 {
		return Hold{}, ErrInvalidAmount
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})

	if err != nil {
		return Hold{}, err
	}

	defer tx.Rollback(ctx)

	var balance, held int64

	err = tx.QueryRow(ctx, `
				SELECT balance,held
				FROM users
				WHERE id = $1
				FOR NO KEY UPDATE
				`, customerID).Scan(&balance, &held)

	if errors.Is(err, pgx.ErrNoRows) {
		return Hold{}, ErrCustomerNotFound
	}

	if err != nil {
		return Hold{}, err
	}

	var ownerID int64

	err = tx.QueryRow(ctx, `
				SELECT customer_id
				FROM reservations
				WHERE id = $1
				FOR UPDATE
				`, reservationID).Scan(&ownerID)

	if errors.Is(err, pgx.ErrNoRows) {
		return Hold{}, ErrReservationNotFound
	}

	if err != nil {
		return Hold{}, err
	}

	if ownerID != customerID {
		return Hold{}, ErrReservationOwnerMismatch
	}

	var hold Hold

	err = tx.QueryRow(ctx, `
				SELECT reservation_id, amount, created_at
			    FROM holds
				WHERE reservation_id = $1
				`, reservationID).Scan(&hold.ReservationID, &hold.Amount, &hold.CreatedAt)

	if err == nil {
		if hold.Amount != amount {
			return Hold{}, ErrHoldConflict
		}

		if err := tx.Commit(ctx); err != nil {
			return Hold{}, err
		}

		return hold, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return Hold{}, err
	}

	if balance-held < amount {
		return Hold{}, ErrInsufficientFunds
	}

	err = tx.QueryRow(ctx, `
				INSERT INTO holds (reservation_id, customer_id, amount)
				VALUES ($1, $2, $3)
				RETURNING reservation_id, amount, created_at
				`, reservationID, customerID, amount).Scan(&hold.ReservationID, &hold.Amount, &hold.CreatedAt)

	if err != nil {
		return Hold{}, err
	}

	_, err = tx.Exec(ctx, `
				UPDATE users
				SET held = held + $1
				WHERE id = $2
				`, amount, customerID)

	if err != nil {
		return Hold{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Hold{}, err
	}

	return hold, nil
}

func (s *Service) Settle(ctx context.Context, reservationID int64, usedAmount int64) (Settlement, error) {
	if usedAmount < 0 {
		return Settlement{}, ErrInvalidAmount
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})

	if err != nil {
		return Settlement{}, err
	}

	defer tx.Rollback(ctx)

	var customerID, heldAmount int64

	err = tx.QueryRow(ctx, `
		SELECT customer_id, amount
		FROM holds
		WHERE reservation_id = $1`, reservationID).Scan(&customerID, &heldAmount)

	if errors.Is(err, pgx.ErrNoRows) {
		var reservationExists bool

		err = tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM reservations WHERE id = $1)`, reservationID).Scan(&reservationExists)

		if err != nil {
			return Settlement{}, err
		}

		if reservationExists {
			return Settlement{}, ErrHoldNotFound
		}

		return Settlement{}, ErrReservationNotFound
	}

	if err != nil {
		return Settlement{}, err
	}

	var locked int

	err = tx.QueryRow(ctx, `
		SELECT 1 FROM users WHERE id = $1 FOR NO KEY UPDATE`, customerID).Scan(&locked)

	if err != nil {
		return Settlement{}, err
	}

	var settlement Settlement

	err = tx.QueryRow(ctx, `
		SELECT reservation_id, used_amount, created_at
		FROM settlements
		WHERE reservation_id = $1
				`, reservationID).Scan(&settlement.ReservationID, &settlement.UsedAmount, &settlement.CreatedAt)

	if err == nil {
		if settlement.UsedAmount != usedAmount {
			return Settlement{}, ErrSettlementConflict
		}

		if err := tx.Commit(ctx); err != nil {
			return Settlement{}, err
		}

		return settlement, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return Settlement{}, err
	}

	if usedAmount > heldAmount {
		return Settlement{}, ErrUsedExceedsHold

	}

	err = tx.QueryRow(ctx, `
				INSERT INTO settlements (reservation_id, held_amount, used_amount)
				VALUES ($1, $2, $3)
				RETURNING reservation_id, used_amount, created_at
				`, reservationID, heldAmount, usedAmount).Scan(&settlement.ReservationID, &settlement.UsedAmount, &settlement.CreatedAt)

	if err != nil {
		return Settlement{}, err
	}

	_, err = tx.Exec(ctx, `
				UPDATE users
				SET balance = balance - $1,
				held = held - $2
				WHERE id = $3
				`, usedAmount, heldAmount, customerID)

	if err != nil {
		return Settlement{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Settlement{}, err
	}

	return settlement, nil
}
