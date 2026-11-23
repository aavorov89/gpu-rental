package ledger

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Service struct {
	db *sql.DB
}

func New(db *sql.DB) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) Hold(ctx context.Context, customerID int64, reservationID int64, amount int64) (Hold, error) {
	if amount <= 0 {
		return Hold{}, ErrInvalidAmount
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})

	if err != nil {
		return Hold{}, err
	}

	defer tx.Rollback()

	var balance, held int64

	err = tx.QueryRowContext(ctx, `
				SELECT balance,held
				FROM users
				WHERE id = $1
				FOR UPDATE
				`, customerID).Scan(&balance, &held)

	if errors.Is(err, sql.ErrNoRows) {
		return Hold{}, ErrCustomerNotFound
	}

	if err != nil {
		return Hold{}, err
	}

	var ownerID int64

	err = tx.QueryRowContext(ctx, `
				SELECT customer_id
				FROM reservations
				WHERE id = $1
				FOR UPDATE
				`, reservationID).Scan(&ownerID)

	if errors.Is(err, sql.ErrNoRows) {
		return Hold{}, ErrReservationNotFound
	}

	if err != nil {
		return Hold{}, err
	}

	if ownerID != customerID {
		return Hold{}, ErrReservationOwnerMismatch
	}

	var hold Hold

	err = tx.QueryRowContext(ctx, `
				SELECT reservation_id, amount, created_at
			    FROM holds
				WHERE reservation_id = $1
				`, reservationID).Scan(&hold.ReservationID, &hold.Amount, &hold.CreatedAt)

	if err == nil {
		if hold.Amount != amount {
			return Hold{}, ErrHoldConflict
		}

		if err := tx.Commit(); err != nil {
			return Hold{}, err
		}

		return hold, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return Hold{}, err
	}

	if balance-held < amount {
		return Hold{}, ErrInsufficientFunds
	}

	err = tx.QueryRowContext(ctx, `
				INSERT INTO holds (reservation_id, amount)
				VALUES ($1, $2)
				RETURNING reservation_id, amount, created_at
				`, reservationID, amount).Scan(&hold.ReservationID, &hold.Amount, &hold.CreatedAt)

	if err != nil {
		return Hold{}, err
	}

	_, err = tx.ExecContext(ctx, `
				UPDATE users
				SET held = held + $1
				WHERE id = $2
				`, amount, customerID)

	if err != nil {
		return Hold{}, err
	}

	if err := tx.Commit(); err != nil {
		return Hold{}, err
	}

	return hold, nil
}

func (s *Service) Settle(ctx context.Context, reservationID int64, usedAmount int64) (Settlement, error) {
	if usedAmount < 0 {
		return Settlement{}, ErrInvalidAmount
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})

	if err != nil {
		return Settlement{}, err
	}

	defer tx.Rollback()

	var customerID, heldAmount int64

	err = tx.QueryRowContext(ctx, `
		SELECT r.customer_id, h.amount
		FROM holds h
		JOIN reservations r ON r.id = h.reservation_id
		WHERE h.reservation_id = $1`, reservationID).Scan(&customerID, &heldAmount)

	if errors.Is(err, sql.ErrNoRows) {
		return Settlement{}, ErrReservationNotFound
	}

	if err != nil {
		return Settlement{}, err
	}

	var locked int

	err = tx.QueryRowContext(ctx, `
		SELECT 1 FROM users WHERE id = $1 FOR UPDATE`, customerID).Scan(&locked)

	if err != nil {
		return Settlement{}, err
	}

	var settlement Settlement

	err = tx.QueryRowContext(ctx, `
		SELECT reservation_id, used_amount, created_at
		FROM settlements
		WHERE reservation_id = $1
				`, reservationID).Scan(&settlement.ReservationID, &settlement.UsedAmount, &settlement.CreatedAt)

	if err == nil {
		if settlement.UsedAmount != usedAmount {
			return Settlement{}, ErrSettlementConflict
		}

		return settlement, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return Settlement{}, err
	}

	if usedAmount > heldAmount {
		return Settlement{}, ErrUsedExceedsHold

	}

	err = tx.QueryRowContext(ctx, `
				INSERT INTO settlements (reservation_id, used_amount)
				VALUES ($1, $2)
				RETURNING reservation_id, used_amount, created_at
				`, reservationID, usedAmount).Scan(&settlement.ReservationID, &settlement.UsedAmount, &settlement.CreatedAt)

	if err != nil {
		return Settlement{}, err
	}

	_, err = tx.ExecContext(ctx, `
				UPDATE users
				SET balance = balance - $1,
				held = held - $2
				WHERE id = $3
				`, usedAmount, heldAmount, customerID)

	if err != nil {
		return Settlement{}, err
	}

	if err := tx.Commit(); err != nil {
		return Settlement{}, err
	}

	return settlement, nil
}
