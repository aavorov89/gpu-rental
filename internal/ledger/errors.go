package ledger

import "errors"

var (
	ErrInvalidAmount            = errors.New("invalid amount")
	ErrCustomerNotFound         = errors.New("customer not found")
	ErrReservationNotFound      = errors.New("reservation not found")
	ErrReservationOwnerMismatch = errors.New("reservation belongs to another customer")
	ErrHoldConflict             = errors.New("hold already exists with another amount")
	ErrSettlementConflict       = errors.New("settlement already exists with another amount")
	ErrInsufficientFunds        = errors.New("insufficient funds")
	ErrUsedExceedsHold          = errors.New("used amount exceeds hold")
)
