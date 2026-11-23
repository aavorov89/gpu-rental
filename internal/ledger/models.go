package ledger

import "time"

type Hold struct {
	ReservationID int64
	Amount        int64
	CreatedAt     time.Time
}

type Settlement struct {
	ReservationID int64
	UsedAmount    int64
	CreatedAt     time.Time
}
