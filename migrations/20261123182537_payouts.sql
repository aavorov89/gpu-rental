-- +goose Up
CREATE TABLE payouts (
    id         TEXT PRIMARY KEY,
    host_id    BIGINT NOT NULL references users(id),
    amount     BIGINT NOT NULL CHECK (amount > 0),
    status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'failed')),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX payouts_pending_idx
    ON payouts(created_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE payouts;
