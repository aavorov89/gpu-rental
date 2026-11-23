-- +goose Up
ALTER TABLE payouts
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DROP INDEX payouts_pending_idx;

CREATE INDEX payouts_pending_idx
    ON payouts (next_attempt_at, created_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP INDEX payouts_pending_idx;

ALTER TABLE payouts DROP COLUMN next_attempt_at;

CREATE INDEX payouts_pending_idx
    ON payouts (created_at, id)
    WHERE status = 'pending';
