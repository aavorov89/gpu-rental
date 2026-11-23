-- +goose Up
CREATE TABLE holds (
    reservation_id BIGINT PRIMARY KEY REFERENCES reservations(id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()

);

-- +goose Down
DROP TABLE holds;
