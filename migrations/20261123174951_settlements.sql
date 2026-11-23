-- +goose Up
CREATE TABLE settlements (
    reservation_id BIGINT PRIMARY KEY REFERENCES holds(reservation_id),
    used_amount BIGINT NOT NULL CHECK (used_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE settlements;
