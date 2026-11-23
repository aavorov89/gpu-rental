-- +goose Up
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    held BIGINT NOT NULL DEFAULT 0 CHECK (held >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (held <= balance)
);

-- +goose Down
DROP TABLE users;
