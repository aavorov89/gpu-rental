-- +goose Up
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    held BIGINT NOT NULL DEFAULT 0 CHECK (held >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (held <= balance)
);

CREATE TABLE reservations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES users (id),
    host_id BIGINT NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT reservations_id_customer_id_key UNIQUE (id, customer_id)
);

CREATE TABLE holds (
    reservation_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT holds_reservation_id_amount_key UNIQUE (reservation_id, amount),
    CONSTRAINT holds_reservation_customer_fkey
        FOREIGN KEY (reservation_id, customer_id)
        REFERENCES reservations (id, customer_id)
);

CREATE TABLE settlements (
    reservation_id BIGINT PRIMARY KEY,
    held_amount BIGINT NOT NULL,
    used_amount BIGINT NOT NULL CHECK (used_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT settlements_hold_fkey
        FOREIGN KEY (reservation_id, held_amount)
        REFERENCES holds (reservation_id, amount),
    CONSTRAINT settlements_used_lte_held CHECK (used_amount <= held_amount)
);

CREATE TABLE payouts (
    id TEXT PRIMARY KEY,
    host_id BIGINT NOT NULL REFERENCES users (id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'failed')),
    idempotency_key TEXT NOT NULL UNIQUE,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX payouts_pending_idx
    ON payouts (next_attempt_at, created_at, id)
    WHERE status = 'pending';

-- +goose StatementBegin
CREATE FUNCTION reject_hold_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'holds cannot be updated or deleted'
        USING ERRCODE = 'restrict_violation', CONSTRAINT = 'holds_immutable';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER holds_immutable
    BEFORE UPDATE OR DELETE ON holds
    FOR EACH ROW EXECUTE FUNCTION reject_hold_change();

-- +goose StatementBegin
CREATE FUNCTION reject_settlement_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'settlements cannot be updated or deleted'
        USING ERRCODE = 'restrict_violation', CONSTRAINT = 'settlements_immutable';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER settlements_immutable
    BEFORE UPDATE OR DELETE ON settlements
    FOR EACH ROW EXECUTE FUNCTION reject_settlement_change();

-- +goose StatementBegin
CREATE FUNCTION guard_payout_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' THEN
            RAISE EXCEPTION 'a new payout must be pending'
                USING ERRCODE = 'restrict_violation', CONSTRAINT = 'payouts_state_machine';
        END IF;

        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'payouts cannot be deleted'
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'payouts_state_machine';
    END IF;

    IF OLD.status <> 'pending'
        OR (NEW.id, NEW.host_id, NEW.amount, NEW.idempotency_key, NEW.created_at)
            IS DISTINCT FROM (OLD.id, OLD.host_id, OLD.amount, OLD.idempotency_key, OLD.created_at)
    THEN
        RAISE EXCEPTION 'only the status and retry time of a pending payout can change'
            USING ERRCODE = 'restrict_violation', CONSTRAINT = 'payouts_state_machine';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER payouts_state_machine
    BEFORE INSERT OR UPDATE OR DELETE ON payouts
    FOR EACH ROW EXECUTE FUNCTION guard_payout_change();

-- +goose Down
DROP TABLE payouts;
DROP TABLE settlements;
DROP TABLE holds;
DROP TABLE reservations;
DROP TABLE users;

DROP FUNCTION guard_payout_change();
DROP FUNCTION reject_settlement_change();
DROP FUNCTION reject_hold_change();
