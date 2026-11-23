-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION guard_payout_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
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
    BEFORE UPDATE OR DELETE ON payouts
    FOR EACH ROW EXECUTE FUNCTION guard_payout_change();

-- +goose Down
DROP TRIGGER payouts_state_machine ON payouts;

DROP FUNCTION guard_payout_change();
