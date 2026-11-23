-- +goose Up
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

-- +goose Down
DROP TRIGGER holds_immutable ON holds;

DROP FUNCTION reject_hold_change();
