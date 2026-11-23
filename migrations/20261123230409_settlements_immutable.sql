-- +goose Up
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

-- +goose Down
DROP TRIGGER settlements_immutable ON settlements;

DROP FUNCTION reject_settlement_change();
