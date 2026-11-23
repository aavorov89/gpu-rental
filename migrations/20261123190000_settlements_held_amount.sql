-- +goose Up
ALTER TABLE holds
    ADD CONSTRAINT holds_reservation_id_amount_key UNIQUE (reservation_id, amount);

ALTER TABLE settlements ADD COLUMN held_amount BIGINT;

UPDATE settlements s
SET held_amount = h.amount
FROM holds h
WHERE h.reservation_id = s.reservation_id;

ALTER TABLE settlements
    ALTER COLUMN held_amount SET NOT NULL,
    DROP CONSTRAINT settlements_reservation_id_fkey,
    ADD CONSTRAINT settlements_hold_fkey
        FOREIGN KEY (reservation_id, held_amount)
        REFERENCES holds (reservation_id, amount),
    ADD CONSTRAINT settlements_used_lte_held CHECK (used_amount <= held_amount);

-- +goose Down
ALTER TABLE settlements
    DROP CONSTRAINT settlements_used_lte_held,
    DROP CONSTRAINT settlements_hold_fkey,
    ADD CONSTRAINT settlements_reservation_id_fkey
        FOREIGN KEY (reservation_id) REFERENCES holds (reservation_id),
    DROP COLUMN held_amount;

ALTER TABLE holds DROP CONSTRAINT holds_reservation_id_amount_key;
