-- +goose Up
ALTER TABLE reservations
    ADD CONSTRAINT reservations_id_customer_id_key UNIQUE (id, customer_id);

ALTER TABLE holds ADD COLUMN customer_id BIGINT;

UPDATE holds h
SET customer_id = r.customer_id
FROM reservations r
WHERE r.id = h.reservation_id;

ALTER TABLE holds
    ALTER COLUMN customer_id SET NOT NULL,
    DROP CONSTRAINT holds_reservation_id_fkey,
    ADD CONSTRAINT holds_reservation_customer_fkey
        FOREIGN KEY (reservation_id, customer_id)
        REFERENCES reservations (id, customer_id);

-- +goose Down
ALTER TABLE holds
    DROP CONSTRAINT holds_reservation_customer_fkey,
    ADD CONSTRAINT holds_reservation_id_fkey
        FOREIGN KEY (reservation_id) REFERENCES reservations (id),
    DROP COLUMN customer_id;

ALTER TABLE reservations DROP CONSTRAINT reservations_id_customer_id_key;
