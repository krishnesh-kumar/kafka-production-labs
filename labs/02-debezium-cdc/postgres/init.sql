-- A small order/payment schema. Debezium reads changes from the WAL via pgoutput.
CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    full_name   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL REFERENCES customers(id),
    status       TEXT NOT NULL DEFAULT 'PENDING',
    total_minor  BIGINT NOT NULL,          -- money in minor units, never floats
    currency     CHAR(3) NOT NULL DEFAULT 'EUR',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full before-images on UPDATE/DELETE, so consumers see what changed.
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Transactional outbox: the service writes business rows and the event in ONE transaction.
-- Debezium's EventRouter turns each outbox row into a clean domain event on outbox.event.<aggregatetype>.
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregatetype  TEXT NOT NULL,
    aggregateid    TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO customers (email, full_name) VALUES
  ('asha@example.com', 'Asha Rao'),
  ('li@example.com',   'Li Wei'),
  ('sam@example.com',  'Sam Okafor');

INSERT INTO orders (customer_id, status, total_minor, currency) VALUES
  (1, 'PAID',    125000, 'EUR'),
  (2, 'PENDING',  49900, 'EUR');
