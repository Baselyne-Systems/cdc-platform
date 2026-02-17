-- CDC Demo seed schema
-- Tables use REPLICA IDENTITY FULL so Debezium captures before-images on UPDATE/DELETE.

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) NOT NULL UNIQUE,
    full_name   VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT          NOT NULL REFERENCES customers(id),
    total_cents  BIGINT       NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- Seed data
INSERT INTO customers (email, full_name) VALUES
    ('alice@example.com', 'Alice Johnson'),
    ('bob@example.com',   'Bob Smith');

INSERT INTO orders (customer_id, total_cents, status) VALUES
    (1, 4999,  'completed'),
    (2, 12500, 'pending');
