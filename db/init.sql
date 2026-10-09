CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    amount_cents   INTEGER NOT NULL,
    merchant       TEXT NOT NULL,
    location       TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_txn_user_time ON transactions (user_id, timestamp DESC);

CREATE TABLE alerts (
    alert_id       SERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions (transaction_id),
    rule           TEXT NOT NULL,
    value          DOUBLE PRECISION,
    status         TEXT NOT NULL DEFAULT 'open',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alerts_txn ON alerts (transaction_id);