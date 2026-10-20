CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    amount_cents   INTEGER NOT NULL,
    merchant       TEXT NOT NULL,
    location       TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ DEFAULT clock_timestamp()
);

CREATE INDEX idx_txn_user_time ON transactions (user_id, timestamp DESC);
CREATE INDEX idx_txn_time ON transactions (timestamp DESC);

CREATE TABLE alerts (
    alert_id       SERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions (transaction_id),
    rule           TEXT NOT NULL,
    value          DOUBLE PRECISION,
    status         TEXT NOT NULL DEFAULT 'open',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alerts_txn ON alerts (transaction_id);

CREATE TABLE operators (
    operator_id     SERIAL PRIMARY KEY,
    email           TEXT UNIQUE NOT NULL,
    hashed_password TEXT NOT NULL
);

CREATE TABLE risk_profiles (
    user_id              INTEGER PRIMARY KEY,
    transaction_count    INTEGER NOT NULL DEFAULT 0,
    flagged_transactions INTEGER NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);