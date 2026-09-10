-- Ledger schema.
--
-- This file is idempotent (safe to run against a database that already has
-- some or all of these objects) so it can double as both the Postgres
-- container's init script and the schema bootstrap used by the test suite.
--
-- Design invariants enforced here, not just in application code:
--   1. entries are append-only: UPDATE and DELETE are rejected by trigger.
--   2. entries for a transaction must sum to zero: enforced by a deferred
--      constraint trigger that fires at COMMIT.
-- See README.md "Decision log" for the reasoning.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- accounts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    currency        CHAR(3) NOT NULL,
    -- External/platform-funding accounts are allowed to go negative: they
    -- model money entering the system from outside the ledger (e.g. a card
    -- network). Ordinary customer/merchant accounts must not go negative.
    allow_negative  BOOLEAN NOT NULL DEFAULT FALSE,
    -- Denormalized balance, maintained transactionally alongside entries.
    -- It exists solely so we can write a test asserting
    -- cached_balance == SUM(entries.amount); SUM(entries.amount) remains
    -- the source of truth.
    cached_balance  BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- transactions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type                    TEXT NOT NULL CHECK (type IN ('charge', 'refund')),
    -- Denormalized face-value amount (always positive, minor units) and
    -- currency. Business rules like "refunds cannot exceed the original
    -- charge" read this instead of re-deriving it from entries every time.
    -- The books themselves are still only ever balanced via entries.
    amount                  BIGINT NOT NULL CHECK (amount > 0),
    currency                CHAR(3) NOT NULL,
    source_account_id       UUID NOT NULL REFERENCES accounts(id),
    destination_account_id  UUID NOT NULL REFERENCES accounts(id),
    -- Set only for refunds: the charge transaction being refunded.
    parent_transaction_id   UUID REFERENCES transactions(id),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_parent
    ON transactions(parent_transaction_id);

-- ---------------------------------------------------------------------------
-- entries — append-only, the actual ledger
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entries (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  UUID NOT NULL REFERENCES transactions(id),
    account_id      UUID NOT NULL REFERENCES accounts(id),
    -- Signed, minor units (bani/cents). Never floats, never NUMERIC.
    amount          BIGINT NOT NULL CHECK (amount <> 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entries_account ON entries(account_id);
CREATE INDEX IF NOT EXISTS idx_entries_transaction ON entries(transaction_id);

-- Append-only enforcement: entries may never be mutated or removed. A
-- refund is a new transaction with opposite-signed entries, never an edit
-- of the original row. This is what makes the ledger a real audit trail
-- instead of a mutable balance counter.
CREATE OR REPLACE FUNCTION reject_entry_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'entries are append-only: % on entries is not permitted', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_entries_no_update ON entries;
CREATE TRIGGER trg_entries_no_update
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION reject_entry_mutation();

-- Balance invariant: every transaction's entries must sum to zero. This is
-- checked with a constraint trigger deferred to COMMIT, so a transaction
-- can insert its entries one at a time (or in any order) and only gets
-- validated once, atomically, right before the surrounding DB transaction
-- commits. If application code ever gets this wrong, Postgres refuses the
-- commit rather than silently accepting unbalanced books.
CREATE OR REPLACE FUNCTION check_entries_balanced() RETURNS TRIGGER AS $$
DECLARE
    txn_id UUID;
    total  BIGINT;
BEGIN
    txn_id := COALESCE(NEW.transaction_id, OLD.transaction_id);
    SELECT COALESCE(SUM(amount), 0) INTO total
    FROM entries
    WHERE transaction_id = txn_id;

    IF total <> 0 THEN
        RAISE EXCEPTION 'transaction % entries do not sum to zero (got %)',
            txn_id, total;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_entries_balanced ON entries;
CREATE CONSTRAINT TRIGGER trg_entries_balanced
    AFTER INSERT OR UPDATE OR DELETE ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_entries_balanced();

-- Convenience view: computed balance, source of truth.
CREATE OR REPLACE VIEW account_balances AS
    SELECT account_id, SUM(amount)::BIGINT AS balance
    FROM entries
    GROUP BY account_id;

-- ---------------------------------------------------------------------------
-- idempotency_keys
-- ---------------------------------------------------------------------------
-- TTL is 24 hours. Expiry is
-- enforced by a reaper (see internal/idempotency) that periodically deletes
-- rows past expires_at, which is also what allows a key value to be reused
-- once it has expired (the UNIQUE constraint only ever sees live rows).
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,
    -- NULL while the original request is still in flight.
    status_code     INT,
    -- Stored as TEXT, not JSONB: JSONB reformats its input (e.g. adds a
    -- space after ':') when read back, which would break the "replay
    -- returns the exact original response bytes" guarantee.
    response_body   TEXT,
    transaction_id  UUID REFERENCES transactions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires_at ON idempotency_keys(expires_at);

-- ---------------------------------------------------------------------------
-- outbox_events — transactional outbox for webhook delivery
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type    TEXT NOT NULL,
    aggregate_id      UUID NOT NULL,
    event_type        TEXT NOT NULL,
    payload           JSONB NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'sent', 'dead_letter')),
    attempts          INT NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events(next_attempt_at)
    WHERE status = 'pending';
