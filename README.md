# ledger-engine

A double-entry ledger service in Go and Postgres: charges, refunds,
idempotency keys, and a transactional outbox for webhooks. No ORM — pgx
over `database/sql`-style access, so the transaction semantics that make
this interesting stay visible instead of hidden behind a query builder.

This README is a decision log, not a feature list. Each section explains
a choice and, where it matters, shows what happens if you make the other
choice.

## Schema

Four core tables, plus a fifth for idempotency:

- **accounts** — id, currency, `allow_negative`, and a `cached_balance`
  that exists purely so a test can assert it against `SUM(entries.amount)`
  (see "Cached balance" below).
- **transactions** — one row per charge or refund. Carries a denormalized
  `amount` (always positive) and `currency` for business rules like the
  refund cap; the *books* are never balanced from this table, only from
  entries.
- **entries** — the actual ledger. Append-only, signed `BIGINT` amounts in
  minor units (bani/cents), one row per account touched per transaction.
- **outbox_events** — see "Outbox" below.
- **idempotency_keys** — see "Idempotency keys" below.

### Minor units, never floats, never gratuitous `NUMERIC`

Amounts are `BIGINT` counts of the smallest currency unit. A float would
silently corrupt a balance; `NUMERIC` would work but buys nothing here —
there's no division, no rounding, no fractional minor units — so it would
just be `BIGINT` with extra formatting overhead. If this project grew FX
conversion, `NUMERIC` (or a fixed-point rate table) would come back into
the conversation.

### Append-only entries, enforced in the database

`entries` never gets `UPDATE` or `DELETE`. A refund is a *new* transaction
with opposite-signed entries pointing back at the original charge, not an
edit of it. A `BEFORE UPDATE OR DELETE` trigger (`reject_entry_mutation`)
makes this a database guarantee, not an application convention:

```sql
CREATE TRIGGER trg_entries_no_update
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION reject_entry_mutation();
```

This is what double-entry actually buys you over a mutable balance
column: a complete, tamper-evident history. Every balance is a replay of
history, not a number someone could have quietly edited.

### Zero-sum, enforced by a deferred constraint trigger

The other invariant: every transaction's entries must sum to zero. This
is checked by a `CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED`,
which fires once per statement at `COMMIT`, not at each individual
`INSERT`:

```sql
CREATE CONSTRAINT TRIGGER trg_entries_balanced
    AFTER INSERT OR UPDATE OR DELETE ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_entries_balanced();
```

Deferring to commit matters mechanically: application code inserts the
debit entry, then the credit entry, as two separate statements. If the
check ran immediately after the first insert, every transaction would
fail by construction. Deferring lets Postgres see the whole transaction's
final state before deciding, which is the only point at which "balanced"
is even a meaningful question to ask.

Verified directly against Postgres during development:

```
-- unbalanced (-50, no offsetting +50), committed anyway:
ERROR:  transaction 44444444-... entries do not sum to zero (got -50)
-- the whole transaction (including the transactions row) rolled back
```

A reviewer noticing the invariant lives in the database, not just in a
service method, is the point of putting it there — it holds even against
a future direct `psql` session, a buggy migration, or a second
service that talks to the same database.

### Cached balance

`accounts.cached_balance` is maintained transactionally alongside every
entry write (`cached_balance = cached_balance ± amount` in the same DB
transaction as the entries). It exists to let
`TestConcurrency_ForUpdatePreventsOvercommit` assert `cached_balance ==
SUM(entries.amount)` after a stress test, i.e. to prove the cache is
never allowed to drift from the source of truth. `SUM(entries.amount)` —
`ledger.Service.ComputedBalance` — remains authoritative; the cache is
only ever a read-path optimization someone could delete without changing
what the system means.

## Charge and refund

`POST /charges` does exactly what it looks like: one DB transaction,
insert the transaction row, insert both entries, update both cached
balances, insert one outbox event, commit. `POST /refunds` does the same
shape in reverse — see "The refund race" for the one place it's not that
simple.

## Concurrency control: `SELECT ... FOR UPDATE`, not `SERIALIZABLE`

Charges enforce "an account cannot go negative unless it's flagged
`allow_negative`" (accounts representing money entering the system from
outside the ledger, e.g. a card network, are `allow_negative`; ordinary
accounts are not). That rule is check-then-write by nature: read the
balance, decide, write. Under concurrency, check-then-write is a lost
update waiting to happen.

**I picked row locks over `SERIALIZABLE` + retry.** `ChargeTx` locks the
source account with `SELECT ... FOR UPDATE` before reading its balance,
so two concurrent charges against the same account serialize on that
lock instead of racing on a snapshot read:

```go
tx.QueryRow(ctx, `
    SELECT id, currency, allow_negative, cached_balance
    FROM accounts WHERE id = $1
    FOR UPDATE`, req.SourceAccountID)
```

The destination account is read without a lock — its `cached_balance` is
updated with `cached_balance = cached_balance + $1`, a relative write
Postgres applies against the row's current value under its own
short-lived write lock, so it can't lose an update even unlocked. Only
one account (the source) is ever explicitly locked per call, which also
means two charges moving money in opposite directions between the same
pair of accounts can't deadlock on lock ordering — there's no ordering
decision to get wrong when there's only ever one lock held.

Why row locks over `SERIALIZABLE`: row locks fail fast and predictably —
a losing transaction blocks briefly and then sees an up-to-date balance,
never an abort it has to notice and retry. `SERIALIZABLE` would remove
the need to reason about *which* row to lock, but trades that for a
different burden: every write path needs a retry loop keyed on Postgres
error `40001`, and under real contention it can abort transactions that
did nothing wrong, purely because the scheduler picked them as the
loser. For a ledger where "this write should basically never spuriously
fail" is a value on its own, the lock is the simpler, more legible
choice. `SERIALIZABLE` would have been the better call in a system with
many *different* tables' worth of invariants to protect and no appetite
to reason about lock placement per query.

### Watching it actually fail

I didn't take the lock on faith. I removed the `FOR UPDATE` clause,
funded an account with exactly enough balance for 10 charges of 100
minor units, and fired 50 concurrent goroutines at it
(`TestConcurrency_ForUpdatePreventsOvercommit`, run against the unlocked
version):

```
successes = 17, want exactly 10 (1000/100)
source cached_balance = -700, want 0
source account went negative: -700
```

Seventeen of fifty charges succeeded — 70% more than the account could
actually cover — because a batch of goroutines all read the same
pre-charge balance before any of them committed. Restoring the `FOR
UPDATE` clause and rerunning the identical test: exactly 10 successes,
40 rejected as `insufficient_funds`, balance never leaves zero. Same
test, same seed of goroutines, only the lock changed.

## Idempotency keys

`Idempotency-Key` header, required on `/charges` and `/refunds`. TTL is
**24 hours**. The key row is inserted *inside* the same
database transaction as the business write it guards — the `UNIQUE`
constraint on `idempotency_keys.key`, not application logic, is what
actually prevents two concurrent identical requests from both creating a
charge.

Three cases, handled explicitly rather than left to chance:

- **Same key, different body** → `422`. A SHA-256 hash of the raw request
  body is stored alongside the key; a mismatch is rejected before any
  business logic runs.
- **Two identical requests racing** → the loser's `INSERT` hits the
  unique violation. Rather than failing immediately, it polls briefly
  (25ms interval, 5s budget) for the winner's stored response and replays
  it, falling back to `409` only if the winner hasn't finished in time.
  Fired 100 identical concurrent `POST /charges` at the same key in
  `TestIdempotency_100ConcurrentIdenticalCharges_ExactlyOneTransaction`:
  exactly one transaction row, all 100 callers got back the identical
  `201` — not a mix of 201s and 409s.
- **Expiry** → enforced two ways: lazily (a claim attempt deletes its own
  key first if it's past `expires_at`, so the value is reusable
  immediately, without waiting on a sweep) and by a periodic `Reap` that
  runs hourly in `cmd/server` purely to keep the table from growing
  unbounded. Reap is never required for correctness — only for
  bookkeeping.

`response_body` is stored as `TEXT`, not `JSONB`. This wasn't the first
draft: JSONB seemed like the obvious type for "a JSON response body"
until the first replay test failed — `{"ok":true}` went in and
`{"ok": true}` came back out. Postgres's JSONB storage reformats its
input (canonical spacing) on the way back out, which quietly breaks
"replay returns the exact original bytes." `TEXT` doesn't reinterpret
what it's given.

### A real deadlock, found by testing this properly

Building `TestExecute_ConcurrentIdenticalRequestsRunHandlerOnce` (20
goroutines, same key, same body) surfaced a genuine bug, not a test
artifact: the first implementation, on losing the `INSERT` race, called
into a fallback lookup that acquired a *second* connection from the pool
while still holding the first (now-aborted) transaction open. Under
enough concurrent losers, every pool connection ends up parked waiting
for a connection nobody can free, because the thing holding each
connection needs another connection to finish and let go of the one it's
holding. `pg_stat_activity` showed it directly: eight sessions sitting in
`idle in transaction (aborted)` for two minutes straight. The fix is one
line — roll back the losing transaction *before* making the fallback
call, not after — but it's exactly the kind of bug that only shows up
under real concurrency, which is the whole argument for writing the
concurrent test instead of trusting the single-request case.

## Refunds

`POST /refunds` locks the *charge's* transaction row with `SELECT ... FOR
UPDATE`, then sums prior refunds against it, then checks the requested
amount (or "whatever remains," if 0) against what's left. Partial refunds
are supported; the running total is capped at the original charge amount.

### The refund race

Two concurrent full refunds of the same charge is the nasty case named
directly in the brief, and it's worth spelling out *why* it's nasty:
**the zero-sum invariant cannot catch a double refund.** Each refund,
taken alone, is perfectly balanced — its own entries sum to zero just
like any other transaction. Nothing about the ledger's core guarantee
notices that the *same charge* got reversed twice. Catching that requires
a business-level check (has this charge already been fully refunded?),
and that check is check-then-write, so it has exactly the same
lost-update shape as the balance check in "Concurrency control" — the
fix is the same shape too: lock the row the decision depends on (here,
the charge's transaction row) before reading it.

`TestRefund_ConcurrentFullRefunds_ExactlyOneSucceeds` fires 20 concurrent
full-refund attempts at one charge: exactly 1 succeeds, 19 are rejected
`already_fully_refunded`, and `SUM(amount)` across all refund
transactions for that charge equals the original charge amount exactly —
not double. The general lesson: an invariant that holds for every
individual transaction says nothing about invariants that span multiple
transactions. Those need their own explicit guard.

## Outbox over dual-write

You cannot atomically write to Postgres and `POST` to an external HTTP
endpoint — there's no shared transaction spanning a database and an HTTP
client. Writing directly to Postgres and then calling the webhook
endpoint (dual-write) means every crash between the two steps is either a
silently dropped notification or a duplicate, and you don't get to choose
which.

The outbox turns that gap into a single atomic operation on one side of
it: `outbox_events` gets a row in the *same* database transaction as the
charge or refund it describes. A separate `Worker` (its own binary,
`cmd/worker`) polls for `pending` rows with `SELECT ... FOR UPDATE SKIP
LOCKED` — the `SKIP LOCKED` is what lets multiple worker instances run
against the same table without ever double-delivering an event *at the
same time* (they just each work on whatever the others aren't currently
holding).

### At-least-once, and what it costs the consumer

If the `POST` succeeds but the process dies before the row is marked
`sent`, the event is still `pending` and gets redelivered on restart.
That's **at-least-once delivery**, and it's a structural consequence of
decoupling the write from the delivery — not a policy choice bolted on
top. The alternative, at-most-once (mark `sent` before or without
confirming delivery), silently drops notifications on any crash or
timeout in that window, and a merchant never learning they got paid is a
strictly worse failure than a merchant's system seeing one event twice.

The cost lands on the consumer: it *must* deduplicate on event ID
(`X-Webhook-Id` header) and tolerate out-of-order delivery. The example
consumer (`internal/webhookconsumer`) does exactly this — an in-memory
set of accepted event IDs, `duplicate: true` in its response once an ID
has been seen. This is the standard pattern for webhook consumers:
best practices advise consumers to deduplicate on event ID
for this reason.

Payloads are signed using `HMAC-SHA256` over
`"<unix-timestamp>.<body>"`, sent as `X-Webhook-Signature: t=<ts>,v1=<hex>`.
Binding the timestamp into the signed material (not just the body) is
what lets a receiver additionally reject a replayed-but-authentic
delivery whose timestamp is stale, which `internal/webhookconsumer` does
with a 5-minute tolerance.

Failed deliveries retry with exponential backoff and full jitter
(`base=500ms, cap=5min, delay = random(0, min(cap, base·2^attempts))`) —
jittered, not lockstep, so a burst of failures doesn't retry in
synchronized waves. After `WORKER_MAX_ATTEMPTS` (default 10), an event
moves to `dead_letter` and stops retrying; `last_error` records what
finally killed it. Verified in `TestOutbox_RetriesThenDeadLetters`: an
always-failing consumer, `MaxAttempts=3` — the event fails exactly 3
times and lands in `dead_letter` with `attempts=3`, never a phantom
delivery in between.

## The crash tests

**#1 — panic mid-transaction
(`internal/ledger/crash_test.go::TestCrash_PanicBetweenEntryInserts_RollsBack`).**
A test-only seam (`Service.crashAfterDebitEntry`, unexported, only ever
set from a white-box test in the same package) panics after the debit
entry is inserted and before the credit entry is. The test recovers the
panic at its own boundary and asserts row counts and both accounts'
`cached_balance` are byte-for-byte unchanged. This works because Go
defers run during a panic's unwind regardless of whether anything ever
recovers it — `Charge`'s `defer tx.Rollback(ctx)` fires before the panic
reaches the test, so by the time the test's own `recover()` catches it,
the rollback has already happened. A second, un-poisoned charge
immediately afterward proves the panic didn't leave the connection or
transaction machinery wedged.

**#2 — kill the worker mid-delivery (`scripts/crash_test_worker.sh`).**
Runs the real worker and webhook-consumer binaries as containers and
sends a genuine `SIGKILL` — no graceful shutdown, no cleanup hook. The
timing window ("after the POST succeeds, before the row is marked sent")
is widened deterministically with `WORKER_TEST_CRASH_DELAY_AFTER_SEND`, a
test-only flag documented in `internal/config` that makes the worker
sleep after a successful delivery and before its own `UPDATE ... SET
status = 'sent'`. That's a legitimate fault-injection technique, not a
shortcut around the test: the race it reproduces (crash between a
completed side-effect and persisting that it completed) is exactly the
same race that happens for real, just at a timing window too narrow to
land on reliably by chance. The script:

1. seeds one charge (through the real `ledger.Service`, not raw SQL),
2. waits for the consumer to actually receive it over HTTP,
3. confirms the DB row is still `pending` (mark-sent hasn't run),
4. `SIGKILL`s the worker container mid-sleep,
5. confirms the row is *still* `pending` after the kill — the crash
   landed exactly where intended,
6. restarts the worker fresh,
7. confirms the event is redelivered and the consumer's dedupe flags the
   second delivery `duplicate: true`.

Uses `docker` by default (`CONTAINER_RUNTIME=podman` to use podman
instead, which is what this environment actually has installed —
`kill -9` under either runtime is the same signal to the same kind of
process, so the substitution doesn't change what's being demonstrated).

## Running it

```sh
docker compose up -d postgres      # or: podman run -d --name ledger_postgres \
                                    #       -e POSTGRES_USER=ledger -e POSTGRES_PASSWORD=ledger -e POSTGRES_DB=ledger \
                                    #       -p 5433:5432 \
                                    #       -v $PWD/internal/dbutil/schema.sql:/docker-entrypoint-initdb.d/001_schema.sql:ro \
                                    #       postgres:16

go run ./cmd/server       # API on :8080
go run ./cmd/worker       # outbox delivery
go run ./cmd/webhookconsumer   # example receiver, for manual testing
```

### Tests

```sh
go test ./...             # everything, against TEST_DATABASE_URL
                           # (default: postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable)
go test ./... -race       # same, with the race detector
./scripts/crash_test_worker.sh   # crash test #2 (needs a container runtime)
```

Every test in `test/` and the white-box tests in `internal/ledger` and
`internal/idempotency` run against a real Postgres instance — no mocks,
no SQLite stand-in. That's a deliberate constraint stated in the brief:
this project's whole point is isolation levels and row locking, and
those don't exist in SQLite.

## Not handled

Named on purpose, not by oversight:

- **FX / multi-currency transactions.** A charge requires the source
  account, destination account, and request to all share one currency.
  Real multi-currency movement needs a rate table, a rate source, and a
  decision about when the rate is locked — a different project.
- **Settlement.** Nothing here models the delay between "a charge is
  recorded" and "money actually moved between banks." Every account here
  is assumed instantly and finally settled.
- **Fees.** A processor's or platform's cut isn't modeled. In a real
  system it would be its own entry pair per transaction, probably with
  its own dedicated fee-revenue account.
- **Auth.** There's no authentication or authorization anywhere in
  `httpapi`. Deliberately: it adds nothing to the story this project is
  telling about transaction semantics, and would just be a standard
  middleware layer bolted on top.
- **A frontend.** Same reasoning as auth.
- **Multi-tenancy / per-merchant scoping.** Every account and transaction
  lives in one flat namespace.
