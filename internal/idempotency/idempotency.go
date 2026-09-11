// Package idempotency implements Idempotency-Key handling on
// top of a Postgres UNIQUE constraint.
//
// The key row is inserted inside the SAME database transaction as the
// business write it guards (see Execute), so the UNIQUE constraint on
// idempotency_keys.key is what actually prevents two concurrent identical
// requests from both creating a charge — not application-level locking.
// Three cases are handled explicitly:
//
//   - Same key, different request body: the stored request_hash won't
//     match, and the request is rejected with 422 before anything is run.
//   - Two identical requests racing: the loser's INSERT hits the unique
//     violation, and instead of erroring out immediately it polls briefly
//     for the winner's response and replays it (falling back to 409 if
//     the winner hasn't finished within the poll budget).
//   - Keys expire after TTL (24h). Expiry is enforced
//     both lazily (a claim attempt deletes its own key first if it has
//     expired, so the value becomes reusable immediately) and by a
//     periodic Reap sweep that keeps the table from growing unbounded.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultTTL          = 24 * time.Hour
	defaultPollInterval = 25 * time.Millisecond
	defaultPollTimeout  = 5 * time.Second

	uniqueViolation = "23505"
)

// Handler performs the business work guarded by an idempotency key. It
// runs inside tx, which the caller must not commit or roll back itself.
// A non-nil err is treated as transient/unexpected (e.g. a DB error): the
// whole transaction, including the key claim, is rolled back so the
// client can retry the same key later. A well-formed business outcome —
// success or a deliberate rejection like insufficient funds — must be
// encoded as (statusCode, body), not as err, since that is the response
// future replays of this key should return.
type Handler func(ctx context.Context, tx pgx.Tx) (statusCode int, body []byte, transactionID *uuid.UUID, err error)

type Store struct {
	pool         *pgxpool.Pool
	ttl          time.Duration
	pollInterval time.Duration
	pollTimeout  time.Duration
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:         pool,
		ttl:          DefaultTTL,
		pollInterval: defaultPollInterval,
		pollTimeout:  defaultPollTimeout,
	}
}

// ErrMismatch is returned when the same key was reused with a different
// request body.
var ErrMismatch = errors.New("idempotency: key reused with a different request body")

// ErrConflict is returned when a concurrent request holds the key and did
// not finish within the poll budget.
var ErrConflict = errors.New("idempotency: request with this key is already in flight")

// HashBody computes the request hash stored alongside a key.
func HashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

type storedResponse struct {
	statusCode int
	body       []byte
}

// Execute runs handler exactly once for a given (key, requestBody) pair.
// Concurrent or later calls with the same key and body get back the
// original response instead of a fresh execution.
func (s *Store) Execute(ctx context.Context, key string, requestBody []byte, handler Handler) (int, []byte, error) {
	hash := HashBody(requestBody)

	if resp, err := s.lookup(ctx, key, hash); err != nil {
		return 0, nil, err
	} else if resp != nil {
		return resp.statusCode, resp.body, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("idempotency: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lazily reap this specific key if it's merely expired, so the same
	// key value can be reused right away without waiting on the
	// background Reap sweep.
	if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE key = $1 AND expires_at <= now()`, key); err != nil {
		return 0, nil, fmt.Errorf("idempotency: reap expired key: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, expires_at)
		VALUES ($1, $2, now() + $3::interval)`,
		key, hash, s.ttl.String())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			// Lost the claim race. Release this connection *before*
			// making another pool round-trip: the fallback lookup below
			// needs a connection of its own, and leaving this one
			// checked out (in an aborted-transaction state, no less)
			// while we wait for another is how you deadlock a bounded
			// pool under enough concurrent losers. The final deferred
			// tx.Rollback becomes a harmless no-op after this.
			_ = tx.Rollback(ctx)
			return s.awaitOrConflict(ctx, key, hash)
		}
		return 0, nil, fmt.Errorf("idempotency: claim key: %w", err)
	}

	statusCode, body, txnID, herr := handler(ctx, tx)
	if herr != nil {
		return 0, nil, herr
	}

	if _, err := tx.Exec(ctx, `
		UPDATE idempotency_keys SET status_code = $1, response_body = $2, transaction_id = $3
		WHERE key = $4`,
		statusCode, string(body), txnID, key,
	); err != nil {
		return 0, nil, fmt.Errorf("idempotency: store response: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, nil, fmt.Errorf("idempotency: commit: %w", err)
	}
	return statusCode, body, nil
}

// lookup returns a non-nil *storedResponse if key already has a completed
// response matching hash. If key exists with a different hash, it returns
// ErrMismatch. If key exists but is still in flight, it polls briefly
// before giving up with ErrConflict. If key does not exist (or is
// expired), it returns (nil, nil) so the caller proceeds to claim it.
func (s *Store) lookup(ctx context.Context, key, hash string) (*storedResponse, error) {
	var (
		storedHash string
		statusCode *int
		body       []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT request_hash, status_code, response_body
		FROM idempotency_keys
		WHERE key = $1 AND expires_at > now()`, key,
	).Scan(&storedHash, &statusCode, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency: lookup: %w", err)
	}
	if storedHash != hash {
		return nil, ErrMismatch
	}
	if statusCode != nil {
		return &storedResponse{statusCode: *statusCode, body: body}, nil
	}
	return s.poll(ctx, key, hash)
}

// awaitOrConflict is lookup's continuation after losing an INSERT race:
// the row is now guaranteed to exist (the winner just created it), so we
// re-read it rather than assume anything about its current state.
func (s *Store) awaitOrConflict(ctx context.Context, key, hash string) (int, []byte, error) {
	resp, err := s.lookup(ctx, key, hash)
	if err != nil {
		return 0, nil, err
	}
	if resp == nil {
		// Vanishingly unlikely: the winner's key already expired and was
		// reaped between our failed INSERT and this lookup. Treat like
		// any other conflict; the client's retry will succeed cleanly.
		return 0, nil, ErrConflict
	}
	return resp.statusCode, resp.body, nil
}

// poll waits for an in-flight request holding key to finish, up to
// pollTimeout, checking every pollInterval.
func (s *Store) poll(ctx context.Context, key, hash string) (*storedResponse, error) {
	deadline := time.Now().Add(s.pollTimeout)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}

		var (
			storedHash string
			statusCode *int
			body       []byte
		)
		err := s.pool.QueryRow(ctx, `
			SELECT request_hash, status_code, response_body
			FROM idempotency_keys WHERE key = $1`, key,
		).Scan(&storedHash, &statusCode, &body)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // claimant rolled back entirely; let caller reclaim
		}
		if err != nil {
			return nil, fmt.Errorf("idempotency: poll: %w", err)
		}
		if storedHash != hash {
			return nil, ErrMismatch
		}
		if statusCode != nil {
			return &storedResponse{statusCode: *statusCode, body: body}, nil
		}
	}
	return nil, ErrConflict
}

// Reap deletes expired idempotency keys. Call it periodically (see
// cmd/server) to keep the table bounded; individual expired keys are also
// deleted lazily on next use by Execute, so Reap is pure housekeeping and
// never required for correctness.
func (s *Store) Reap(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at <= now()`)
	if err != nil {
		return 0, fmt.Errorf("idempotency: reap: %w", err)
	}
	return tag.RowsAffected(), nil
}
