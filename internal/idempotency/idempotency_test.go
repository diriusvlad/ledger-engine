package idempotency

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vlad/ledger-engine/internal/dbutil"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := dbutil.Open(ctx, url)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	if err := dbutil.ApplySchema(ctx, pool); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM idempotency_keys`); err != nil {
		t.Fatalf("clean idempotency_keys: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func okHandler(calls *int64) Handler {
	return func(ctx context.Context, tx pgx.Tx) (int, []byte, *uuid.UUID, error) {
		atomic.AddInt64(calls, 1)
		return 201, []byte(`{"ok":true}`), nil, nil
	}
}

func TestExecute_SecondCallWithSameKeyAndBodyReplays(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	ctx := context.Background()

	var calls int64
	status1, body1, err1 := store.Execute(ctx, "key-1", []byte(`{"x":1}`), okHandler(&calls))
	if err1 != nil {
		t.Fatalf("first call: %v", err1)
	}
	status2, body2, err2 := store.Execute(ctx, "key-1", []byte(`{"x":1}`), okHandler(&calls))
	if err2 != nil {
		t.Fatalf("second call: %v", err2)
	}

	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}
	if status1 != status2 || string(body1) != string(body2) {
		t.Fatalf("replay mismatch: (%d,%s) vs (%d,%s)", status1, body1, status2, body2)
	}
	if status1 != 201 {
		t.Fatalf("unexpected status %d", status1)
	}
}

func TestExecute_SameKeyDifferentBodyReturnsMismatch(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	ctx := context.Background()

	var calls int64
	if _, _, err := store.Execute(ctx, "key-2", []byte(`{"x":1}`), okHandler(&calls)); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, _, err := store.Execute(ctx, "key-2", []byte(`{"x":2}`), okHandler(&calls))
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("expected ErrMismatch, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times for a rejected mismatch, want 1", calls)
	}
}

func TestExecute_ConcurrentIdenticalRequestsRunHandlerOnce(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	ctx := context.Background()

	var calls int64
	handler := func(ctx context.Context, tx pgx.Tx) (int, []byte, *uuid.UUID, error) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(50 * time.Millisecond) // widen the race window
		return 201, []byte(`{"ok":true}`), nil, nil
	}

	const n = 20
	var wg sync.WaitGroup
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i], bodies[i], errs[i] = store.Execute(ctx, "key-concurrent", []byte(`{"x":1}`), handler)
		}(i)
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", calls)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: unexpected error %v", i, errs[i])
		}
		if statuses[i] != 201 || string(bodies[i]) != `{"ok":true}` {
			t.Fatalf("goroutine %d: got (%d,%s), want the winner's stored response", i, statuses[i], bodies[i])
		}
	}
}

func TestReapDeletesExpiredKeys(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	store.ttl = 10 * time.Millisecond
	ctx := context.Background()

	var calls int64
	if _, _, err := store.Execute(ctx, "key-ttl", []byte(`{"x":1}`), okHandler(&calls)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	n, err := store.Reap(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reap deleted %d rows, want 1", n)
	}
}

func TestExpiredKeyIsReusableWithoutWaitingForReap(t *testing.T) {
	pool := testPool(t)
	store := New(pool)
	store.ttl = 10 * time.Millisecond
	ctx := context.Background()

	var calls int64
	if _, _, err := store.Execute(ctx, "key-reuse", []byte(`{"x":1}`), okHandler(&calls)); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// A different body under the same, now-expired key value must be
	// treated as a fresh request, not a 422 mismatch.
	status, _, err := store.Execute(ctx, "key-reuse", []byte(`{"x":2}`), okHandler(&calls))
	if err != nil {
		t.Fatalf("execute after expiry: %v", err)
	}
	if status != 201 {
		t.Fatalf("unexpected status %d", status)
	}
	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2 (once per generation of the key)", calls)
	}
}
