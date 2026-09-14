// Package integration_test holds black-box integration tests that run
// against a real Postgres instance (see README "Running the tests").
package integration_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vlad/ledger-engine/internal/dbutil"
	"github.com/vlad/ledger-engine/internal/ledger"
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
	truncateAll(t, pool)
	t.Cleanup(func() { pool.Close() })
	return pool
}

func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		TRUNCATE entries, transactions, accounts, idempotency_keys, outbox_events
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// fundedAccount creates a fresh account and funds it with amount minor
// units by charging it from a fresh external/unbounded funding account —
// i.e. funding goes through the exact same Charge path as everything
// else, rather than poking cached_balance directly.
func fundedAccount(t *testing.T, ctx context.Context, svc *ledger.Service, currency string, amount int64) *ledger.Account {
	t.Helper()
	external, err := svc.CreateAccount(ctx, "external-funding", currency, true)
	if err != nil {
		t.Fatalf("create external account: %v", err)
	}
	acct, err := svc.CreateAccount(ctx, "test-account", currency, false)
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if amount > 0 {
		if _, err := svc.Charge(ctx, ledger.ChargeRequest{
			SourceAccountID:      external.ID,
			DestinationAccountID: acct.ID,
			Amount:               amount,
			Currency:             currency,
		}); err != nil {
			t.Fatalf("fund account: %v", err)
		}
	}
	return acct
}
