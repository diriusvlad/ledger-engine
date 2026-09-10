package ledger

import (
	"context"
	"os"
	"testing"

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
	t.Cleanup(pool.Close)
	return pool
}

// TestCrash_PanicBetweenEntryInserts_RollsBack is crash test #1 from the
// project brief: inject a panic between the two entry inserts of a
// charge and confirm the whole database transaction rolled back, leaving
// no half-written state (no orphan transaction row, no lone entry, no
// stale cached_balance).
func TestCrash_PanicBetweenEntryInserts_RollsBack(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	svc := New(pool)
	external, err := svc.CreateAccount(ctx, "external-funding", "RON", true)
	if err != nil {
		t.Fatalf("create external account: %v", err)
	}
	dest, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create dest account: %v", err)
	}

	before := snapshotCounts(t, pool)
	srcBalBefore, err := svc.CachedBalance(ctx, external.ID)
	if err != nil {
		t.Fatalf("balance before: %v", err)
	}
	dstBalBefore, err := svc.CachedBalance(ctx, dest.ID)
	if err != nil {
		t.Fatalf("balance before: %v", err)
	}

	svc.crashAfterDebitEntry = true

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected Charge to panic, but it returned normally")
			}
		}()
		_, _ = svc.Charge(ctx, ChargeRequest{
			SourceAccountID:      external.ID,
			DestinationAccountID: dest.ID,
			Amount:               500,
			Currency:             "RON",
		})
		t.Fatal("unreachable")
	}()

	after := snapshotCounts(t, pool)
	if before != after {
		t.Fatalf("row counts changed after panicking charge: before=%+v after=%+v; the transaction did not roll back cleanly", before, after)
	}

	srcBalAfter, err := svc.CachedBalance(ctx, external.ID)
	if err != nil {
		t.Fatalf("balance after: %v", err)
	}
	dstBalAfter, err := svc.CachedBalance(ctx, dest.ID)
	if err != nil {
		t.Fatalf("balance after: %v", err)
	}
	if srcBalBefore != srcBalAfter || dstBalBefore != dstBalAfter {
		t.Fatalf("cached_balance changed despite rollback: source %d->%d dest %d->%d",
			srcBalBefore, srcBalAfter, dstBalBefore, dstBalAfter)
	}

	// A second, un-poisoned charge must still work: the panic must not
	// have left the pool connection or transaction machinery wedged.
	svc.crashAfterDebitEntry = false
	txn, err := svc.Charge(ctx, ChargeRequest{
		SourceAccountID:      external.ID,
		DestinationAccountID: dest.ID,
		Amount:               500,
		Currency:             "RON",
	})
	if err != nil {
		t.Fatalf("charge after recovering from panic: %v", err)
	}
	if txn.Amount != 500 {
		t.Fatalf("unexpected amount: %d", txn.Amount)
	}
}

type rowCounts struct {
	Accounts     int
	Transactions int
	Entries      int
}

func snapshotCounts(t *testing.T, pool *pgxpool.Pool) rowCounts {
	t.Helper()
	ctx := context.Background()
	var c rowCounts
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&c.Accounts); err != nil {
		t.Fatalf("count accounts: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&c.Transactions); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entries`).Scan(&c.Entries); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	return c
}
