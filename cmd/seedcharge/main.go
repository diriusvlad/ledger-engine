// Command seedcharge performs exactly one charge (through the real
// ledger.Service code path, not raw SQL) and prints the resulting
// transaction ID and outbox event ID as JSON. It exists so
// scripts/crash_test_worker.sh can generate a single, deterministic
// outbox event without depending on the HTTP server being up.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/vlad/ledger-engine/internal/config"
	"github.com/vlad/ledger-engine/internal/dbutil"
	"github.com/vlad/ledger-engine/internal/ledger"
)

func main() {
	ctx := context.Background()
	cfg := config.LoadServer() // reuses DATABASE_URL handling

	pool, err := dbutil.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open database:", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := dbutil.ApplySchema(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "apply schema:", err)
		os.Exit(1)
	}

	svc := ledger.New(pool)
	external, err := svc.CreateAccount(ctx, "crash-test-external", "RON", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create external account:", err)
		os.Exit(1)
	}
	dest, err := svc.CreateAccount(ctx, "crash-test-merchant", "RON", false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create merchant account:", err)
		os.Exit(1)
	}

	txn, err := svc.Charge(ctx, ledger.ChargeRequest{
		SourceAccountID:      external.ID,
		DestinationAccountID: dest.ID,
		Amount:               999,
		Currency:             "RON",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "charge:", err)
		os.Exit(1)
	}

	var eventID string
	if err := pool.QueryRow(ctx, `SELECT id FROM outbox_events WHERE aggregate_id = $1`, txn.ID).Scan(&eventID); err != nil {
		fmt.Fprintln(os.Stderr, "look up outbox event:", err)
		os.Exit(1)
	}

	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{
		"transaction_id": txn.ID.String(),
		"event_id":       eventID,
	})
}
