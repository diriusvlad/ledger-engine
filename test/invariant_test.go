package integration_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/google/uuid"

	"github.com/vlad/ledger-engine/internal/ledger"
)

// invariantSeed is fixed so a failure here always reproduces exactly.
// Change it deliberately (and note why) if you ever need a different
// sequence; never leave it unseeded.
const invariantSeed = 1337

type recordedCharge struct {
	id     uuid.UUID
	amount int64
}

// TestInvariant_RandomizedChargesAndRefunds is the centerpiece test: fire
// a long, seeded, random sequence of charges and refunds and check the
// two invariants the whole schema exists to guarantee — the total of
// every entry ever written is zero, and every individual transaction's
// entries sum to zero. The database's own deferred constraint trigger
// already enforces the second invariant at every commit; this test
// re-checks it independently after the fact so a regression shows up
// here even if someone weakens or removes the trigger.
func TestInvariant_RandomizedChargesAndRefunds(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)
	r := rand.New(rand.NewSource(invariantSeed))

	const numAccounts = 6
	const iterations = 500
	const startingBalance = 1_000_000

	accounts := make([]*ledger.Account, numAccounts)
	for i := range accounts {
		accounts[i] = fundedAccount(t, ctx, svc, "RON", startingBalance)
	}

	var charges []recordedCharge
	var chargesOK, chargesRejected, refundsOK, refundsRejected int

	for i := 0; i < iterations; i++ {
		if len(charges) == 0 || r.Intn(10) < 7 {
			// charge
			src := accounts[r.Intn(numAccounts)]
			dst := accounts[r.Intn(numAccounts)]
			for dst.ID == src.ID {
				dst = accounts[r.Intn(numAccounts)]
			}
			amount := int64(r.Intn(5000) + 1)

			txn, err := svc.Charge(ctx, ledger.ChargeRequest{
				SourceAccountID:      src.ID,
				DestinationAccountID: dst.ID,
				Amount:               amount,
				Currency:             "RON",
			})
			if err != nil {
				var lerr *ledger.Error
				if !errors.As(err, &lerr) {
					t.Fatalf("unexpected charge error: %v", err)
				}
				chargesRejected++
				continue
			}
			chargesOK++
			charges = append(charges, recordedCharge{id: txn.ID, amount: amount})
		} else {
			// refund a random prior charge, full or partial
			c := charges[r.Intn(len(charges))]
			var amount int64
			if r.Intn(2) == 0 {
				amount = 0 // full remaining
			} else {
				amount = int64(r.Intn(int(c.amount))) + 1
			}

			_, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: c.id, Amount: amount})
			if err != nil {
				var lerr *ledger.Error
				if !errors.As(err, &lerr) {
					t.Fatalf("unexpected refund error: %v", err)
				}
				refundsRejected++
				continue
			}
			refundsOK++
		}
	}

	t.Logf("charges: %d ok, %d rejected; refunds: %d ok, %d rejected",
		chargesOK, chargesRejected, refundsOK, refundsRejected)
	if chargesOK == 0 || refundsOK == 0 {
		t.Fatalf("test didn't exercise both charges and refunds (charges=%d refunds=%d); widen the random ranges", chargesOK, refundsOK)
	}

	var total int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0) FROM entries`).Scan(&total); err != nil {
		t.Fatalf("sum all entries: %v", err)
	}
	if total != 0 {
		t.Fatalf("total sum of all entries = %d, want 0", total)
	}

	rows, err := pool.Query(ctx, `
		SELECT transaction_id, SUM(amount) FROM entries
		GROUP BY transaction_id HAVING SUM(amount) <> 0`)
	if err != nil {
		t.Fatalf("per-transaction sums: %v", err)
	}
	defer rows.Close()
	var unbalanced int
	for rows.Next() {
		var txnID string
		var sum int64
		if err := rows.Scan(&txnID, &sum); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Errorf("transaction %s entries sum to %d, want 0", txnID, sum)
		unbalanced++
	}
	if unbalanced > 0 {
		t.Fatalf("%d transactions were not individually balanced", unbalanced)
	}
}
