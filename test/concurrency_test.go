package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vlad/ledger-engine/internal/ledger"
)

// TestConcurrency_ForUpdatePreventsOvercommit is the "first real decision"
// test from the project brief: fire 50 goroutines at the same account's
// balance-guarded charge path simultaneously. The account starts with
// exactly enough balance for 10 charges of 100 minor units; without the
// SELECT ... FOR UPDATE lock in ChargeTx, this is a textbook lost-update
// race (see README "Concurrency control" for what happens with the lock
// removed — spoiler: it overcommits). With the lock, exactly 10 must
// succeed, the other 40 must be rejected as insufficient funds, and the
// account must never be observed to go negative.
func TestConcurrency_ForUpdatePreventsOvercommit(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	source := fundedAccount(t, ctx, svc, "RON", 1000)
	merchant, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create merchant: %v", err)
	}

	const attempts = 50
	const chargeAmount = 100

	var (
		wg                sync.WaitGroup
		mu                sync.Mutex
		successes         int
		insufficientFunds int
		unexpected        []error
	)

	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			_, err := svc.Charge(ctx, ledger.ChargeRequest{
				SourceAccountID:      source.ID,
				DestinationAccountID: merchant.ID,
				Amount:               chargeAmount,
				Currency:             "RON",
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ledger.ErrAccountNotFound):
				unexpected = append(unexpected, err)
			default:
				var lerr *ledger.Error
				if errors.As(err, &lerr) && lerr.Code == "insufficient_funds" {
					insufficientFunds++
				} else {
					unexpected = append(unexpected, err)
				}
			}
		}()
	}
	wg.Wait()

	for _, e := range unexpected {
		t.Errorf("unexpected error: %v", e)
	}
	if successes != 10 {
		t.Errorf("successes = %d, want exactly 10 (1000/100)", successes)
	}
	if successes+insufficientFunds != attempts {
		t.Errorf("successes(%d) + insufficient_funds(%d) != attempts(%d)", successes, insufficientFunds, attempts)
	}

	cached, err := svc.CachedBalance(ctx, source.ID)
	if err != nil {
		t.Fatalf("cached balance: %v", err)
	}
	computed, err := svc.ComputedBalance(ctx, source.ID)
	if err != nil {
		t.Fatalf("computed balance: %v", err)
	}
	if cached != 0 {
		t.Errorf("source cached_balance = %d, want 0", cached)
	}
	if computed != 0 {
		t.Errorf("source computed balance (SUM(entries)) = %d, want 0", computed)
	}
	if cached != computed {
		t.Errorf("cached_balance (%d) != computed balance (%d)", cached, computed)
	}
	if cached < 0 {
		t.Errorf("source account went negative: %d", cached)
	}
}
