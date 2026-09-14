package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vlad/ledger-engine/internal/ledger"
)

func chargeFor(t *testing.T, ctx context.Context, svc *ledger.Service, amount int64) (*ledger.Transaction, *ledger.Account, *ledger.Account) {
	t.Helper()
	payer := fundedAccount(t, ctx, svc, "RON", amount*2)
	merchant, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create merchant: %v", err)
	}
	txn, err := svc.Charge(ctx, ledger.ChargeRequest{
		SourceAccountID: payer.ID, DestinationAccountID: merchant.ID,
		Amount: amount, Currency: "RON",
	})
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	return txn, payer, merchant
}

func TestRefund_PartialRefundsCapAtOriginalAmount(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	charge, _, _ := chargeFor(t, ctx, svc, 1000)

	if _, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 400}); err != nil {
		t.Fatalf("first partial refund: %v", err)
	}

	_, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 700})
	var lerr *ledger.Error
	if !errors.As(err, &lerr) || lerr.Code != "refund_exceeds_remaining" {
		t.Fatalf("refund of 700 with 600 remaining: got %v, want refund_exceeds_remaining", err)
	}

	if _, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 600}); err != nil {
		t.Fatalf("refund the remaining 600: %v", err)
	}

	_, err = svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 1})
	if !errors.As(err, &lerr) || lerr.Code != "already_fully_refunded" {
		t.Fatalf("refund after fully refunded: got %v, want already_fully_refunded", err)
	}

	var total int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount),0) FROM entries`).Scan(&total); err != nil {
		t.Fatalf("sum entries: %v", err)
	}
	if total != 0 {
		t.Fatalf("total entries = %d, want 0", total)
	}
}

func TestRefund_FullRefundThenFullRefundAgainRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	charge, _, _ := chargeFor(t, ctx, svc, 1000)

	if _, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 0}); err != nil {
		t.Fatalf("full refund: %v", err)
	}
	_, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 0})
	var lerr *ledger.Error
	if !errors.As(err, &lerr) || lerr.Code != "already_fully_refunded" {
		t.Fatalf("second full refund: got %v, want already_fully_refunded", err)
	}
	_ = pool
}

// TestRefund_ConcurrentFullRefunds_ExactlyOneSucceeds is the nasty race
// from the project brief: two (here, twenty, for a stronger signal)
// concurrent full refunds of the same charge. The zero-sum invariant
// alone cannot catch a double refund — each individual refund is
// perfectly balanced — so this test checks the thing that actually
// matters: the charge's transaction row lock (SELECT ... FOR UPDATE in
// RefundTx) must serialize the attempts so exactly one succeeds and the
// total refunded never exceeds the original charge.
func TestRefund_ConcurrentFullRefunds_ExactlyOneSucceeds(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	charge, _, _ := chargeFor(t, ctx, svc, 1000)

	const attempts = 20
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		successes  int
		rejected   int
		unexpected []error
	)

	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			_, err := svc.Refund(ctx, ledger.RefundRequest{ChargeTransactionID: charge.ID, Amount: 0})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			default:
				var lerr *ledger.Error
				if errors.As(err, &lerr) && lerr.Code == "already_fully_refunded" {
					rejected++
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
	if successes != 1 {
		t.Errorf("successes = %d, want exactly 1", successes)
	}
	if successes+rejected != attempts {
		t.Errorf("successes(%d) + rejected(%d) != attempts(%d)", successes, rejected, attempts)
	}

	var totalRefunded int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0) FROM transactions
		WHERE parent_transaction_id = $1 AND type = 'refund'`, charge.ID,
	).Scan(&totalRefunded); err != nil {
		t.Fatalf("sum refunds: %v", err)
	}
	if totalRefunded != charge.Amount {
		t.Errorf("total refunded = %d, want exactly the charge amount %d (not double)", totalRefunded, charge.Amount)
	}
}
