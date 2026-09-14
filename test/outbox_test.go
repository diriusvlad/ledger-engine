package integration_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vlad/ledger-engine/internal/ledger"
	"github.com/vlad/ledger-engine/internal/outbox"
	"github.com/vlad/ledger-engine/internal/webhookconsumer"
)

// waitFor polls cond every 10ms for up to timeout. It exists because
// outbox delivery is genuinely asynchronous (a separate worker polling
// the database) — this is ordinary async-test synchronization, not a
// retry loop papering over a flaky assertion.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

const testWebhookSecret = "test-webhook-secret"

// outboxEventID looks up the outbox_events row produced for a given
// transaction. Every fundedAccount call also produces its own funding
// charge (and therefore its own unrelated outbox event) which the same
// background worker will happily pick up, so tests must key off the
// specific event they care about rather than assuming it's the only one
// in flight.
func outboxEventID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, txnID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM outbox_events WHERE aggregate_id = $1`, txnID).Scan(&id); err != nil {
		t.Fatalf("look up outbox event for %s: %v", txnID, err)
	}
	return id
}

func chargeForOutboxTest(t *testing.T, ctx context.Context, svc *ledger.Service) *ledger.Transaction {
	t.Helper()
	payer := fundedAccount(t, ctx, svc, "RON", 10_000)
	merchant, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create merchant: %v", err)
	}
	txn, err := svc.Charge(ctx, ledger.ChargeRequest{
		SourceAccountID: payer.ID, DestinationAccountID: merchant.ID,
		Amount: 750, Currency: "RON",
	})
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	return txn
}

func TestOutbox_ChargeIsDeliveredWithValidSignatureAndMarkedSent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	consumer := webhookconsumer.New(testWebhookSecret)
	srv := httptest.NewServer(consumer.Handler())
	defer srv.Close()

	// Fund and charge before the worker starts, so the eventID lookup
	// below is unambiguous and we're not racing the worker's very first
	// poll against account setup.
	txn := chargeForOutboxTest(t, ctx, svc)
	eventID := outboxEventID(t, ctx, pool, txn.ID)

	w := outbox.NewWorker(pool, outbox.WorkerConfig{
		WebhookURL:   srv.URL + "/webhook",
		Secret:       testWebhookSecret,
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  5,
	})
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.Run(workerCtx)

	waitFor(t, 2*time.Second, func() bool { return consumer.CountByID(eventID) >= 1 })

	var found *webhookconsumer.ReceivedEvent
	for _, e := range consumer.Received() {
		if e.ID == eventID {
			e := e
			found = &e
		}
	}
	if found == nil {
		t.Fatalf("event %s never appeared in consumer.Received()", eventID)
	}
	if found.Type != "charge.succeeded" {
		t.Fatalf("event type = %q, want charge.succeeded", found.Type)
	}
	if found.Duplicate {
		t.Fatalf("first delivery flagged as duplicate")
	}

	waitFor(t, 2*time.Second, func() bool {
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM outbox_events WHERE id = $1`, eventID).Scan(&status); err != nil {
			t.Fatalf("query outbox status: %v", err)
		}
		return status == "sent"
	})
}

// TestOutbox_RetriesThenDeadLetters exercises the failure path: a
// consumer that always fails should push the event through increasing
// backoff and eventually land in dead_letter once MaxAttempts is
// exhausted, never delivering "successfully" in between. BaseBackoff is
// shrunk to milliseconds so the test doesn't have to burn real wall-clock
// seconds waiting out the production retry schedule.
func TestOutbox_RetriesThenDeadLetters(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	consumer := webhookconsumer.New(testWebhookSecret)
	consumer.FailUntilAttempt = 1000 // always fail
	srv := httptest.NewServer(consumer.Handler())
	defer srv.Close()

	txn := chargeForOutboxTest(t, ctx, svc)
	eventID := outboxEventID(t, ctx, pool, txn.ID)

	w := outbox.NewWorker(pool, outbox.WorkerConfig{
		WebhookURL:   srv.URL + "/webhook",
		Secret:       testWebhookSecret,
		PollInterval: 5 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
		BaseBackoff:  5 * time.Millisecond,
		MaxBackoff:   50 * time.Millisecond,
	})
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go w.Run(workerCtx)

	var status string
	var attempts int
	waitFor(t, 2*time.Second, func() bool {
		if err := pool.QueryRow(ctx, `SELECT status, attempts FROM outbox_events WHERE id = $1`, eventID).Scan(&status, &attempts); err != nil {
			t.Fatalf("query outbox status: %v", err)
		}
		return status == "dead_letter"
	})
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (MaxAttempts)", attempts)
	}
	// CountByID counts every delivery attempt including injected
	// failures, so check the accepted-events list instead: it must
	// never contain this event, since every attempt was rejected.
	for _, e := range consumer.Received() {
		if e.ID == eventID {
			t.Fatalf("an always-failing consumer should never have accepted this delivery")
		}
	}
}

// TestOutbox_RedeliveryIsDeduplicatedByConsumer simulates the exact
// scenario crash test #2 (scripts/crash_test_worker.sh) exercises against
// a real killed process: the same event ID delivered twice. It proves the
// consumer side of the at-least-once contract — dedup on X-Webhook-Id —
// independent of process-kill timing.
func TestOutbox_RedeliveryIsDeduplicatedByConsumer(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	consumer := webhookconsumer.New(testWebhookSecret)
	srv := httptest.NewServer(consumer.Handler())
	defer srv.Close()

	txn := chargeForOutboxTest(t, ctx, svc)
	eventID := outboxEventID(t, ctx, pool, txn.ID)

	w := outbox.NewWorker(pool, outbox.WorkerConfig{
		WebhookURL:   srv.URL + "/webhook",
		Secret:       testWebhookSecret,
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  5,
	})

	// Drive delivery with explicit Tick calls (no background Run loop)
	// so this test controls exactly when each delivery attempt happens.
	for i := 0; i < 5 && consumer.CountByID(eventID) == 0; i++ {
		w.Tick(ctx)
		time.Sleep(5 * time.Millisecond)
	}
	if consumer.CountByID(eventID) != 1 {
		t.Fatalf("first delivery count = %d, want 1", consumer.CountByID(eventID))
	}

	// Force redelivery of the same, already-sent event, as if a crashed
	// worker had never persisted the 'sent' status after a successful
	// POST (see scripts/crash_test_worker.sh for the real-crash version
	// of this scenario).
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET status = 'pending', next_attempt_at = now() WHERE id = $1`, eventID); err != nil {
		t.Fatalf("force redelivery: %v", err)
	}
	for i := 0; i < 5 && consumer.CountByID(eventID) < 2; i++ {
		w.Tick(ctx)
		time.Sleep(5 * time.Millisecond)
	}
	if consumer.CountByID(eventID) != 2 {
		t.Fatalf("redelivery count = %d, want 2", consumer.CountByID(eventID))
	}

	var dup bool
	for _, e := range consumer.Received() {
		if e.ID == eventID && e.Duplicate {
			dup = true
		}
	}
	if !dup {
		t.Fatalf("redelivered event was never flagged as a duplicate")
	}
}
