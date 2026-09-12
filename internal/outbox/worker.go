package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultBaseBackoff = 500 * time.Millisecond
	defaultMaxBackoff  = 5 * time.Minute
)

// Worker polls outbox_events for pending rows and delivers them over HTTP.
// At-least-once delivery is inherent to the design: if the POST succeeds
// but the process dies before the row is marked 'sent' (see
// TestCrashDelayAfterSend below), the same event is picked up again on
// restart and redelivered. Consumers are expected to deduplicate on the
// X-Webhook-Id header. See README "Outbox and at-least-once delivery".
type Worker struct {
	pool       *pgxpool.Pool
	httpClient *http.Client
	webhookURL string
	secret     string

	pollInterval time.Duration
	batchSize    int
	maxAttempts  int
	baseBackoff  time.Duration
	maxBackoff   time.Duration

	// TestCrashDelayAfterSend: see internal/config doc comment. Zero in
	// normal operation.
	TestCrashDelayAfterSend time.Duration
}

type WorkerConfig struct {
	WebhookURL              string
	Secret                  string
	PollInterval            time.Duration
	BatchSize               int
	MaxAttempts             int
	TestCrashDelayAfterSend time.Duration

	// BaseBackoff/MaxBackoff override the retry backoff schedule. Zero
	// means "use the production defaults" (500ms base, 5min cap); tests
	// that want to exercise the retry-then-dead-letter path without
	// real-time waiting set these to a few milliseconds instead.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func NewWorker(pool *pgxpool.Pool, cfg WorkerConfig) *Worker {
	base := cfg.BaseBackoff
	if base == 0 {
		base = defaultBaseBackoff
	}
	maxB := cfg.MaxBackoff
	if maxB == 0 {
		maxB = defaultMaxBackoff
	}
	return &Worker{
		pool:                    pool,
		httpClient:              &http.Client{Timeout: 10 * time.Second},
		webhookURL:              cfg.WebhookURL,
		secret:                  cfg.Secret,
		pollInterval:            cfg.PollInterval,
		batchSize:               cfg.BatchSize,
		maxAttempts:             cfg.MaxAttempts,
		baseBackoff:             base,
		maxBackoff:              maxB,
		TestCrashDelayAfterSend: cfg.TestCrashDelayAfterSend,
	}
}

// Run polls until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Tick(ctx)
		}
	}
}

// Tick drains up to batchSize due events. It is exported so tests can step
// the worker deterministically instead of racing a background goroutine.
func (w *Worker) Tick(ctx context.Context) {
	for i := 0; i < w.batchSize; i++ {
		processed, err := w.processOne(ctx)
		if err != nil {
			slog.Error("outbox worker: process event", "err", err)
			return
		}
		if !processed {
			return
		}
	}
}

type event struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       []byte
	Attempts      int
	CreatedAt     time.Time
}

type envelope struct {
	ID            uuid.UUID       `json:"id"`
	Type          string          `json:"type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	CreatedAt     time.Time       `json:"created_at"`
	Data          json.RawMessage `json:"data"`
}

// processOne claims (via SELECT ... FOR UPDATE SKIP LOCKED) and attempts
// delivery of a single due event. It returns processed=false only when
// there is currently no due work, so multiple worker processes can run
// concurrently without ever delivering the same event twice at the same
// time — SKIP LOCKED makes a locked-but-due row invisible to other
// workers instead of making them wait on it.
func (w *Worker) processOne(ctx context.Context) (processed bool, err error) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("outbox: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var ev event
	err = tx.QueryRow(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, attempts, created_at
		FROM outbox_events
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`,
	).Scan(&ev.ID, &ev.AggregateType, &ev.AggregateID, &ev.EventType, &ev.Payload, &ev.Attempts, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("outbox: claim event: %w", err)
	}

	body, err := json.Marshal(envelope{
		ID:            ev.ID,
		Type:          ev.EventType,
		AggregateType: ev.AggregateType,
		AggregateID:   ev.AggregateID,
		CreatedAt:     ev.CreatedAt,
		Data:          ev.Payload,
	})
	if err != nil {
		return false, fmt.Errorf("outbox: marshal envelope: %w", err)
	}

	if err := w.deliver(ctx, ev, body, tx); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("outbox: commit: %w", err)
	}
	return true, nil
}

func (w *Worker) deliver(ctx context.Context, ev event, body []byte, tx pgx.Tx) error {
	timestamp := time.Now().Unix()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("outbox: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Id", ev.ID.String())
	req.Header.Set("X-Webhook-Signature", SignatureHeader(w.secret, timestamp, body))

	resp, doErr := w.httpClient.Do(req)
	success := doErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
	var httpErr string
	if resp != nil {
		httpErr = fmt.Sprintf("http status %d", resp.StatusCode)
		_ = resp.Body.Close()
	}

	if success {
		// Deliberate, test-only fault injection window: see
		// TestCrashDelayAfterSend doc comment and
		// scripts/crash_test_worker.sh. A SIGKILL landing during this
		// sleep reproduces "POST succeeded, mark-sent never happened"
		// deterministically instead of relying on real network timing.
		if w.TestCrashDelayAfterSend > 0 {
			time.Sleep(w.TestCrashDelayAfterSend)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox_events SET status = 'sent', sent_at = now() WHERE id = $1`, ev.ID); err != nil {
			return fmt.Errorf("outbox: mark sent: %w", err)
		}
		return nil
	}

	errMsg := httpErr
	if doErr != nil {
		errMsg = doErr.Error()
	}
	return w.recordFailure(ctx, tx, ev, errMsg)
}

func (w *Worker) recordFailure(ctx context.Context, tx pgx.Tx, ev event, errMsg string) error {
	attempts := ev.Attempts + 1
	if attempts >= w.maxAttempts {
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events SET status = 'dead_letter', attempts = $1, last_error = $2
			WHERE id = $3`, attempts, errMsg, ev.ID)
		if err != nil {
			return fmt.Errorf("outbox: dead-letter: %w", err)
		}
		return nil
	}

	delay := backoffWithFullJitter(w.baseBackoff, w.maxBackoff, attempts)
	_, err := tx.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = $1, last_error = $2, next_attempt_at = now() + $3::interval
		WHERE id = $4`, attempts, errMsg, delay.String(), ev.ID)
	if err != nil {
		return fmt.Errorf("outbox: schedule retry: %w", err)
	}
	return nil
}

// backoffWithFullJitter implements the "full jitter" strategy: a uniform
// random delay between 0 and min(maxBackoff, baseBackoff*2^attempts). This
// spreads out retries instead of having every failed event retry in
// lockstep.
func backoffWithFullJitter(base, maxBackoff time.Duration, attempts int) time.Duration {
	ceiling := float64(maxBackoff)
	exp := float64(base) * math.Pow(2, float64(attempts))
	if exp > ceiling {
		exp = ceiling
	}
	return time.Duration(rand.Float64() * exp)
}
