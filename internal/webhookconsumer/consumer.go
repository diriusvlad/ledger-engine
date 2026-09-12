// Package webhookconsumer is a minimal example of what a well-behaved
// webhook consumer must do given an at-least-once sender: verify the
// signature, then deduplicate on event ID before acting on a delivery.
// It backs both cmd/webhookconsumer (used by the crash test script) and
// the in-process consumer used by outbox tests.
package webhookconsumer

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/vlad/ledger-engine/internal/outbox"
)

// MaxClockSkew bounds how old a signature's timestamp may be before it is
// rejected as a (possibly replayed) stale delivery.
const MaxClockSkew = 5 * time.Minute

type ReceivedEvent struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	Data       json.RawMessage `json:"data"`
	ReceivedAt time.Time       `json:"received_at"`
	Duplicate  bool            `json:"duplicate"`
}

// Consumer records deliveries and deduplicates by event ID. It is safe
// for concurrent use.
type Consumer struct {
	secret string

	mu       sync.Mutex
	attempts map[uuid.UUID]int  // every delivery attempt, success or fail
	accepted map[uuid.UUID]bool // whether we've ever successfully recorded this event
	received []ReceivedEvent

	// FailUntilAttempt, if > 0, makes the consumer return 500 for the
	// first FailUntilAttempt-1 deliveries of any event, so tests can
	// exercise the worker's retry/backoff path deterministically.
	FailUntilAttempt int
}

func New(secret string) *Consumer {
	return &Consumer{
		secret:   secret,
		attempts: make(map[uuid.UUID]int),
		accepted: make(map[uuid.UUID]bool),
	}
}

func (c *Consumer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", c.handleWebhook)
	mux.HandleFunc("/_received", c.handleReceived)
	mux.HandleFunc("/_reset", c.handleReset)
	return mux
}

type inboundEnvelope struct {
	ID   uuid.UUID       `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (c *Consumer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	sigHeader := r.Header.Get("X-Webhook-Signature")
	timestamp, ok := outbox.VerifySignature(sigHeader, c.secret, body)
	if !ok {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	if age := time.Since(time.Unix(timestamp, 0)); age > MaxClockSkew || age < -MaxClockSkew {
		http.Error(w, "stale signature timestamp", http.StatusUnauthorized)
		return
	}

	var env inboundEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	c.attempts[env.ID]++
	attempt := c.attempts[env.ID]
	duplicate := c.accepted[env.ID]
	shouldFail := c.FailUntilAttempt > 0 && attempt < c.FailUntilAttempt
	if !shouldFail {
		c.accepted[env.ID] = true
		c.received = append(c.received, ReceivedEvent{
			ID: env.ID, Type: env.Type, Data: env.Data,
			ReceivedAt: time.Now(), Duplicate: duplicate,
		})
	}
	c.mu.Unlock()

	if shouldFail {
		slog.Info("webhookconsumer: injected failure", "event_id", env.ID, "attempt", attempt)
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}

	slog.Info("webhookconsumer: received", "event_id", env.ID, "type", env.Type, "duplicate", duplicate)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"received": true, "duplicate": duplicate})
}

func (c *Consumer) handleReceived(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	out := make([]ReceivedEvent, len(c.received))
	copy(out, c.received)
	c.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (c *Consumer) handleReset(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.attempts = make(map[uuid.UUID]int)
	c.accepted = make(map[uuid.UUID]bool)
	c.received = nil
	c.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// Received returns a snapshot of everything accepted so far (deduplicated
// entries excluded from re-recording, but each still flagged Duplicate).
func (c *Consumer) Received() []ReceivedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ReceivedEvent, len(c.received))
	copy(out, c.received)
	return out
}

// CountByID returns how many times an event ID has been attempted
// (including deliveries that were injected-failed).
func (c *Consumer) CountByID(id uuid.UUID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts[id]
}
