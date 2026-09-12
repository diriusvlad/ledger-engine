// Package outbox implements the transactional outbox pattern: producers
// insert events into outbox_events inside the same DB transaction as the
// business write they describe, and a separate Worker (see worker.go)
// polls for unsent rows and delivers them over HTTP.
//
// This exists because there is no way to atomically write to Postgres and
// POST to an external HTTP endpoint in the same transaction. Writing to
// the outbox table *is* the atomic operation; delivery is decoupled and
// happens at-least-once. See README.md for the full argument.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Insert writes a new pending event as part of tx. Callers must be inside
// the same DB transaction that performs the business write the event
// describes (e.g. inserting a charge's entries) so that the event can
// never be recorded without the write actually having committed, or vice
// versa.
func Insert(ctx context.Context, tx pgx.Tx, aggregateType string, aggregateID uuid.UUID, eventType string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4)`,
		aggregateType, aggregateID, eventType, body)
	if err != nil {
		return fmt.Errorf("outbox: insert event: %w", err)
	}
	return nil
}
