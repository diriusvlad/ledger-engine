// Package dbutil handles connection pool creation and schema bootstrap.
package dbutil

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Open creates a pgx connection pool for databaseURL.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("dbutil: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("dbutil: ping: %w", err)
	}
	return pool, nil
}

// ApplySchema runs the embedded schema against pool. It is idempotent, so
// it is safe to call at the start of every test run or server boot against
// a database that may already be migrated.
func ApplySchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("dbutil: apply schema: %w", err)
	}
	return nil
}
