// Command worker polls outbox_events and delivers them as signed
// webhooks. It is a separate binary from the API server so the crash
// tests can kill it in isolation (see scripts/crash_test_worker.sh).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vlad/ledger-engine/internal/config"
	"github.com/vlad/ledger-engine/internal/dbutil"
	"github.com/vlad/ledger-engine/internal/outbox"
)

func main() {
	cfg := config.LoadWorker()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := dbutil.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("open database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := dbutil.ApplySchema(ctx, pool); err != nil {
		slog.Error("apply schema", "err", err)
		os.Exit(1)
	}

	w := outbox.NewWorker(pool, outbox.WorkerConfig{
		WebhookURL:              cfg.WebhookURL,
		Secret:                  cfg.Secret,
		PollInterval:            cfg.PollInterval,
		BatchSize:               cfg.BatchSize,
		MaxAttempts:             cfg.MaxAttempts,
		TestCrashDelayAfterSend: cfg.TestCrashDelayAfterSend,
	})

	slog.Info("worker started", "webhook_url", cfg.WebhookURL, "poll_interval", cfg.PollInterval)
	w.Run(ctx)
	slog.Info("worker stopped")
}
