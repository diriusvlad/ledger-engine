// Command server runs the ledger's HTTP API.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vlad/ledger-engine/internal/config"
	"github.com/vlad/ledger-engine/internal/dbutil"
	"github.com/vlad/ledger-engine/internal/httpapi"
	"github.com/vlad/ledger-engine/internal/idempotency"
)

func main() {
	cfg := config.LoadServer()

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

	// Idempotency keys are pure housekeeping past their TTL: sweep
	// expired rows periodically so the table doesn't grow unbounded. See
	// internal/idempotency for why this is not required for correctness.
	reaper := idempotency.New(pool)
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n, err := reaper.Reap(ctx); err != nil {
					slog.Error("idempotency reap", "err", err)
				} else if n > 0 {
					slog.Info("idempotency reap", "deleted", n)
				}
			}
		}
	}()

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           httpapi.NewServer(pool),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("server listening", "addr", cfg.ListenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
