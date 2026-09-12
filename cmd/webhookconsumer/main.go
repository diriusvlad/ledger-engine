// Command webhookconsumer is an example receiver used by the crash test
// and available for manual testing against the outbox worker. It is not
// part of the ledger service itself.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/vlad/ledger-engine/internal/config"
	"github.com/vlad/ledger-engine/internal/webhookconsumer"
)

func main() {
	cfg := config.LoadWebhookConsumer()
	consumer := webhookconsumer.New(cfg.Secret)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           consumer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("webhookconsumer listening", "addr", cfg.ListenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("webhookconsumer error", "err", err)
		os.Exit(1)
	}
}
