// Package config loads process configuration from the environment.
package config

import (
	"os"
	"strconv"
	"time"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// Server holds configuration for cmd/server.
type Server struct {
	DatabaseURL string
	ListenAddr  string
}

func LoadServer() Server {
	return Server{
		DatabaseURL: getenv("DATABASE_URL", "postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable"),
		ListenAddr:  getenv("LISTEN_ADDR", ":8080"),
	}
}

// Worker holds configuration for cmd/worker.
type Worker struct {
	DatabaseURL string
	WebhookURL  string
	Secret      string

	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int

	// TestCrashDelayAfterSend, when non-zero, makes the worker sleep for
	// this long after receiving a successful HTTP response from the
	// webhook consumer but before marking the outbox event 'sent'. It
	// exists solely to make the "kill worker mid-delivery" crash test
	// (see scripts/crash_test_worker.sh) reliably hit that exact window
	// instead of depending on real network timing. It is never set in
	// the docker-compose default configuration.
	TestCrashDelayAfterSend time.Duration
}

func LoadWorker() Worker {
	return Worker{
		DatabaseURL:             getenv("DATABASE_URL", "postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable"),
		WebhookURL:              getenv("WEBHOOK_URL", "http://localhost:9090/webhook"),
		Secret:                  getenv("WEBHOOK_SECRET", "dev-webhook-secret"),
		PollInterval:            getenvDuration("WORKER_POLL_INTERVAL", 200*time.Millisecond),
		BatchSize:               getenvInt("WORKER_BATCH_SIZE", 20),
		MaxAttempts:             getenvInt("WORKER_MAX_ATTEMPTS", 10),
		TestCrashDelayAfterSend: getenvDuration("WORKER_TEST_CRASH_DELAY_AFTER_SEND", 0),
	}
}

// WebhookConsumer holds configuration for cmd/webhookconsumer.
type WebhookConsumer struct {
	ListenAddr string
	Secret     string
}

func LoadWebhookConsumer() WebhookConsumer {
	return WebhookConsumer{
		ListenAddr: getenv("LISTEN_ADDR", ":9090"),
		Secret:     getenv("WEBHOOK_SECRET", "dev-webhook-secret"),
	}
}
