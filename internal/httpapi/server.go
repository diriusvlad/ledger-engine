// Package httpapi wires the ledger and idempotency packages to HTTP
// handlers. It intentionally has no authentication: see README "Not
// handled" for why that's out of scope for this project.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vlad/ledger-engine/internal/idempotency"
	"github.com/vlad/ledger-engine/internal/ledger"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type Server struct {
	ledger *ledger.Service
	idem   *idempotency.Store
	router chi.Router
}

func NewServer(pool *pgxpool.Pool) *Server {
	s := &Server{
		ledger: ledger.New(pool),
		idem:   idempotency.New(pool),
	}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.router.ServeHTTP(w, r) }

func (s *Server) routes() {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	r.Post("/accounts", s.handleCreateAccount)
	r.Get("/accounts/{id}", s.handleGetBalance)
	r.Post("/charges", s.handleCharge)
	r.Post("/refunds", s.handleRefund)

	s.router = r
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes)).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.Name == "" || len(req.Currency) != 3 {
		respondError(w, http.StatusBadRequest, "invalid_request", "name and a 3-letter currency are required")
		return
	}
	acct, err := s.ledger.CreateAccount(r.Context(), req.Name, req.Currency, req.AllowNegative)
	if err != nil {
		slog.Error("create account", "err", err)
		respondError(w, http.StatusInternalServerError, "internal_error", "failed to create account")
		return
	}
	respond(w, http.StatusCreated, encodeAccount(acct))
}

func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid_account_id", "account id must be a UUID")
		return
	}
	cached, err := s.ledger.CachedBalance(r.Context(), id)
	if errors.Is(err, ledger.ErrAccountNotFound) {
		respondError(w, http.StatusNotFound, "account_not_found", "account not found")
		return
	}
	if err != nil {
		slog.Error("cached balance", "err", err)
		respondError(w, http.StatusInternalServerError, "internal_error", "failed to read balance")
		return
	}
	computed, err := s.ledger.ComputedBalance(r.Context(), id)
	if err != nil {
		slog.Error("computed balance", "err", err)
		respondError(w, http.StatusInternalServerError, "internal_error", "failed to read balance")
		return
	}
	respond(w, http.StatusOK, encodeBalance(id, cached, computed))
}

func (s *Server) handleCharge(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		respondError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}

	statusCode, body, err := s.idem.Execute(r.Context(), key, rawBody, func(ctx context.Context, tx pgx.Tx) (int, []byte, *uuid.UUID, error) {
		var req chargeRequest
		if err := json.Unmarshal(rawBody, &req); err != nil {
			return encodeError(http.StatusBadRequest, "invalid_json", err.Error())
		}
		if req.Amount <= 0 {
			return encodeError(http.StatusBadRequest, "invalid_amount", "amount must be a positive integer number of minor units")
		}
		if len(req.Currency) != 3 {
			return encodeError(http.StatusBadRequest, "invalid_currency", "currency must be a 3-letter code")
		}

		txn, err := s.ledger.ChargeTx(ctx, tx, ledger.ChargeRequest{
			SourceAccountID:      req.SourceAccountID,
			DestinationAccountID: req.DestinationAccountID,
			Amount:               req.Amount,
			Currency:             req.Currency,
		})
		if err != nil {
			var lerr *ledger.Error
			if errors.As(err, &lerr) {
				return encodeError(lerr.HTTPStatus, lerr.Code, lerr.Message)
			}
			return 0, nil, nil, err
		}
		return http.StatusCreated, encodeTransaction(txn), &txn.ID, nil
	})
	respondIdempotent(w, statusCode, body, err)
}

func (s *Server) handleRefund(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		respondError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}

	statusCode, body, err := s.idem.Execute(r.Context(), key, rawBody, func(ctx context.Context, tx pgx.Tx) (int, []byte, *uuid.UUID, error) {
		var req refundRequest
		if err := json.Unmarshal(rawBody, &req); err != nil {
			return encodeError(http.StatusBadRequest, "invalid_json", err.Error())
		}
		if req.Amount < 0 {
			return encodeError(http.StatusBadRequest, "invalid_amount", "amount must not be negative")
		}

		txn, err := s.ledger.RefundTx(ctx, tx, ledger.RefundRequest{
			ChargeTransactionID: req.ChargeTransactionID,
			Amount:              req.Amount,
		})
		if err != nil {
			var lerr *ledger.Error
			if errors.As(err, &lerr) {
				return encodeError(lerr.HTTPStatus, lerr.Code, lerr.Message)
			}
			return 0, nil, nil, err
		}
		return http.StatusCreated, encodeTransaction(txn), &txn.ID, nil
	})
	respondIdempotent(w, statusCode, body, err)
}

func respondIdempotent(w http.ResponseWriter, statusCode int, body []byte, err error) {
	switch {
	case err == nil:
		respond(w, statusCode, body)
	case errors.Is(err, idempotency.ErrMismatch):
		respondError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "this Idempotency-Key was already used with a different request body")
	case errors.Is(err, idempotency.ErrConflict):
		respondError(w, http.StatusConflict, "request_in_flight", "a request with this Idempotency-Key is already being processed")
	default:
		slog.Error("idempotent execute", "err", err)
		respondError(w, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
	}
}

func respond(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func respondError(w http.ResponseWriter, status int, code, message string) {
	_, body, _, _ := encodeError(status, code, message)
	respond(w, status, body)
}
