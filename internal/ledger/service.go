package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vlad/ledger-engine/internal/outbox"
)

type Service struct {
	pool *pgxpool.Pool

	// crashAfterDebitEntry is a test-only fault injection seam (see
	// crash_test.go, package-internal so it has no exported surface).
	// When set, writeEntryPair panics after inserting the debit entry
	// and before inserting the credit entry, letting a white-box test
	// prove that a mid-transaction panic leaves no half-written state:
	// the deferred tx.Rollback in Charge/Refund still runs during the
	// panic unwind.
	crashAfterDebitEntry bool
}

func New(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// Charge runs ChargeTx in its own, freshly-begun database transaction.
func (s *Service) Charge(ctx context.Context, req ChargeRequest) (*Transaction, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("ledger: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	txn, err := s.ChargeTx(ctx, tx, req)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("ledger: commit: %w", err)
	}
	return txn, nil
}

// ChargeTx performs a single balanced money movement from
// req.SourceAccountID to req.DestinationAccountID: one transaction row and
// two entries (-amount, +amount), plus an outbox event, all within the
// caller-supplied transaction. It is exported so the idempotency layer can
// run it inside the same DB transaction that claims the idempotency key.
//
// Concurrency: the source account row is locked with SELECT ... FOR
// UPDATE before its balance is checked, so two concurrent charges against
// the same source account are serialized rather than racing on a
// check-then-write of the balance (see README "Concurrency control").
// The destination account is read without a lock; its cached_balance is
// updated with an atomic `cached_balance = cached_balance + $1`, which
// Postgres applies against the row's current value under its own
// short-lived write lock, so it never loses an update even without an
// explicit FOR UPDATE. Because only ever one account (the source) is
// explicitly locked per call, two charges moving money in opposite
// directions between the same pair of accounts cannot deadlock on lock
// ordering.
func (s *Service) ChargeTx(ctx context.Context, tx pgx.Tx, req ChargeRequest) (*Transaction, error) {
	if req.Amount <= 0 {
		return nil, ErrInvalidAmount
	}

	var source Account
	err := tx.QueryRow(ctx, `
		SELECT id, currency, allow_negative, cached_balance
		FROM accounts WHERE id = $1
		FOR UPDATE`, req.SourceAccountID,
	).Scan(&source.ID, &source.Currency, &source.AllowNegative, &source.CachedBalance)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: lock source account: %w", err)
	}

	var destCurrency string
	err = tx.QueryRow(ctx, `SELECT currency FROM accounts WHERE id = $1`, req.DestinationAccountID).Scan(&destCurrency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: read destination account: %w", err)
	}

	if source.Currency != req.Currency {
		return nil, ErrCurrencyMismatch(source.Currency, req.Currency)
	}
	if destCurrency != req.Currency {
		return nil, ErrCurrencyMismatch(destCurrency, req.Currency)
	}

	if !source.AllowNegative && source.CachedBalance-req.Amount < 0 {
		return nil, ErrInsufficientFunds(source.ID.String(), source.CachedBalance, req.Amount)
	}

	txn := &Transaction{
		ID:                   uuid.New(),
		Type:                 TxnTypeCharge,
		Amount:               req.Amount,
		Currency:             req.Currency,
		SourceAccountID:      req.SourceAccountID,
		DestinationAccountID: req.DestinationAccountID,
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, type, amount, currency, source_account_id, destination_account_id)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		txn.ID, txn.Type, txn.Amount, txn.Currency, txn.SourceAccountID, txn.DestinationAccountID,
	); err != nil {
		return nil, fmt.Errorf("ledger: insert transaction: %w", err)
	}

	if err := s.writeEntryPair(ctx, tx, txn.ID, req.SourceAccountID, req.DestinationAccountID, req.Amount); err != nil {
		return nil, err
	}

	payload := chargeEventPayload{
		TransactionID:        txn.ID,
		Amount:               txn.Amount,
		Currency:             txn.Currency,
		SourceAccountID:      txn.SourceAccountID,
		DestinationAccountID: txn.DestinationAccountID,
	}
	if err := outbox.Insert(ctx, tx, "transaction", txn.ID, "charge.succeeded", payload); err != nil {
		return nil, err
	}

	return txn, nil
}

// Refund runs RefundTx in its own, freshly-begun database transaction.
func (s *Service) Refund(ctx context.Context, req RefundRequest) (*Transaction, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("ledger: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	txn, err := s.RefundTx(ctx, tx, req)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("ledger: commit: %w", err)
	}
	return txn, nil
}

// RefundTx reverses all or part of a prior charge. It locks the charge's
// transaction row with SELECT ... FOR UPDATE before computing how much of
// it has already been refunded, which is what makes two concurrent
// refunds of the same charge safe: the second refund's lock acquisition
// blocks until the first has committed (and its refund total is visible),
// at which point the remaining-amount check sees the up-to-date total and
// rejects any refund that would exceed the original charge. Without this
// lock, two concurrent full refunds would each read "0 already refunded"
// and both succeed — the books would still balance (each refund is
// individually balanced), so the zero-sum invariant alone can never catch
// a double refund. See README "The refund race".
func (s *Service) RefundTx(ctx context.Context, tx pgx.Tx, req RefundRequest) (*Transaction, error) {
	var charge Transaction
	err := tx.QueryRow(ctx, `
		SELECT id, type, amount, currency, source_account_id, destination_account_id
		FROM transactions WHERE id = $1
		FOR UPDATE`, req.ChargeTransactionID,
	).Scan(&charge.ID, &charge.Type, &charge.Amount, &charge.Currency, &charge.SourceAccountID, &charge.DestinationAccountID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrChargeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: lock charge: %w", err)
	}
	if charge.Type != TxnTypeCharge {
		return nil, ErrChargeNotFound
	}

	var alreadyRefunded int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount), 0) FROM transactions
		WHERE parent_transaction_id = $1 AND type = 'refund'`, charge.ID,
	).Scan(&alreadyRefunded); err != nil {
		return nil, fmt.Errorf("ledger: sum prior refunds: %w", err)
	}

	remaining := charge.Amount - alreadyRefunded
	if remaining <= 0 {
		// Checked before consulting req.Amount so that a concurrent
		// full refund which lost the race (see RefundTx doc comment)
		// gets a clear "already fully refunded" rejection instead of a
		// confusing "invalid amount" for its "refund whatever remains"
		// request.
		return nil, ErrAlreadyFullyRefunded(charge.ID.String())
	}
	amount := req.Amount
	if amount == 0 {
		amount = remaining // 0 means "refund whatever remains"
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if amount > remaining {
		return nil, ErrRefundExceedsRemaining(remaining, amount)
	}

	parentID := charge.ID
	txn := &Transaction{
		ID:                   uuid.New(),
		Type:                 TxnTypeRefund,
		Amount:               amount,
		Currency:             charge.Currency,
		SourceAccountID:      charge.DestinationAccountID, // money flows back
		DestinationAccountID: charge.SourceAccountID,
		ParentTransactionID:  &parentID,
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, type, amount, currency, source_account_id, destination_account_id, parent_transaction_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		txn.ID, txn.Type, txn.Amount, txn.Currency, txn.SourceAccountID, txn.DestinationAccountID, txn.ParentTransactionID,
	); err != nil {
		return nil, fmt.Errorf("ledger: insert refund transaction: %w", err)
	}

	if err := s.writeEntryPair(ctx, tx, txn.ID, txn.SourceAccountID, txn.DestinationAccountID, amount); err != nil {
		return nil, err
	}

	payload := refundEventPayload{
		TransactionID:       txn.ID,
		ChargeTransactionID: charge.ID,
		Amount:              amount,
		Currency:            txn.Currency,
	}
	if err := outbox.Insert(ctx, tx, "transaction", txn.ID, "refund.succeeded", payload); err != nil {
		return nil, err
	}

	return txn, nil
}

// writeEntryPair inserts the two entries for a balanced transaction and
// keeps each account's cached_balance in lockstep. The UPDATE statements
// use `cached_balance = cached_balance + $1` (a relative, not absolute,
// write), so they are safe under concurrent execution without any extra
// locking: Postgres evaluates the right-hand side against the row's
// current value while holding the row's write lock for the statement.
func (s *Service) writeEntryPair(ctx context.Context, tx pgx.Tx, txnID, debitAccountID, creditAccountID uuid.UUID, amount int64) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)`,
		txnID, debitAccountID, -amount,
	); err != nil {
		return fmt.Errorf("ledger: insert debit entry: %w", err)
	}
	if s.crashAfterDebitEntry {
		panic("ledger: injected crash between entry inserts (test only)")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)`,
		txnID, creditAccountID, amount,
	); err != nil {
		return fmt.Errorf("ledger: insert credit entry: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET cached_balance = cached_balance - $1 WHERE id = $2`, amount, debitAccountID); err != nil {
		return fmt.Errorf("ledger: debit cached balance: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET cached_balance = cached_balance + $1 WHERE id = $2`, amount, creditAccountID); err != nil {
		return fmt.Errorf("ledger: credit cached balance: %w", err)
	}
	return nil
}

// ComputedBalance returns SUM(entries.amount) for accountID: the source of
// truth balance.
func (s *Service) ComputedBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	var bal int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0) FROM entries WHERE account_id = $1`, accountID).Scan(&bal)
	if err != nil {
		return 0, fmt.Errorf("ledger: computed balance: %w", err)
	}
	return bal, nil
}

// CachedBalance returns accounts.cached_balance for accountID.
func (s *Service) CachedBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	var bal int64
	err := s.pool.QueryRow(ctx, `SELECT cached_balance FROM accounts WHERE id = $1`, accountID).Scan(&bal)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrAccountNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("ledger: cached balance: %w", err)
	}
	return bal, nil
}

// CreateAccount inserts a new account row.
func (s *Service) CreateAccount(ctx context.Context, name, currency string, allowNegative bool) (*Account, error) {
	acct := &Account{ID: uuid.New(), Name: name, Currency: currency, AllowNegative: allowNegative}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (id, name, currency, allow_negative) VALUES ($1, $2, $3, $4)`,
		acct.ID, acct.Name, acct.Currency, acct.AllowNegative)
	if err != nil {
		return nil, fmt.Errorf("ledger: create account: %w", err)
	}
	return acct, nil
}

// GetTransaction fetches a transaction by ID.
func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (*Transaction, error) {
	var txn Transaction
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, amount, currency, source_account_id, destination_account_id, parent_transaction_id
		FROM transactions WHERE id = $1`, id,
	).Scan(&txn.ID, &txn.Type, &txn.Amount, &txn.Currency, &txn.SourceAccountID, &txn.DestinationAccountID, &txn.ParentTransactionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrChargeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: get transaction: %w", err)
	}
	return &txn, nil
}

type chargeEventPayload struct {
	TransactionID        uuid.UUID `json:"transaction_id"`
	Amount               int64     `json:"amount"`
	Currency             string    `json:"currency"`
	SourceAccountID      uuid.UUID `json:"source_account_id"`
	DestinationAccountID uuid.UUID `json:"destination_account_id"`
}

type refundEventPayload struct {
	TransactionID       uuid.UUID `json:"transaction_id"`
	ChargeTransactionID uuid.UUID `json:"charge_transaction_id"`
	Amount              int64     `json:"amount"`
	Currency            string    `json:"currency"`
}
