// Package ledger implements the double-entry core: accounts, transactions
// and append-only entries, plus the charge and refund operations that
// produce them.
package ledger

import (
	"time"

	"github.com/google/uuid"
)

type Account struct {
	ID            uuid.UUID
	Name          string
	Currency      string
	AllowNegative bool
	CachedBalance int64
	CreatedAt     time.Time
}

const (
	TxnTypeCharge = "charge"
	TxnTypeRefund = "refund"
)

type Transaction struct {
	ID                   uuid.UUID
	Type                 string
	Amount               int64
	Currency             string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	ParentTransactionID  *uuid.UUID
	CreatedAt            time.Time
}

type Entry struct {
	ID            int64
	TransactionID uuid.UUID
	AccountID     uuid.UUID
	Amount        int64
	CreatedAt     time.Time
}

// ChargeRequest describes a request to move funds from one account to
// another. Amount is always positive; the service derives the signed
// entries.
type ChargeRequest struct {
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	Amount               int64
	Currency             string
}

// RefundRequest describes a request to reverse (all or part of) a prior
// charge.
type RefundRequest struct {
	ChargeTransactionID uuid.UUID
	Amount              int64 // 0 means "refund whatever remains"
}
