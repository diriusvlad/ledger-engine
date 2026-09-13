package httpapi

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/vlad/ledger-engine/internal/ledger"
)

type errorDTO struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func encodeError(status int, code, message string) (int, []byte, *uuid.UUID, error) {
	var e errorDTO
	e.Error.Code = code
	e.Error.Message = message
	body, _ := json.Marshal(e)
	return status, body, nil, nil
}

type accountDTO struct {
	ID            uuid.UUID `json:"id"`
	Name          string    `json:"name"`
	Currency      string    `json:"currency"`
	AllowNegative bool      `json:"allow_negative"`
	CachedBalance int64     `json:"cached_balance"`
}

func encodeAccount(a *ledger.Account) []byte {
	body, _ := json.Marshal(accountDTO{
		ID:            a.ID,
		Name:          a.Name,
		Currency:      a.Currency,
		AllowNegative: a.AllowNegative,
		CachedBalance: a.CachedBalance,
	})
	return body
}

type createAccountRequest struct {
	Name          string `json:"name"`
	Currency      string `json:"currency"`
	AllowNegative bool   `json:"allow_negative"`
}

type transactionDTO struct {
	ID                   uuid.UUID  `json:"id"`
	Type                 string     `json:"type"`
	Amount               int64      `json:"amount"`
	Currency             string     `json:"currency"`
	SourceAccountID      uuid.UUID  `json:"source_account_id"`
	DestinationAccountID uuid.UUID  `json:"destination_account_id"`
	ParentTransactionID  *uuid.UUID `json:"parent_transaction_id,omitempty"`
	CreatedAt            time.Time  `json:"created_at,omitempty"`
}

func encodeTransaction(t *ledger.Transaction) []byte {
	body, _ := json.Marshal(transactionDTO{
		ID:                   t.ID,
		Type:                 t.Type,
		Amount:               t.Amount,
		Currency:             t.Currency,
		SourceAccountID:      t.SourceAccountID,
		DestinationAccountID: t.DestinationAccountID,
		ParentTransactionID:  t.ParentTransactionID,
	})
	return body
}

type chargeRequest struct {
	Amount               int64     `json:"amount"`
	Currency             string    `json:"currency"`
	SourceAccountID      uuid.UUID `json:"source_account_id"`
	DestinationAccountID uuid.UUID `json:"destination_account_id"`
}

type refundRequest struct {
	ChargeTransactionID uuid.UUID `json:"charge_transaction_id"`
	Amount              int64     `json:"amount"` // 0/omitted = refund remaining balance
}

type balanceDTO struct {
	AccountID       uuid.UUID `json:"account_id"`
	CachedBalance   int64     `json:"cached_balance"`
	ComputedBalance int64     `json:"computed_balance"`
}

func encodeBalance(accountID uuid.UUID, cached, computed int64) []byte {
	body, _ := json.Marshal(balanceDTO{AccountID: accountID, CachedBalance: cached, ComputedBalance: computed})
	return body
}
