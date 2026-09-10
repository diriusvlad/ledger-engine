package ledger

import "fmt"

// Error is a domain error with an HTTP status attached, so the API layer
// never has to guess how to translate a business rule violation into a
// response code.
type Error struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newError(code string, status int, format string, args ...any) *Error {
	return &Error{Code: code, HTTPStatus: status, Message: fmt.Sprintf(format, args...)}
}

var (
	ErrAccountNotFound = &Error{Code: "account_not_found", HTTPStatus: 404, Message: "account not found"}
	ErrChargeNotFound  = &Error{Code: "charge_not_found", HTTPStatus: 404, Message: "charge transaction not found"}
	ErrInvalidAmount   = &Error{Code: "invalid_amount", HTTPStatus: 400, Message: "amount must be a positive integer number of minor units"}
)

func ErrCurrencyMismatch(expected, got string) *Error {
	return newError("currency_mismatch", 400, "accounts/request use currency %q but got %q; cross-currency transactions are not supported", expected, got)
}

func ErrInsufficientFunds(accountID string, balance, amount int64) *Error {
	return newError("insufficient_funds", 402, "account %s has balance %d, which cannot cover amount %d", accountID, balance, amount)
}

func ErrRefundExceedsRemaining(remaining, requested int64) *Error {
	return newError("refund_exceeds_remaining", 422, "refund amount %d exceeds remaining refundable amount %d", requested, remaining)
}

func ErrAlreadyFullyRefunded(chargeID string) *Error {
	return newError("already_fully_refunded", 422, "charge %s has already been fully refunded", chargeID)
}
