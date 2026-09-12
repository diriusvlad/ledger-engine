package outbox

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
)

// Sign computes a standard webhook signature: HMAC-SHA256 over
// "<timestamp>.<body>", keyed by secret. Binding the timestamp into the
// signed material (rather than signing the body alone) lets a receiver
// reject replayed deliveries whose timestamp is too old, in addition to
// verifying authenticity.
func Sign(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignatureHeader formats the value of the X-Webhook-Signature header.
func SignatureHeader(secret string, timestamp int64, body []byte) string {
	return fmt.Sprintf("t=%d,v1=%s", timestamp, Sign(secret, timestamp, body))
}

// VerifySignature checks a header value produced by SignatureHeader
// against secret and body. It does not enforce a timestamp tolerance
// itself; callers that care about replay windows should parse the
// timestamp out and check it separately.
func VerifySignature(header, secret string, body []byte) (timestamp int64, ok bool) {
	var t int64
	var v1 string
	if _, err := fmt.Sscanf(header, "t=%d,v1=%s", &t, &v1); err != nil {
		return 0, false
	}
	expected := Sign(secret, t, body)
	if subtle.ConstantTimeCompare([]byte(expected), []byte(v1)) != 1 {
		return 0, false
	}
	return t, true
}
