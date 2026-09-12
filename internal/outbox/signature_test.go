package outbox

import "testing"

func TestSignatureRoundTrip(t *testing.T) {
	secret := "shh"
	body := []byte(`{"hello":"world"}`)
	ts := int64(1700000000)

	header := SignatureHeader(secret, ts, body)
	gotTS, ok := VerifySignature(header, secret, body)
	if !ok {
		t.Fatalf("expected signature to verify, header=%q", header)
	}
	if gotTS != ts {
		t.Fatalf("timestamp mismatch: got %d want %d", gotTS, ts)
	}
}

func TestSignatureRejectsTamperedBody(t *testing.T) {
	secret := "shh"
	header := SignatureHeader(secret, 1700000000, []byte(`{"a":1}`))
	if _, ok := VerifySignature(header, secret, []byte(`{"a":2}`)); ok {
		t.Fatal("expected signature verification to fail for a tampered body")
	}
}

func TestSignatureRejectsWrongSecret(t *testing.T) {
	body := []byte(`{"a":1}`)
	header := SignatureHeader("secret-a", 1700000000, body)
	if _, ok := VerifySignature(header, "secret-b", body); ok {
		t.Fatal("expected signature verification to fail for the wrong secret")
	}
}
