package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/vlad/ledger-engine/internal/httpapi"
	"github.com/vlad/ledger-engine/internal/ledger"
)

// TestIdempotency_100ConcurrentIdenticalCharges_ExactlyOneTransaction is
// the test called for directly in the project brief: fire the same charge
// request 100 times in parallel with the same Idempotency-Key and assert
// exactly one transaction is created. It also checks the stronger,
// realistic property that every one of the 100 callers gets back the same
// 201 response instead of a mix of 201s and 409s.
func TestIdempotency_100ConcurrentIdenticalCharges_ExactlyOneTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	srv := httptest.NewServer(httpapi.NewServer(pool))
	defer srv.Close()

	payer := fundedAccount(t, ctx, svc, "RON", 1_000_000)
	merchant, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create merchant: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"amount":                 500,
		"currency":               "RON",
		"source_account_id":      payer.ID,
		"destination_account_id": merchant.ID,
	})

	const n = 100
	client := &http.Client{}
	statuses := make([]int, n)
	bodies := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/charges", bytes.NewReader(body))
			if err != nil {
				t.Errorf("build request: %v", err)
				return
			}
			req.Header.Set("Idempotency-Key", "concurrent-charge-key")
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(resp.Body)
			statuses[i] = resp.StatusCode
			bodies[i] = buf.String()
		}(i)
	}
	wg.Wait()

	for i, s := range statuses {
		if s != http.StatusCreated {
			t.Errorf("response %d: status = %d, body = %s, want 201", i, s, bodies[i])
		}
	}
	for i := 1; i < n; i++ {
		if bodies[i] != bodies[0] {
			t.Errorf("response %d body differs from response 0's: %s vs %s", i, bodies[i], bodies[0])
		}
	}

	var txnCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE destination_account_id = $1`, merchant.ID).Scan(&txnCount); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if txnCount != 1 {
		t.Fatalf("transactions created for this charge = %d, want exactly 1", txnCount)
	}

	var keyCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE key = 'concurrent-charge-key'`).Scan(&keyCount); err != nil {
		t.Fatalf("count idempotency keys: %v", err)
	}
	if keyCount != 1 {
		t.Fatalf("idempotency_keys rows for the key = %d, want exactly 1", keyCount)
	}
}

func TestIdempotency_HTTP_SameKeyDifferentBodyReturns422(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc := ledger.New(pool)

	srv := httptest.NewServer(httpapi.NewServer(pool))
	defer srv.Close()

	payer := fundedAccount(t, ctx, svc, "RON", 1_000_000)
	merchant, err := svc.CreateAccount(ctx, "merchant", "RON", false)
	if err != nil {
		t.Fatalf("create merchant: %v", err)
	}

	post := func(amount int) *http.Response {
		body, _ := json.Marshal(map[string]any{
			"amount": amount, "currency": "RON",
			"source_account_id": payer.ID, "destination_account_id": merchant.ID,
		})
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/charges", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", "reused-key")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		return resp
	}

	resp1 := post(100)
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("first request: status = %d, want 201", resp1.StatusCode)
	}

	resp2 := post(200)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("second request (different body, same key): status = %d, want 422", resp2.StatusCode)
	}

	var txnCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE destination_account_id = $1`, merchant.ID).Scan(&txnCount); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if txnCount != 1 {
		t.Fatalf("transactions created for this charge = %d, want exactly 1", txnCount)
	}
}

func TestIdempotency_HTTP_MissingHeaderReturns400(t *testing.T) {
	pool := testPool(t)
	srv := httptest.NewServer(httpapi.NewServer(pool))
	defer srv.Close()

	body, _ := json.Marshal(map[string]any{"amount": 100, "currency": "RON"})
	resp, err := http.Post(srv.URL+"/charges", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
