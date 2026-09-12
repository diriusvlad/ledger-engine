package outbox

import "testing"

func TestBackoffWithFullJitterStaysWithinBounds(t *testing.T) {
	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := backoffWithFullJitter(defaultBaseBackoff, defaultMaxBackoff, attempt)
			if d < 0 {
				t.Fatalf("attempt %d: negative backoff %v", attempt, d)
			}
			if d > defaultMaxBackoff {
				t.Fatalf("attempt %d: backoff %v exceeds cap %v", attempt, d, defaultMaxBackoff)
			}
		}
	}
}

func TestBackoffWithFullJitterGrowsWithAttempts(t *testing.T) {
	// Not strictly monotonic per-sample (it's jittered), but the ceiling
	// for attempt 5 should be well above the ceiling for attempt 1.
	maxAt := func(attempt int) float64 {
		var max float64
		for i := 0; i < 200; i++ {
			d := backoffWithFullJitter(defaultBaseBackoff, defaultMaxBackoff, attempt)
			if f := float64(d); f > max {
				max = f
			}
		}
		return max
	}
	if maxAt(5) <= maxAt(1) {
		t.Fatalf("expected backoff ceiling to grow with attempts")
	}
}
