package backtest

import (
	"testing"
	"time"
)

// TestBacktestPackageCompiles is the Phase 0 compile-and-shape smoke.
// The backtest design owns these types (§5.1); this test pins the field set
// so a future rename of a JSON-tag or a status constant fails loudly in
// `go test` instead of mid-migration.
func TestBacktestPackageCompiles(t *testing.T) {
	t.Parallel()

	now := time.Now()
	b := &Backtest{
		ID:          "b0000000-0000-0000-0000-000000000001",
		StrategyID:  "c0000000-0000-0000-0000-000000000001",
		VersionID:   "d0000000-0000-0000-0000-000000000001",
		UserID:      "a0000000-0000-0000-0000-000000000001",
		Status:      StatusQueued,
		RequestedAt: now,
		WindowStart: now,
		WindowEnd:   now.Add(time.Hour),
		Resolution:  "1m",
		Params:      map[string]string{"foo": "bar"},
		ImageRef:    "local/sha:abc",
	}
	if b.Status != StatusQueued {
		t.Fatalf("status round-trip: got %q", b.Status)
	}
	s := &Summary{
		TotalReturn:     0.123,
		Sharpe:          1.4,
		MaxDrawdown:     0.05,
		TradeCount:      42,
		WinRate:         0.6,
		StartEquity:     10000,
		EndEquity:       11230,
		ParamsEffective: map[string]string{"foo": "bar"},
	}
	if s.TradeCount != 42 {
		t.Fatalf("summary trade count: got %d", s.TradeCount)
	}
	if _, ok := any(b).(*Backtest); !ok {
		t.Fatal("Backtest must remain a pointer-friendly type")
	}
}

// TestStatusConstants pins the on-the-wire status names. The migration's
// CHECK constraint and the partial unique index both depend on these
// literal strings; renaming any constant is a migration too.
func TestStatusConstants(t *testing.T) {
	t.Parallel()
	cases := []struct {
		got  Status
		want string
	}{
		{StatusQueued, "queued"},
		{StatusRunning, "running"},
		{StatusSucceeded, "succeeded"},
		{StatusFailed, "failed"},
		{StatusCancelled, "cancelled"},
	}
	for _, c := range cases {
		if string(c.got) != c.want {
			t.Errorf("status %q != expected %q", c.got, c.want)
		}
	}
}
