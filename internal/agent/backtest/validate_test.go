package backtest

import (
	"errors"
	"testing"
	"time"
)

func TestParseRequest_Valid(t *testing.T) {
	t.Parallel()
	req := &Request{
		Start:       time.Now().Add(-24 * time.Hour),
		End:         time.Now(),
		Resolution:  "1h",
		OrderBookID: "ob-1",
	}
	if err := ParseRequest(req); err != nil {
		t.Errorf("valid request: got %v, want nil", err)
	}
}

func TestParseRequest_WindowTooLong(t *testing.T) {
	t.Parallel()
	req := &Request{
		Start:       time.Now().Add(-366 * 24 * time.Hour),
		End:         time.Now(),
		Resolution:  "1h",
		OrderBookID: "ob-1",
	}
	if err := ParseRequest(req); !errors.Is(err, errWindowTooLong) {
		t.Errorf("expected errWindowTooLong, got %v", err)
	}
}

// TestParseRequest_WindowCapBoundary pins the streaming cap at 365
// days. After the streaming backtest slice landed, 100-day and
// 180-day windows must be ACCEPTED (the old 90-day heuristic no
// longer applies); 365 days is the boundary ACCEPT; 366+ REJECTS.
// This test would have caught the c09f761 churn that left the
// constant at 90 while updating the error message to 365.
func TestParseRequest_WindowCapBoundary(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cases := []struct {
		name    string
		days    int
		wantErr bool
	}{
		{"100d", 100, false},
		{"180d", 180, false},
		{"365d", 365, false},
		{"366d", 366, true},
		{"730d", 730, true},
	}
	for _, tc := range cases {
		days := tc.days
		wantErr := tc.wantErr
		t.Run(tc.name, func(t *testing.T) {
			req := &Request{
				Start:       now.Add(-time.Duration(days) * 24 * time.Hour),
				End:         now,
				Resolution:  "1h",
				OrderBookID: "ob-1",
			}
			err := ParseRequest(req)
			if (err != nil) != wantErr {
				t.Errorf("%s: wantErr=%v gotErr=%v err=%v", tc.name, wantErr, err != nil, err)
			}
		})
	}
}

func TestParseRequest_EndBeforeStart(t *testing.T) {
	now := time.Now()
	req := &Request{
		Start:       now,
		End:         now.Add(-time.Hour),
		Resolution:  "1h",
		OrderBookID: "ob-1",
	}
	if err := ParseRequest(req); !errors.Is(err, errInvalidWindow) {
		t.Errorf("expected errInvalidWindow, got %v", err)
	}
}

func TestParseRequest_EndEqualsStart(t *testing.T) {
	t.Parallel()
	now := time.Now()
	req := &Request{
		Start:       now,
		End:         now,
		Resolution:  "1h",
		OrderBookID: "ob-1",
	}
	// Equal start/end is a degenerate window — same error as inverted.
	if err := ParseRequest(req); !errors.Is(err, errInvalidWindow) {
		t.Errorf("expected errInvalidWindow, got %v", err)
	}
}

func TestParseRequest_BadResolution(t *testing.T) {
	t.Parallel()
	req := &Request{
		Start:       time.Now().Add(-time.Hour),
		End:         time.Now(),
		Resolution:  "7h",
		OrderBookID: "ob-1",
	}
	if err := ParseRequest(req); !errors.Is(err, errInvalidResolution) {
		t.Errorf("expected errInvalidResolution, got %v", err)
	}
}

func TestParseRequest_AllResolutions(t *testing.T) {
	t.Parallel()
	cases := []string{"1m", "5m", "15m", "1h", "4h", "1d"}
	for _, res := range cases {
		req := &Request{
			Start:       time.Now().Add(-time.Hour),
			End:         time.Now(),
			Resolution:  res,
			OrderBookID: "ob-1",
		}
		if err := ParseRequest(req); err != nil {
			t.Errorf("resolution %q: unexpected error %v", res, err)
		}
	}
}

func TestParseRequest_MissingOrderBookID(t *testing.T) {
	t.Parallel()
	req := &Request{
		Start:      time.Now().Add(-24 * time.Hour),
		End:        time.Now(),
		Resolution: "1h",
	}
	if err := ParseRequest(req); !errors.Is(err, errMissingOrderBookID) {
		t.Errorf("expected errMissingOrderBookID, got %v", err)
	}
}

func TestParseRequest_WarmupCandlesBounds(t *testing.T) {
	t.Parallel()
	base := func(w int) *Request {
		return &Request{
			Start:         time.Now().Add(-24 * time.Hour),
			End:           time.Now(),
			Resolution:    "1h",
			OrderBookID:   "ob-1",
			WarmupCandles: w,
		}
	}
	if err := ParseRequest(base(0)); err != nil {
		t.Errorf("warmup 0: got %v, want nil", err)
	}
	if err := ParseRequest(base(MaxWarmupCandles)); err != nil {
		t.Errorf("warmup max int32: got %v, want nil", err)
	}
	if err := ParseRequest(base(-1)); !errors.Is(err, errWarmupOutOfRange) {
		t.Errorf("warmup -1: want errWarmupOutOfRange, got %v", err)
	}
	if err := ParseRequest(base(MaxWarmupCandles + 1)); !errors.Is(err, errWarmupOutOfRange) {
		t.Errorf("warmup max int32+1: want errWarmupOutOfRange, got %v", err)
	}
}
