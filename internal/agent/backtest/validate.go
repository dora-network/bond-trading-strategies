package backtest

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// maxWindowDays caps a single backtest's time span. The WasmStarter
// streams the replay window page-by-page (bounded by ~3,500 rows in
// flight regardless of length) and overlaps fetch with replay, so
// memory + wall-clock are no longer dominated by window length.
// 365 days covers full-year seasonality for mean-reversion and
// momentum strategies. Tune via a config value if real workload
// shows 365d is too aggressive — ponytail: hard cap keeps the
// validation a single function.
const maxWindowDays = 365

// MaxWarmupCandles is the inclusive upper bound on the preamble
// fetch window. math.MaxInt32 matches the Postgres INTEGER column
// from migration 014_deployments_warmup_candles.sql — values
// above this overflow into a 500. Single source of truth used by
// the backtest, deploy-HTTP, and deploy-tool bounds checks;
// exported so callers don't repeat the literal.
const MaxWarmupCandles = math.MaxInt32

var (
	errInvalidWindow      = errors.New("backtest: end must be after start")
	errInvalidResolution  = errors.New("backtest: unsupported resolution")
	errWindowTooLong      = errors.New("backtest: window exceeds 365 days")
	errMissingOrderBookID = errors.New("backtest: order_book_id is required")
	errWarmupOutOfRange   = fmt.Errorf("backtest: warmup_candles must be between 0 and %d", MaxWarmupCandles)
)

// ParseRequest validates the wire shape of POST .../backtest. The handler
// calls this before touching persistence so the caller gets a 400 for the
// cheap errors and never a 5xx from a malformed body. Returns a sentinel
// error so the handler can map each case to the right status code without
// string-matching.
func ParseRequest(req *Request) error {
	if req == nil {
		return errInvalidWindow
	}
	if req.OrderBookID == "" {
		return errMissingOrderBookID
	}
	if !req.End.After(req.Start) {
		return errInvalidWindow
	}
	if req.End.Sub(req.Start) > maxWindowDays*24*time.Hour {
		return errWindowTooLong
	}
	// Local lookup; a const map would be flagged by gochecknoglobals and
	// a package-level var would create an import-order surface for tests.
	if req.WarmupCandles < 0 || req.WarmupCandles > MaxWarmupCandles {
		return errWarmupOutOfRange
	}
	switch req.Resolution {
	case "1m", "5m", "15m", "1h", "4h", "1d":
	default:
		return errInvalidResolution
	}
	return nil
}
