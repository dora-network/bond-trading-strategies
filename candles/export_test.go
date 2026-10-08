package candles

import (
	"context"
	"time"
)

// Export for testing
func (h *Handler) BuildURL(orderBookID string, since *time.Time) (string, error) {
	return h.buildURL(orderBookID, since)
}

func (h *Handler) SafeURL(rawURL string) string {
	return h.safeURL(rawURL)
}

func (h *Handler) ProcessMessage(ctx context.Context, orderBookID string, data []byte) error {
	return h.processMessage(ctx, orderBookID, data)
}

func (h *Handler) StreamSingle(ctx context.Context, orderBookID string) error {
	return h.streamSingle(ctx, orderBookID)
}

func (h *Handler) Cfg() Config {
	return h.cfg
}

// MaxCandleRowsForTest lets tests shrink the pagination page size.
var MaxCandleRowsForTest = &maxCandleRows

// PushTimeoutForTest lets tests snapshot the current fan-out
// deadline (raw read; safe because tests serialize writes through
// SetPushTimeoutForTest).
var PushTimeoutForTest = &pushTimeout

// SetPushTimeoutForTest shrinks the fan-out deadline under the
// write lock so concurrent processMessage reads (RLock) are
// race-free. Snapshot via PushTimeoutForTest before calling.
func SetPushTimeoutForTest(d time.Duration) {
	pushTimeoutMu.Lock()
	defer pushTimeoutMu.Unlock()
	pushTimeout = d
}
