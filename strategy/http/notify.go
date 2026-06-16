package http

import (
	"context"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/google/uuid"
)

// publishTimeout bounds how long a single Publish call may take. A
// notification insert is a single small PG write; if it cannot complete
// inside this window the bus or DB is in trouble and we should not block
// the calling goroutine. The value is generous enough to absorb a slow
// network round-trip or a transient DB stall without dropping events on
// the floor, but tight enough that a fully hung connection unblocks
// within a few seconds.
const publishTimeout = 5 * time.Second

// publishEvent forwards a lifecycle event to the configured Notifier.
// When no Notifier is wired in, it is a no-op so handlers that emit
// events stay safe to call from tests and from code paths that run
// before the option is applied. The error from Publish is intentionally
// discarded: log failures are surfaced inside the bus, and the publish
// path itself does not need to propagate errors to the HTTP handler.
//
// The event ID is filled in here (UUIDv7) so producers do not have to
// remember to generate it; the persisted log requires a valid UUID.
//
// The publish call runs under a bounded timeout derived from the
// supplied ctx, so a hung DB does not leak goroutines on backtest
// completion paths where the request context is already cancelled.
// Server shutdown propagates through the parent ctx, so the timeout
// also exits early during graceful shutdown.
func (h *Handler) publishEvent(ctx context.Context, evt notifications.Event) {
	if h.notifier == nil {
		return
	}
	if evt.UserID == "" {
		return
	}
	if evt.ID == "" {
		evt.ID = uuid.Must(uuid.NewV7()).String()
	}
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	_ = h.notifier.Publish(pubCtx, evt)
}
