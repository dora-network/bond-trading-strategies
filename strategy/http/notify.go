package http

import (
	"context"

	"github.com/dora-network/bond-trading-strategies/notifications"
)

// publishEvent forwards a lifecycle event to the configured Notifier.
// When no Notifier is wired in, it is a no-op so handlers that emit
// events stay safe to call from tests and from code paths that run
// before the option is applied. The error from Publish is intentionally
// discarded: log failures are surfaced inside the bus, and the publish
// path itself does not need to propagate errors to the HTTP handler.
func (h *Handler) publishEvent(ctx context.Context, evt notifications.Event) {
	if h.notifier == nil {
		return
	}
	_ = h.notifier.Publish(ctx, evt)
}
