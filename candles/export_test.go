package candles

import (
	"context"
	"time"
)

// Export for testing
func (h *Handler) BuildURL(orderBookID string, since *time.Time) (string, error) {
	return h.buildURL(orderBookID, since)
}

func (h *Handler) ProcessMessage(ctx context.Context, orderBookID string, data []byte) error {
	return h.processMessage(ctx, orderBookID, data)
}

func (h *Handler) StreamSingle(ctx context.Context, orderBookID string) error {
	return h.streamSingle(ctx, orderBookID)
}
