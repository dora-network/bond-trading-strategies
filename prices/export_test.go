package prices

import "context"

// Export for testing
func (h *Handler) BuildURL() (string, error) {
	return h.buildURL()
}

func (h *Handler) ProcessMessage(ctx context.Context, data []byte) error {
	return h.processMessage(ctx, data)
}
