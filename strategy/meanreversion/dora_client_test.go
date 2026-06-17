package meanreversion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoraAPIClient_CreateMarketOrder_ErrorHandling(t *testing.T) {
	// Create a mock server that returns a JSON error with "error" field.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{
			"error": "insufficient margin to open this position",
			"metadata": {
				"status_code": 400,
				"trace_id": "test-trace-id",
				"request_id": "test-request-id"
			}
		}`))
	}))
	defer server.Close()

	// Temporarily set DORA_BASE_URL to point to our mock server.
	originalBaseURL := os.Getenv("DORA_BASE_URL")
	defer os.Setenv("DORA_BASE_URL", originalBaseURL)
	os.Setenv("DORA_BASE_URL", server.URL)

	client := NewDoraClientWithKey("test-api-key")
	err := client.CreateMarketOrder(
		context.Background(),
		"test-orderbook",
		doraclient.SIDE_BUY,
		decimal.MustNew(10, 0),
		decimal.One,
		false,
		"test-client-order-id",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient margin to open this position")
	assert.Contains(t, err.Error(), "raw:")
}
