// Package dora implements the read-only Dora tool handlers.
// Each handler is a thin wrapper around the regenerated doraclient SDK using
// a per-request API key. See spec §5 (Dora read tools).
package dora

import (
	"context"
	"net/http"
	"time"

	doraclient "github.com/dora-network/dora-client-go/doraclient"
)

// DoraReadTimeout bounds an upstream Dora read-tool HTTP call. A stalled
// upstream (open TCP socket, no response) would otherwise hang the agent's
// tool dispatch forever and the agent's per-iteration LLM timeout would never
// observe the goroutine stuck in the SDK call. The 90s DefaultLLMTimeout
// only watches the Stream call, not handler invocations.
const DoraReadTimeout = 30 * time.Second

// apiKeyHeader is the map key the SDK reads from the auth context to populate
// the HTTP "Authorization" header. Mirrors internal/auth/dora.go.
const apiKeyHeader = "apiKeyAuthHeader"

// apiKeyPrefix is the HTTP auth scheme Dora expects: "Authorization: ApiKey <key>".
const apiKeyPrefix = "ApiKey"

// NewClient returns a *doraclient.APIClient pointed at baseURL. The API key is
// NOT baked into the client: per spec §3 the credential is per-request, held in
// memory for the turn and attached via WithAPIKey on each call, matching the
// per-request auth flow in internal/auth/dora.go.
func NewClient(baseURL string) *doraclient.APIClient {
	cfg := doraclient.NewConfiguration()
	cfg.Servers = doraclient.ServerConfigurations{
		{URL: baseURL},
	}
	cfg.HTTPClient = &http.Client{Timeout: DoraReadTimeout}
	return doraclient.NewAPIClient(cfg)
}

// WithAPIKey returns a context carrying apiKey in the SDK's ContextAPIKeys
// value, so the outbound request gets "Authorization: ApiKey <key>". This is
// the same idiom internal/auth/dora.go uses for GetUserSelf.
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, doraclient.ContextAPIKeys, map[string]doraclient.APIKey{
		apiKeyHeader: {Key: apiKey, Prefix: apiKeyPrefix},
	})
}
