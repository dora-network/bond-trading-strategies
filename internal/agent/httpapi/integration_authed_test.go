package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestIntegration_AgentRoutesReturnExpectedResponsesWhenAuthed closes the
// coverage gap left by the 401-only tests: with a resolver that accepts the
// request's API key, the agent's own handlers must answer 200 with a
// non-empty valid JSON body. GET /v1/agent/sessions returns a JSON array
// (handleListSessions marshals []sessionResp); GET /v1/agent/strategies
// Note: RoutesAt("/v1/agent") strips the leading "/v1" from the internal
// route table, so the wire path is the spec's "/v1/agent/sessions" (not
// "/v1/agent/v1/sessions" as a naive basePath+path concat would produce).
func TestIntegration_AgentRoutesReturnExpectedResponsesWhenAuthed(t *testing.T) {
	ts := newTestServerWithAuthResolver(t, func(ctx context.Context) (string, error) {
		// requireAuth has already parsed and validated the Authorization
		// header; a fixed user ID keeps the agent's PG queries scoped.
		return "00000000-0000-0000-0000-000000000051", nil
	})
	defer ts.Close(t.Context())

	cases := []struct {
		name string
		path string
	}{
		{"list sessions", "/v1/agent/sessions"},
		{"list strategies", "/v1/agent/strategies"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(),
				http.MethodGet, ts.srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "ApiKey test-key")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s: want 200, got %d", tc.path, resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("%s: read body: %v", tc.path, err)
			}
			if len(body) == 0 {
				t.Fatalf("%s: empty body", tc.path)
			}
			if !json.Valid(body) {
				t.Fatalf("%s: invalid JSON body: %s", tc.path, body)
			}
		})
	}
}
