package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestIntegration_AgentRoutesRequireAuth is original-spec test #1: every
// agent route must answer 401 when called without an Authorization header.
func TestIntegration_AgentRoutesRequireAuth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close(t.Context())

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"list sessions", http.MethodGet, "/v1/agent/sessions", ""},
		{"list strategies", http.MethodGet, "/v1/agent/strategies", ""},
		{"create session", http.MethodPost, "/v1/agent/sessions", `{}`},
		{"run backtest", http.MethodPost, "/v1/agent/strategies/00000000-0000-0000-0000-000000000000/versions/1/backtest", `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req, err := http.NewRequestWithContext(t.Context(), tc.method, ts.srv.URL+tc.path, body)
			if err != nil {
				t.Fatal(err)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			// Deliberately no Authorization header.
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s %s: want 401, got %d", tc.method, tc.path, resp.StatusCode)
			}
		})
	}
}
