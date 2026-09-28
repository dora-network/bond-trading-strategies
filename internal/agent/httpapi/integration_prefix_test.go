package httpapi_test

import (
	"net/http"
	"testing"
)

// TestIntegration_RoutesArePrefixedUnderAgent is original-spec test #2:
// agent routes must resolve under /v1/agent/*, not bare /v1/*. A bare
// /v1/sessions request must not reach the agent's session handler.
func TestIntegration_RoutesArePrefixedUnderAgent(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close(t.Context())

	t.Run("bare_v1_sessions_does_not_leak_to_agent", func(t *testing.T) {
		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodGet, ts.srv.URL+"/v1/sessions", nil)
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately no Authorization header.
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		// The agent's session-list handler would answer 200 with a JSON
		// body. 401 (host auth wall), 404 (host mux has no such route),
		// or 405 (method mismatch) all confirm the request did NOT
		// reach the agent.
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusNotFound, http.StatusMethodNotAllowed:
			// expected — request did not reach the agent
		default:
			t.Fatalf("bare /v1/sessions: want 401/404/405, got %d", resp.StatusCode)
		}
	})
}
