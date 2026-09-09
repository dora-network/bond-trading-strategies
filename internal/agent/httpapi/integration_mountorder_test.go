package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/ratelimit"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// TestIntegration_AgentMountOrderPassesBothRateLimiters is original-spec
// test #4: "Mount order is correct. A single request goes through both
// rate limiters in order."
//
// The chain mirrors cmd/strategy-server/main.go:
//
//	host ratelimit.Middleware -> requireAuth -> principal bridge -> agent
//	subtree (agent's per-user RateLimitMiddleware inside RoutesAt).
//
// The host's Limiter exposes no counters, so the test instruments the seam
// immediately downstream of it (passedHost) — every request the counter
// sees was admitted by the host's limiters. The 5th request is then
// rejected by the host itself (distinctive RateLimit-* headers), proving
// the host's limiter is armed and outermost; the 3rd is rejected by the
// agent's per-user limiter (no RateLimit-* headers), proving the agent's
// limiter is reachable inside the mount.
func TestIntegration_AgentMountOrderPassesBothRateLimiters(t *testing.T) {
	resolveUserID := func(context.Context) (string, error) {
		return "00000000-0000-0000-0000-000000000061", nil
	}
	ts := newTestServerWithAuthResolver(t, resolveUserID,
		func(c *config.Config) { c.RateLimitPerMin = 2 })
	defer ts.Close(t.Context())

	// Host limiter: every scope generous except the per-user read tier
	// (keyed by the Authorization header), which admits exactly 4 with
	// no meaningful refill. GETs are reads.
	hostRL := ratelimit.NewLimiter(ratelimit.Config{
		Enabled: true,
		Global:  ratelimit.TierConfig{RPS: 1000, Burst: 1000},
		IP:      ratelimit.TierConfig{RPS: 1000, Burst: 1000},
		Read:    ratelimit.TierConfig{RPS: 1e-9, Burst: 4},
		Write:   ratelimit.TierConfig{RPS: 1000, Burst: 1000},
	}, nil)
	t.Cleanup(hostRL.Stop)

	var passedHost atomic.Int32
	counting := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			passedHost.Add(1)
			next.ServeHTTP(w, r)
		})
	}

	srv := httptest.NewServer(hostRL.Middleware(counting(
		strategyhttp.RequireAuthWithResolver(resolveUserID, agentPrincipalBridge(ts.runtime)))))
	t.Cleanup(srv.Close)

	send := func() *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodGet, srv.URL+"/v1/agent/sessions", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "ApiKey test-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Requests 1-2: both limiters allow.
	for i := range 2 {
		r := send()
		r.Body.Close()
		if r.StatusCode == http.StatusTooManyRequests {
			t.Fatalf("request %d: no limiter should have fired yet, got 429", i+1)
		}
	}

	// Request 3: the agent's per-user limiter rejects. The host must
	// have admitted it first — that is the mount-order assertion.
	r3 := send()
	r3.Body.Close()
	if r3.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third request: want 429 from the agent's per-user limiter, got %d", r3.StatusCode)
	}
	if got := r3.Header.Get("RateLimit-Limit"); got != "" {
		t.Fatalf("third request: 429 carries the host's RateLimit-Limit header (%q); it should come from the agent's limiter", got)
	}
	if got := passedHost.Load(); got != 3 {
		t.Fatalf("after 3 requests: the host's limiter admitted %d, want 3 (it must run before the agent's limiter)", got)
	}

	// Request 4: still under the host's burst of 4, so the host admits
	// it and the agent's (still-empty) bucket rejects again.
	r4 := send()
	r4.Body.Close()
	if r4.StatusCode != http.StatusTooManyRequests || r4.Header.Get("RateLimit-Limit") != "" {
		t.Fatalf("fourth request: want agent-limiter 429 without RateLimit-Limit, got %d", r4.StatusCode)
	}

	// Request 5: the host's per-user read bucket is exhausted — the
	// host itself rejects, with its distinctive RateLimit-* headers and
	// without the request reaching the agent mount. This proves the
	// host's limiter was counting every earlier request.
	r5 := send()
	r5.Body.Close()
	if r5.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("fifth request: want 429 from the host's limiter, got %d", r5.StatusCode)
	}
	if got := r5.Header.Get("RateLimit-Limit"); got == "" {
		t.Fatal("fifth request: host-limiter 429 must carry the RateLimit-Limit header")
	}
	if got := passedHost.Load(); got != 4 {
		t.Fatalf("after 5 requests: the host's limiter admitted %d, want 4", got)
	}
}
