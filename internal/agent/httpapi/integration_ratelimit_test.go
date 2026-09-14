package httpapi_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	agenthttpapi "github.com/dora-network/bond-trading-strategies/internal/agent/httpapi"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
)

// TestIntegration_AgentRateLimitReturns429WithRetryAfter is original-spec
// test #3: burn the agent's per-user token bucket (RateLimitPerMin: 2) with
// two requests, assert the third returns 429 with a Retry-After header.
//
// The agent subtree is mounted directly (not behind the host's
// RequireAuth): the spec targets the agent's rate limiter, so a fixed
// Principal is injected server-side via PrincipalMiddleware and the
// limiter keys on that user ID. The full host-auth + limiter chain is
// Task 5's concern.
func TestIntegration_AgentRateLimitReturns429WithRetryAfter(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	if pool == nil {
		t.Skip("DATABASE_URL not set; skipping agent PG test")
	}
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", t.TempDir())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := config.Config{
		CapturePendingSweepInterval: 5 * time.Minute,
		CapturePendingRetention:     24 * time.Hour,
		RateLimitPerMin:             2, // tiny bucket: 3rd request is rejected
		LLMTimeout:                  30 * time.Second,
	}
	encryptionKey := make([]byte, 32)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
	if err != nil {
		cancel()
		pool.Close()
		t.Fatalf("agent wiring: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Close(t.Context())
		cancel()
	})

	principal := agenthttpapi.Principal{UserID: "user-ratelimit-test"}
	handler := agenthttpapi.PrincipalMiddleware(principal, "test-key")(rt.Server.RoutesAt("/v1/agent"))
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	send := func() *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
			srv.URL+"/v1/agent/sessions", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// First 2 requests burn the bucket.
	for i := range 2 {
		r := send()
		r.Body.Close()
		if r.StatusCode == http.StatusTooManyRequests {
			t.Fatalf("request %d: bucket should not be exhausted yet, got 429", i+1)
		}
	}

	// 3rd request is rejected with a Retry-After hint.
	r := send()
	defer r.Body.Close()
	if r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third request: want 429, got %d", r.StatusCode)
	}
	if got := r.Header.Get("Retry-After"); got == "" {
		t.Fatalf("third request: 429 missing Retry-After header")
	} else {
		t.Logf("Retry-After: %s", got)
	}
}

// TestIntegration_AgentRateLimitRefillsAfterDepletion closes the recovery
// gap left by TestIntegration_AgentRateLimitReturns429WithRetryAfter (which
// only covers the depletion path). Burns the bucket, sleeps the refill
// interval (30s for RateLimitPerMin: 2), asserts a subsequent request is
// not 429.
//
// Live-only: the 30s sleep is too slow for hermetic CI, so it is gated on
// AGENT_RATE_LIMIT_REFILL_TEST=1 in addition to DATABASE_URL.
func TestIntegration_AgentRateLimitRefillsAfterDepletion(t *testing.T) {
	if os.Getenv("AGENT_RATE_LIMIT_REFILL_TEST") != "1" {
		t.Skip("set AGENT_RATE_LIMIT_REFILL_TEST=1 to run; 30s sleep is too slow for hermetic CI")
	}
	pool := agenttest.StartPostgres(t)
	if pool == nil {
		t.Skip("DATABASE_URL not set; skipping agent PG test")
	}
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", t.TempDir())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := config.Config{
		CapturePendingSweepInterval: 5 * time.Minute,
		CapturePendingRetention:     24 * time.Hour,
		RateLimitPerMin:             2, // 30s refill interval
		LLMTimeout:                  30 * time.Second,
	}
	encryptionKey := make([]byte, 32)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
	if err != nil {
		cancel()
		pool.Close()
		t.Fatalf("agent wiring: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Close(t.Context())
		cancel()
	})

	principal := agenthttpapi.Principal{UserID: "user-ratelimit-refill-test"}
	handler := agenthttpapi.PrincipalMiddleware(principal, "test-key")(rt.Server.RoutesAt("/v1/agent"))
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	send := func() *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
			srv.URL+"/v1/agent/sessions", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// First 2 requests burn the bucket.
	for i := range 2 {
		r := send()
		r.Body.Close()
		if r.StatusCode == http.StatusTooManyRequests {
			t.Fatalf("request %d: bucket should not be exhausted yet, got 429", i+1)
		}
	}

	// 3rd request is rejected: the bucket is depleted.
	r := send()
	r.Body.Close()
	if r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third request: want 429, got %d", r.StatusCode)
	}

	// Wait out the refill interval (30s) plus a buffer for clock skew.
	time.Sleep(35 * time.Second)

	// 4th request: the bucket must have refilled at least one token.
	r4 := send()
	defer r4.Body.Close()
	if r4.StatusCode == http.StatusTooManyRequests {
		t.Fatal("fourth request: bucket should have refilled after the interval, still got 429")
	}
}
