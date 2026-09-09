package httpapi_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	agenthttpapi "github.com/dora-network/bond-trading-strategies/internal/agent/httpapi"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wiring"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// testServer boots the strategy-server's full agent HTTP chain
// (RequireAuth + principal bridge + RoutesAt("/v1/agent")) plus the agent
// runtime against a real Postgres, gated on DATABASE_URL per host
// convention. Tasks 2-5 of the binary-verification plan build on it.
type testServer struct {
	srv     *httptest.Server
	runtime *wiring.Runtime
	pool    *pgxpool.Pool
	cancel  context.CancelFunc
}

func (ts *testServer) Close(ctx context.Context) {
	if ts.srv != nil {
		ts.srv.Close()
	}
	if ts.runtime != nil {
		_ = ts.runtime.Close(ctx)
	}
	if ts.pool != nil {
		ts.pool.Close()
	}
	if ts.cancel != nil {
		ts.cancel()
	}
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	if pool == nil {
		t.Skip("DATABASE_URL not set; skipping agent PG test")
	}

	// Wire requires a writable WASM artifact root; a per-test temp dir
	// satisfies it without touching the operator's configured path.
	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", t.TempDir())

	ctx, cancel := context.WithCancel(t.Context())

	cfg := config.Config{
		// Zero-valued intervals panic the capture-pending janitor's
		// ticker; the production defaults keep it quiet and harmless.
		CapturePendingSweepInterval: 5 * time.Minute,
		CapturePendingRetention:     24 * time.Hour,
		RateLimitPerMin:             1000, // effectively disabled for tests
		LLMTimeout:                  30 * time.Second,
		// Other fields zero-valued; the LLM/tool paths aren't exercised.
	}
	encryptionKey := make([]byte, 32)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
	if err != nil {
		cancel()
		pool.Close()
		t.Fatalf("agent wiring: %v", err)
	}

	handler := strategyhttp.RequireAuth(agentPrincipalBridge(rt))
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
		_ = rt.Close(t.Context())
		cancel()
	})

	return &testServer{srv: srv, runtime: rt, pool: pool, cancel: cancel}
}

// newTestServerWithAuthResolver is the same as newTestServer but lets the
// test inject a custom Dora user resolver, so successful-auth requests can
// be driven through the host's requireAuth gate without hitting the real
// Dora API. override (nil = defaults) mutates the agent config before
// wiring; the mount-order test uses it to shrink the per-user bucket.
func newTestServerWithAuthResolver(
	t *testing.T,
	resolveUserID func(context.Context) (string, error),
	override func(*config.Config),
) *testServer {
	t.Helper()
	pool := agenttest.StartPostgres(t)
	if pool == nil {
		t.Skip("DATABASE_URL not set; skipping agent PG test")
	}

	t.Setenv("AGENT_WASM_ARTIFACT_ROOT", t.TempDir())

	ctx, cancel := context.WithCancel(t.Context())

	cfg := config.Config{
		CapturePendingSweepInterval: 5 * time.Minute,
		CapturePendingRetention:     24 * time.Hour,
		RateLimitPerMin:             1000, // effectively disabled for tests
		LLMTimeout:                  30 * time.Second,
	}
	if override != nil {
		override(&cfg)
	}
	encryptionKey := make([]byte, 32)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	rt, err := wiring.Wire(ctx, pool, encryptionKey, cfg, log)
	if err != nil {
		cancel()
		pool.Close()
		t.Fatalf("agent wiring: %v", err)
	}

	handler := strategyhttp.RequireAuthWithResolver(resolveUserID, agentPrincipalBridge(rt))
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
		_ = rt.Close(t.Context())
		cancel()
	})

	return &testServer{srv: srv, runtime: rt, pool: pool, cancel: cancel}
}

// agentPrincipalBridge mirrors cmd/strategy-server/main.go's bridge from
// authctx.AuthInfo to PrincipalMiddleware: after RequireAuth succeeds, the
// request context carries the verified Dora user ID and parsed credentials;
// re-inject them as the httpapi Principal.
func agentPrincipalBridge(rt *wiring.Runtime) http.Handler {
	inner := rt.Server.RoutesAt("/v1/agent")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := strategyhttp.DoraUserIDFromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorised", http.StatusUnauthorized)
			return
		}
		info, _ := authctx.AuthInfoFromContext(r.Context())
		var tenantID, apiKey string
		if info != nil {
			tenantID, apiKey = info.TenantID, info.APIKey
		}
		p := agenthttpapi.Principal{UserID: userID, TenantID: tenantID}
		agenthttpapi.PrincipalMiddleware(p, apiKey)(inner).ServeHTTP(w, r)
	})
}

// TestTestServerBoots is the fixture's own smoke check: the full chain
// (RequireAuth over the agent mount) answers, and unauthenticated agent
// requests are rejected with 401. Task 2 expands this into the per-route
// auth matrix.
func TestTestServerBoots(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close(t.Context())

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.srv.URL+"/v1/agent/sessions", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get sessions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /v1/agent/sessions: got %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
