package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

// requireAuthWithRecording wraps strategyhttp.RequireAuthWithResolver
// with a sink handler that captures the authctx.AuthInfo that landed
// on the request context. Used by the tenant-precedence tests below
// to assert which TenantID value requireAuth ultimately stored.
func requireAuthWithRecording(
	t *testing.T,
	resolveUserID func(context.Context) (string, string, error),
	rec *authctx.AuthInfo,
) http.Handler {
	t.Helper()
	return strategyhttp.RequireAuthWithResolver(
		resolveUserID,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if info, ok := authctx.AuthInfoFromContext(r.Context()); ok {
				*rec = *info
			}
			w.WriteHeader(http.StatusOK)
		}),
	)
}

// TestRequireAuth_FillsTenantIDFromResolverWhenHeaderAbsent pins the
// behaviour that bridges the host's auth chain to the agent's
// EnsurePrincipal: when the chatui does not send a tenant-id header,
// requireAuth populates AuthInfo.TenantID from the DORA user record
// (resolved by the resolver via /v1/user/self). Without this, the
// agent's agent.users.tenant_id stays empty and per-tenant queries
// silently degrade.
func TestRequireAuth_FillsTenantIDFromResolverWhenHeaderAbsent(t *testing.T) {
	var got authctx.AuthInfo
	h := requireAuthWithRecording(
		t,
		func(context.Context) (string, string, error) {
			return "user-123", "tenant-dora", nil
		},
		&got,
	)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "ApiKey k-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rec.Code)
	}
	if got.TenantID != "tenant-dora" {
		t.Errorf("AuthInfo.TenantID: want %q (from resolver), got %q", "tenant-dora", got.TenantID)
	}
	if got.APIKey != "k-1" {
		t.Errorf("AuthInfo.APIKey: want %q, got %q", "k-1", got.APIKey)
	}
}

// TestRequireAuth_HeaderTenantWinsOverResolver pins the precedence
// rule: a client that explicitly claims a tenant via the tenant-id
// header overrides the authoritative tenant bound to the user's DORA
// record. Matches the original dora-agent auth.Validate semantics —
// an explicit claim always wins.
func TestRequireAuth_HeaderTenantWinsOverResolver(t *testing.T) {
	var got authctx.AuthInfo
	h := requireAuthWithRecording(
		t,
		func(context.Context) (string, string, error) {
			return "user-123", "tenant-from-dora", nil
		},
		&got,
	)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "ApiKey k-1")
	req.Header.Set(strategyhttp.TenantIDHeader, "tenant-explicit")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", rec.Code)
	}
	if got.TenantID != "tenant-explicit" {
		t.Errorf("AuthInfo.TenantID: want %q (header), got %q", "tenant-explicit", got.TenantID)
	}
}
