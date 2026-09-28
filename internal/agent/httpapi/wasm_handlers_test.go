package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/httpapi"
	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
)

// haltSessionStore is a no-op SessionStore for the halt/resume tests.
type haltSessionStore struct{}

func (haltSessionStore) CreateSession(_ context.Context, _, _, _, _ string) (session.Session, error) {
	return session.Session{}, nil
}

func (haltSessionStore) ListSessions(_ context.Context, _ string) ([]session.Session, error) {
	return nil, nil
}

func (haltSessionStore) GetSession(_ context.Context, _, _ string) (session.Session, []session.StoredMessage, error) {
	return session.Session{}, nil, nil
}
func (haltSessionStore) DeleteSession(_ context.Context, _, _ string) error    { return nil }
func (haltSessionStore) AppendMessage(_ context.Context, _, _, _ string) error { return nil }
func (haltSessionStore) UpdateSummary(_ context.Context, _, _ string) error    { return nil }

// haltConfigStore is a no-op ProviderConfigStore for the halt/resume tests.
type haltConfigStore struct{}

func (haltConfigStore) Set(_ context.Context, _, _, _, _, _ string) error { return nil }
func (haltConfigStore) GetDecrypted(_ context.Context, _, _ string) (string, string, string, error) {
	return "", "", "", nil
}
func (haltConfigStore) Delete(_ context.Context, _, _ string) error { return nil }
func (haltConfigStore) List(_ context.Context, _ string) ([]providerconfig.ListEntry, error) {
	return nil, nil
}

// haltPrincipal is the fixed trader identity the halt/resume tests
// inject via httpapi.PrincipalMiddleware so they can drive the
// Server's Routes() end-to-end without a real auth chain.
var haltPrincipal = httpapi.Principal{UserID: haltUserID, TenantID: "t-halt", Roles: []string{"TRADER"}}

const haltUserID = "00000000-0000-0000-0000-000000000060"

// newHaltServer builds a Server wired with a real safety kernel backed
// by the testcontainers Postgres, and returns it plus the kernel so the
// test can assert on kill-switch state.
func newHaltServer(t *testing.T) (*httptest.Server, *safety.Kernel) {
	t.Helper()
	pool := servertest.StartPostgres(t)
	// Seed the user row the kill-switch + audit writes reference.
	if _, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 't-halt', ARRAY['TRADER'])
		on conflict do nothing`, haltUserID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	kernel, err := safety.NewKernel(pool)
	if err != nil {
		t.Fatalf("NewKernel: %v", err)
	}
	srv := httpapi.New(
		haltSessionStore{}, haltConfigStore{},
		httpapi.WithSafetyKernel(kernel),
	)
	return httptest.NewServer(httpapi.PrincipalMiddleware(haltPrincipal, "test")(srv.Routes())), kernel
}

// authPost builds an authenticated POST to url. The header is
// vestigial (identity comes from PrincipalMiddleware) but kept to
// mirror the production request shape.
func authPost(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "ApiKey test")
	return req
}

// TestHaltEndpoint_SetsKillSwitch drives POST /v1/strategies/{id}/halt
// through the Server's Routes() and asserts the kernel records the halt.
func TestHaltEndpoint_SetsKillSwitch(t *testing.T) {
	srv, kernel := newHaltServer(t)
	t.Cleanup(srv.Close)

	req := authPost(t, srv.URL+"/v1/strategies/strat-1/halt?reason=http%20halt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d want 204", resp.StatusCode)
	}

	halted, reason, err := kernel.IsHalted(t.Context(), haltUserID)
	if err != nil {
		t.Fatalf("IsHalted: %v", err)
	}
	if !halted {
		t.Error("expected halted=true after POST /halt")
	}
	if reason != "http halt" {
		t.Errorf("reason: got %q want %q", reason, "http halt")
	}
}

// TestResumeEndpoint_ClearsKillSwitch drives POST /resume after a halt
// and asserts the kernel clears the kill switch.
func TestResumeEndpoint_ClearsKillSwitch(t *testing.T) {
	srv, kernel := newHaltServer(t)
	t.Cleanup(srv.Close)

	if err := kernel.Halt(t.Context(), haltUserID, "pre-halt"); err != nil {
		t.Fatalf("Halt: %v", err)
	}
	req := authPost(t, srv.URL+"/v1/strategies/strat-1/resume")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d want 204", resp.StatusCode)
	}
	halted, _, err := kernel.IsHalted(t.Context(), haltUserID)
	if err != nil {
		t.Fatalf("IsHalted: %v", err)
	}
	if halted {
		t.Error("expected halted=false after POST /resume")
	}
}

// TestRestartEndpoint_NotWired asserts the restart endpoint returns 501
// when no RestartClearer is wired (the orchestrator is not yet plumbed).
func TestRestartEndpoint_NotWired(t *testing.T) {
	srv, _ := newHaltServer(t)
	t.Cleanup(srv.Close)

	req := authPost(t, srv.URL+"/v1/strategies/strat-1/restart")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d want 501, body=%s", resp.StatusCode, body)
	}
}

// TestHaltEndpoint_NoKernel_503 asserts that without a wired kernel the
// halt endpoint returns 503 (not a panic).
func TestHaltEndpoint_NoKernel_503(t *testing.T) {
	srv := httpapi.New(haltSessionStore{}, haltConfigStore{})
	ts := httptest.NewServer(httpapi.PrincipalMiddleware(haltPrincipal, "test")(srv.Routes()))
	t.Cleanup(ts.Close)

	req := authPost(t, ts.URL+"/v1/strategies/strat-1/halt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503", resp.StatusCode)
	}
}
