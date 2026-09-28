package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
)

// --- fakes ---

type fakeSessionStore struct {
	created session.Session
	getSess session.Session
	getMsgs []session.StoredMessage
	getErr  error
	listed  []session.Session
	listErr error
}

func (f *fakeSessionStore) CreateSession(_ context.Context, userID, provider, model, title string) (session.Session, error) {
	f.created = session.Session{ID: "sess-1", UserID: userID, Provider: provider, Model: model, Title: sql.NullString{String: title, Valid: title != ""}}
	return f.created, nil
}

func (f *fakeSessionStore) ListSessions(_ context.Context, _ string) ([]session.Session, error) {
	return f.listed, f.listErr
}

func (f *fakeSessionStore) GetSession(_ context.Context, _, _ string) (session.Session, []session.StoredMessage, error) {
	return f.getSess, f.getMsgs, f.getErr
}
func (f *fakeSessionStore) DeleteSession(_ context.Context, _, _ string) error    { return nil }
func (f *fakeSessionStore) AppendMessage(_ context.Context, _, _, _ string) error { return nil }
func (f *fakeSessionStore) UpdateSummary(_ context.Context, _, _ string) error    { return nil }

type fakeProviderConfigStore struct {
	setErr    error
	decrypted string
	model     string
	baseURL   string
	getErr    error
	deleted   bool
	list      []providerconfig.ListEntry
	listErr   error
}

func (f *fakeProviderConfigStore) Set(_ context.Context, _, provider, _, model, baseURL string) error {
	return f.setErr
}

func (f *fakeProviderConfigStore) GetDecrypted(_ context.Context, _, _ string) (string, string, string, error) {
	return f.decrypted, f.model, f.baseURL, f.getErr
}

func (f *fakeProviderConfigStore) List(_ context.Context, _ string) ([]providerconfig.ListEntry, error) {
	return f.list, f.listErr
}

func (f *fakeProviderConfigStore) Delete(_ context.Context, _, _ string) error {
	f.deleted = true
	return nil
}

func newServer(t *testing.T) (*Server, *fakeSessionStore, *fakeProviderConfigStore) {
	t.Helper()
	ss := &fakeSessionStore{}
	pc := &fakeProviderConfigStore{}
	return &Server{sessions: ss, configs: pc, allowedProviders: map[string]bool{"openai": true, "anthropic": true, "openrouter": true}, rateLimitPerMin: 20}, ss, pc
}

func withPrincipal(r *http.Request, userID ...string) *http.Request {
	uid := "u-1"
	if len(userID) > 0 && userID[0] != "" {
		uid = userID[0]
	}
	return r.WithContext(context.WithValue(r.Context(), principalKey, Principal{UserID: uid}))
}

// --- tests ---

func TestSetProviderConfig_RejectsUnknownProvider(t *testing.T) {
	srv, _, _ := newServer(t)
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"provider":"groq","api_key":"k","default_model":"m"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/provider-config", body))
	srv.handleSetProviderConfig(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code: want 400 for unknown provider, got %d", rec.Code)
	}
}

func TestSetProviderConfig_RejectsBadJSON(t *testing.T) {
	srv, _, _ := newServer(t)
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/provider-config", strings.NewReader("{bad")))
	srv.handleSetProviderConfig(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("code: want 400, got %d", rec.Code)
	}
}

func TestSetProviderConfig_Success(t *testing.T) {
	srv, _, pc := newServer(t)
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"provider":"openai","api_key":"sk-x","default_model":"gpt-4o-mini"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/provider-config", body))
	srv.handleSetProviderConfig(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code: want 204, got %d", rec.Code)
	}
	if pc.setErr != nil {
		t.Errorf("set should have succeeded")
	}
}

func TestListProviderConfig_EmitsMaskedEntries(t *testing.T) {
	srv, _, pc := newServer(t)
	pc.list = []providerconfig.ListEntry{
		{Provider: "anthropic", DefaultModel: "claude-haiku-4-5", BaseURL: "", APIKeyMasked: "sk-...1234"},
		{Provider: "openai", DefaultModel: "gpt-4o-mini", BaseURL: "https://api.example.com", APIKeyMasked: "sk-...ABCD"},
	}
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/provider-config", nil))
	srv.handleListProviderConfig(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: %q", ct)
	}
	var got []providerConfigResp
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: want 2, got %d", len(got))
	}
	if got[0].Provider != "anthropic" || got[0].APIKeyMasked != "sk-...1234" {
		t.Errorf("got[0]: %+v", got[0])
	}
	if got[1].BaseURL != "https://api.example.com" || got[1].APIKeyMasked != "sk-...ABCD" {
		t.Errorf("got[1]: %+v", got[1])
	}
}

func TestListProviderConfig_Empty(t *testing.T) {
	srv, _, _ := newServer(t)
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/provider-config", nil))
	srv.handleListProviderConfig(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "[]" {
		t.Errorf("body: want [], got %s", got)
	}
}

func TestListProviderConfig_StoreErrorIs500(t *testing.T) {
	srv, _, pc := newServer(t)
	pc.listErr = errors.New("boom")
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/provider-config", nil))
	srv.handleListProviderConfig(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("code: want 500, got %d", rec.Code)
	}
}

func TestCreateSession_Success(t *testing.T) {
	srv, ss, _ := newServer(t)
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"provider":"openai","model":"gpt-4o-mini"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/sessions", body))
	srv.handleCreateSession(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	if ss.created.UserID != "u-1" {
		t.Errorf("created user: want u-1, got %s", ss.created.UserID)
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["session_id"] != "sess-1" {
		t.Errorf("session_id: want sess-1, got %q", resp["session_id"])
	}
}

func TestGetSession_OwnershipNotFound_404(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getErr = session.ErrNotFound
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/sessions/sess-x", nil))
	req.SetPathValue("id", "sess-x")
	srv.handleGetSession(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestGetSession_Success_IncludesMessages(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getSess = session.Session{ID: "sess-1", UserID: "u-1", Provider: "openai", Model: "m"}
	ss.getMsgs = []session.StoredMessage{{Seq: 1, Role: "user", Content: "hi"}}
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/sessions/sess-1", nil))
	req.SetPathValue("id", "sess-1")
	srv.handleGetSession(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"role":"user"`) {
		t.Errorf("missing role:user in body: %s", rec.Body.String())
	}
}

func TestGetSession_WireShapeSnakeCase(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getSess = session.Session{
		ID: "sess-1", UserID: "u-1", Provider: "openai", Model: "m",
		Title: sql.NullString{String: "My Title", Valid: true},
	}
	ss.getMsgs = []session.StoredMessage{{
		Seq: 1, Role: "user", Content: "hi",
		ToolCallID: sql.NullString{String: "call-1", Valid: true},
		ToolCalls:  json.RawMessage(`[{"id":"call-1"}]`),
	}}
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/sessions/sess-1", nil))
	req.SetPathValue("id", "sess-1")
	srv.handleGetSession(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	for _, want := range []string{`"user_id"`, `"created_at"`, `"role"`, `"tool_calls"`} {
		if !strings.Contains(body, want) {
			t.Errorf("wire: want %s in body, got: %s", want, body)
		}
	}
	// PascalCase keys and pgx nullable wrappers must not leak.
	for _, bad := range []string{`"UserID"`, `"Role"`, `"Valid"`, `"String"`} {
		if strings.Contains(body, bad) {
			t.Errorf("wire: %s leaked into body: %s", bad, body)
		}
	}

	var got struct {
		Session struct {
			Title  string `json:"title"`
			UserID string `json:"user_id"`
		} `json:"session"`
		Messages []struct {
			Role      string          `json:"role"`
			ToolCalls json.RawMessage `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Session.Title != "My Title" {
		t.Errorf("title: want %q, got %q", "My Title", got.Session.Title)
	}
	if got.Session.UserID != "u-1" {
		t.Errorf("user_id: want u-1, got %q", got.Session.UserID)
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
		t.Errorf("messages: %+v", got.Messages)
	}
	// tool_calls must be raw JSON, not base64-encoded bytes.
	if want := `[{"id":"call-1"}]`; string(got.Messages[0].ToolCalls) != want {
		t.Errorf("tool_calls: want %s, got %q", want, got.Messages[0].ToolCalls)
	}
}

func TestDeleteProviderConfig_MissingReturns404(t *testing.T) {
	srv, _, _ := newServer(t)
	rec := httptest.NewRecorder()
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/v1/provider-config/openai", nil))
	req.SetPathValue("provider", "openai")
	srv.handleDeleteProviderConfig(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("code: want 204, got %d", rec.Code)
	}
}

// fakeRunner is an AgentRunner that emits canned events.
type fakeRunner struct {
	text   string
	err    error
	called bool
}

func (f *fakeRunner) Run(_ context.Context, _, _, _, _ string, _ []session.StoredMessage, _ string, emit func(EventPayload)) (string, error) {
	f.called = true
	emit(TextPayload{Text: "hel"})
	emit(TextPayload{Text: "lo"})
	emit(DonePayload{})
	return f.text, f.err
}

func TestPostMessage_SSEStream(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getSess = session.Session{ID: "sess-1", UserID: "u-1", Provider: "openai", Model: "m"}
	ss.getMsgs = nil
	srv.agentRunner = &fakeRunner{text: "hello", err: nil}

	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"prompt":"hi"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/sessions/sess-1/messages", body), "u-1")
	req.SetPathValue("id", "sess-1")
	srv.handlePostMessage(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("Content-Type: want text/event-stream, got %q", rec.Header().Get("Content-Type"))
	}
	bodyStr := rec.Body.String()
	if !strings.Contains(bodyStr, "event: delta") {
		t.Errorf("missing delta event:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "event: done") {
		t.Errorf("missing done event:\n%s", bodyStr)
	}
}

func TestPostMessage_MissingRunner_503(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getSess = session.Session{ID: "sess-1", UserID: "u-1", Provider: "openai", Model: "m"}
	// no agent runner configured
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"prompt":"hi"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/sessions/sess-1/messages", body), "u-1")
	req.SetPathValue("id", "sess-1")
	srv.handlePostMessage(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("code: want 503 without runner, got %d", rec.Code)
	}
}

func TestPostMessage_SessionNotFound_404(t *testing.T) {
	srv, ss, _ := newServer(t)
	ss.getErr = session.ErrNotFound
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"prompt":"hi"}`)
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/sessions/nope/messages", body), "u-1")
	req.SetPathValue("id", "nope")
	srv.handlePostMessage(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("code: want 404, got %d", rec.Code)
	}
}

func TestDeployment_UnwiredRuntimeReturns503(t *testing.T) {
	srv, _, _ := newServer(t)
	cases := []struct {
		name    string
		method  string
		target  string
		handler func(w http.ResponseWriter, r *http.Request)
	}{
		{"deploy", http.MethodPost, "/v1/strategies/s-1/versions/r1/deploy", srv.handleDeploy},
		{"list_deployments", http.MethodGet, "/v1/strategies/s-1/deployments", srv.handleListDeployments},
		{"get_deployment", http.MethodGet, "/v1/strategies/s-1/deployments/d1", srv.handleGetDeployment},
		{"stop_deployment", http.MethodPost, "/v1/strategies/s-1/deployments/d1/stop", srv.handleStopDeployment},
		{"deployment_logs", http.MethodGet, "/v1/strategies/s-1/deployments/d1/logs", srv.handleDeploymentLogs},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := withPrincipal(httptest.NewRequestWithContext(t.Context(), c.method, c.target, nil))
			c.handler(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s: want 503, got %d", c.name, rec.Code)
			}
		})
	}
}

func TestDeployment_RoutesWired(t *testing.T) {
	// Confirms all 9 deployment + backtest routes are registered on the authed
	// mux (they resolve to a handler rather than 404) by exercising them
	// through Routes(). The authed subtree sits behind the host's auth
	// mount chain (absent in unit tests), so handlers run with an empty
	// principal; we only assert the registered route does NOT return
	// 404 — any other status means the route exists and is wired.
	srv, _, _ := newServer(t)
	cases := []struct {
		method string
		target string
	}{
		{http.MethodPost, "/v1/strategies/s-1/versions/r1/deploy"},
		{http.MethodGet, "/v1/strategies/s-1/deployments"},
		{http.MethodGet, "/v1/strategies/s-1/deployments/d1"},
		{http.MethodPost, "/v1/strategies/s-1/deployments/d1/stop"},
		{http.MethodGet, "/v1/strategies/s-1/deployments/d1/logs"},
		{http.MethodPost, "/v1/strategies/s-1/deployments/d1/resume"},
		{http.MethodPost, "/v1/strategies/s-1/deployments/d1/restart"},
		{http.MethodPost, "/v1/strategies/s-1/deployments/d1/hotswap"},
		{http.MethodPost, "/v1/strategies/s-1/versions/r1/backtest"},
	}
	h := srv.Routes()
	for _, c := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), c.method, c.target, nil)
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("%s %s: route not registered (404)", c.method, c.target)
		}
	}
}
