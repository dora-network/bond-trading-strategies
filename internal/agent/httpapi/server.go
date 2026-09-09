// Package httpapi exposes the agent HTTP API: provider-config CRUD,
// session/message CRUD, and SSE streaming of LLM responses.
package httpapi

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/deployment"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orchestrator"
	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// SessionStore is the session/message subset the API depends on.
type SessionStore interface {
	CreateSession(ctx context.Context, userID, provider, model, title string) (session.Session, error)
	ListSessions(ctx context.Context, userID string) ([]session.Session, error)
	GetSession(ctx context.Context, userID, sessionID string) (session.Session, []session.StoredMessage, error)
	DeleteSession(ctx context.Context, userID, sessionID string) error
	AppendMessage(ctx context.Context, sessionID, role, content string) error
	UpdateSummary(ctx context.Context, sessionID, summary string) error
}

// ProviderConfigStore is the provider-config subset the API depends on.
type ProviderConfigStore interface {
	Set(ctx context.Context, userID, provider, apiKey, defaultModel, baseURL string) error
	GetDecrypted(ctx context.Context, userID, provider string) (apiKey, defaultModel, baseURL string, err error)
	List(ctx context.Context, userID string) ([]providerconfig.ListEntry, error)
	Delete(ctx context.Context, userID, provider string) error
}

type Server struct {
	sessions      SessionStore
	configs       ProviderConfigStore
	strategies    strategies.Store
	backtestOrch  *backtest.Orchestrator // POST handler uses Submit (wasm-only) + Cancel for the per-job cancel goroutine
	backtestStore backtest.Store         // GET / list / cancel handlers
	wasmStarter   *backtest.WasmStarter  // POST handler uses for go-wasm
	deployStore   deployment.Store       // GET / list / logs handlers
	liveOrch      LiveOrchestrator       // POST deploy/stop/resume/restart/hotswap
	audit         AuditWriter

	agentRunner      AgentRunner
	providerFactory  ProviderFactory  // for summarization before agent turns
	modelCaps        config.ModelCaps // for context-window threshold
	allowedProviders map[string]bool
	rateLimitPerMin  int
	corsOrigins      map[string]bool // exact-match CORS allow-list. nil = CORS disabled.
	// safetyKernel backs the halt/resume endpoints. Nil => 503.
	safetyKernel SafetyController
	// restartClearer backs the restart endpoint. Nil => 501.
	restartClearer RestartClearer
}

// SafetyController is the safety-kernel subset the halt/resume
// endpoints need. *safety.Kernel satisfies it structurally.
type SafetyController interface {
	Halt(ctx context.Context, userID, reason string) error
	Resume(ctx context.Context, userID string) error
	IsHalted(ctx context.Context, userID string) (bool, string, error)
}

// LiveOrchestrator is the live-deployment lifecycle subset the
// deployment handlers reach. *orchestrator.Orchestrator satisfies it
// structurally; tests inject a fake to avoid standing up the wasm
// runtime + wsbroker.
type LiveOrchestrator interface {
	Deploy(ctx context.Context, cfg orchestrator.DeployConfig) error
	Stop(ctx context.Context, deploymentID, userID string) error
	Resume(ctx context.Context, deploymentID, userID, doraAPIKey string) error
	HotSwap(
		ctx context.Context,
		deploymentID, userID, newRevision string,
		newWasmRef, newManifestHash string,
		doraAPIKey, orderBookID string,
	) error
}

// AgentRunner runs one assistant turn, emitting events. Implemented in Task 6.
type AgentRunner interface {
	Run(
		ctx context.Context,
		userID, sessionID, provider, model string,
		messages []session.StoredMessage,
		prompt string,
		emit func(EventPayload),
	) (string, error)
}

// EventPayload is a closed set of events the handler can render to SSE.
type EventPayload interface{ isPayload() }

// Option configures the Server.
type Option func(*Server)

// WithAgentRunner sets the agent runner (Task 6).
func WithAgentRunner(r AgentRunner) Option { return func(s *Server) { s.agentRunner = r } }

// WithModelCaps sets the model capability table used to compute the
// context-window compaction threshold.
func WithModelCaps(c config.ModelCaps) Option { return func(s *Server) { s.modelCaps = c } }

// WithProviderFactory sets the provider factory used for history summarization
// before agent turns. When unset, history compaction is skipped.
func WithProviderFactory(f ProviderFactory) Option { return func(s *Server) { s.providerFactory = f } }

// WithRateLimit overrides the default 20 requests/min/user.
func WithRateLimit(perMin int) Option { return func(s *Server) { s.rateLimitPerMin = perMin } }

// WithStrategies sets the strategy version store. When unset, strategy/version
// endpoints return 503.
func WithStrategies(store strategies.Store) Option {
	return func(s *Server) { s.strategies = store }
}

// WithBacktest wires the backtest orchestrator (POST) and store (GET/list/cancel).
// When unset, backtest endpoints return 503.
func WithBacktest(orch *backtest.Orchestrator, store backtest.Store) Option {
	return func(s *Server) { s.backtestOrch = orch; s.backtestStore = store }
}

// WithWasmStarter wires the WASM backtest runner. When unset, go-wasm
// versions return 503.
func WithWasmStarter(starter *backtest.WasmStarter) Option {
	return func(s *Server) { s.wasmStarter = starter }
}

// WithDeploymentStore wires the deployment store (GET / list / logs).
// When unset, deployment read endpoints return 503.
func WithDeploymentStore(store deployment.Store) Option {
	return func(s *Server) { s.deployStore = store }
}

// WithOrchestrator wires the live-deployment orchestrator. Required for
// POST deploy/stop/resume/restart/hotswap; when unset those endpoints
func WithOrchestrator(o LiveOrchestrator) Option {
	return func(s *Server) { s.liveOrch = o }
}

// WithAuditWriter sets the audit writer for strategy rollback/save actions.
func WithAuditWriter(a AuditWriter) Option { return func(s *Server) { s.audit = a } }

// WithSafetyKernel wires the safety kernel backing the halt/resume
// endpoints. When unset, those endpoints return 503.
func WithSafetyKernel(k SafetyController) Option {
	return func(s *Server) { s.safetyKernel = k }
}

// WithRestartClearer wires the restart-budget clearer backing the
// restart endpoint. When unset, the endpoint returns 501.
func WithRestartClearer(c RestartClearer) Option {
	return func(s *Server) { s.restartClearer = c }
}

// defaultRateLimitPerMin is the per-user requests/minute floor used
// when WithRateLimit is not supplied.
const defaultRateLimitPerMin = 20

func New(sessions SessionStore, configs ProviderConfigStore, opts ...Option) *Server {
	s := &Server{
		sessions:         sessions,
		configs:          configs,
		allowedProviders: map[string]bool{"openai": true, "anthropic": true, "openrouter": true},
		rateLimitPerMin:  defaultRateLimitPerMin, // override via WithRateLimit
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// RoutesAt registers every agent route under the given basePath.
// basePath is the production mount prefix (e.g. "/v1/agent"); pass ""
// for unit tests. healthz is public; everything else sits behind the
// host's requireAuth mount chain plus this package's request-ID +
// per-user rate-limit middleware.
func (s *Server) RoutesAt(basePath string) http.Handler {
	mux := http.NewServeMux()
	s.registerRoutes(mux, basePath)
	return corsMiddleware(mux, s.corsOrigins)
}

// Routes is the legacy entry point; equivalent to RoutesAt("").
// Kept for unit tests that don't care about the production mount prefix.
func (s *Server) Routes() http.Handler {
	return s.RoutesAt("")
}

func (s *Server) registerRoutes(mux *http.ServeMux, basePath string) {
	mux.HandleFunc("GET "+basePath+"/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Every authed pattern, registered under basePath + original path.
	// The host's requireAuth chain (L8 mount) authenticates before these
	// run; the per-user rate limiter still applies here.
	routes := []struct {
		pattern string // "METHOD /path"
		h       http.HandlerFunc
	}{
		{"POST /v1/provider-config", s.handleSetProviderConfig},
		{"GET /v1/provider-config", s.handleListProviderConfig},
		{"DELETE /v1/provider-config/{provider}", s.handleDeleteProviderConfig},
		{"POST /v1/sessions", s.handleCreateSession},
		{"GET /v1/sessions", s.handleListSessions},
		{"GET /v1/sessions/{id}", s.handleGetSession},
		{"DELETE /v1/sessions/{id}", s.handleDeleteSession},
		{"POST /v1/sessions/{id}/messages", s.handlePostMessage},
		{"GET /v1/strategies", s.handleListStrategies},
		{"GET /v1/strategies/{id}", s.handleGetStrategy},
		{"GET /v1/strategies/{id}/versions", s.handleListVersions},
		{"GET /v1/strategies/{id}/versions/{revision}", s.handleGetVersion},
		// WASM runtime control (Plan 4): halt/resume drive the safety
		// kernel's per-user kill switch; restart clears a strategy's
		// restart budget via the orchestrator (501 until wired).
		{"POST /v1/strategies/{id}/halt", s.haltStrategy},
		{"POST /v1/strategies/{id}/resume", s.resumeStrategy},
		{"POST /v1/strategies/{id}/restart", s.restartStrategy},
		{"POST /v1/strategies/{id}/rollback", s.handleRollback},
		{"POST /v1/sessions/{id}/save", s.handleSaveSession},
		// Deployments + backtests. See job_handlers.go.
		{"POST /v1/strategies/{id}/versions/{revision}/deploy", s.handleDeploy},
		{"GET /v1/strategies/{id}/deployments", s.handleListDeployments},
		{"GET /v1/strategies/{id}/deployments/{deployment_id}", s.handleGetDeployment},
		{"POST /v1/strategies/{id}/deployments/{deployment_id}/stop", s.handleStopDeployment},
		{"GET /v1/strategies/{id}/deployments/{deployment_id}/logs", s.handleDeploymentLogs},
		{"POST /v1/strategies/{id}/deployments/{deployment_id}/resume", s.handleResumeDeployment},
		{"POST /v1/strategies/{id}/deployments/{deployment_id}/restart", s.handleRestartDeployment},
		{"POST /v1/strategies/{id}/deployments/{deployment_id}/hotswap", s.handleHotSwapDeployment},
		{"POST /v1/strategies/{id}/versions/{revision}/backtest", s.handleBacktest},
		{"GET /v1/strategies/{id}/backtests", s.handleListBacktests},
		{"GET /v1/strategies/{id}/backtests/{backtest_id}", s.handleGetBacktest},
		{"POST /v1/strategies/{id}/backtests/{backtest_id}/cancel", s.handleCancelBacktest},
	}
	authed := http.NewServeMux()
	for _, rt := range routes {
		method, path, _ := strings.Cut(rt.pattern, " ")
		authed.HandleFunc(method+" "+basePath+path, rt.h)
	}

	// Chain: request-ID+logging outer, per-user rate-limit inner, on the
	// authed subtree.
	limiter := newRateLimiter(s.rateLimitPerMin, time.Minute/time.Duration(s.rateLimitPerMin))
	authedRL := RateLimitMiddleware(limiter)(authed)
	mux.Handle(basePath+"/v1/", RequestIDMiddleware(authedRL))
}

// corsMiddleware sets CORS headers per the server's allow-list. Empty
// allow-list (or nil) = no CORS headers (production tight). "*" in the
// allow-list = any origin (local dev only). Otherwise the request's
// Origin header must match exactly. CORS only relaxes the browser's
// same-origin gate; auth and rate limiting still apply.
func corsMiddleware(next http.Handler, allow map[string]bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(allow) == 0 {
			next.ServeHTTP(w, r)
			return
		}
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		var allowedOrigin string
		if allow["*"] {
			allowedOrigin = "*"
		} else if allow[origin] || originMatchesAllow(origin, allow) {
			allowedOrigin = origin
		} else {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// originMatchesAllow returns true if origin matches any entry in allow
// under localhost/127.0.0.1 aliasing. The browser sends Origin with
// the host the user typed in the address bar (e.g. "localhost" or
// "127.0.0.1" are different origins per the same-origin policy), but
// both refer to the same loopback. We accept either form so a
// developer who leans on either gets a working CORS without
// bookkeeping both in the env var.
func originMatchesAllow(origin string, allow map[string]bool) bool {
	o, err := url.Parse(origin)
	if err != nil || o.Host == "" {
		return false
	}
	aliases := hostAliases(o.Host)
	for _, h := range aliases {
		if allow[(&url.URL{Scheme: o.Scheme, Host: h, Path: o.Path, RawQuery: o.RawQuery}).String()] {
			return true
		}
	}
	return false
}

// hostAliases returns the set of host strings that should be considered
// the same origin as h for CORS purposes. Today: localhost and
// 127.0.0.1 are interchangeable; ::1 is IPv6 loopback.
func hostAliases(h string) []string {
	host, _, err := net.SplitHostPort(h)
	if err != nil {
		host = h
	}
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return []string{"localhost", "127.0.0.1", "::1"}
	}
	return []string{host}
}
