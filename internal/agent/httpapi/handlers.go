package httpapi

// Strategy/version endpoints live in strategy_handlers.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
)

// summaryRE extracts the strategy summary marker from an assistant response.
// Format: <!-- strategy_summary: one-line summary -->
var summaryRE = regexp.MustCompile(`<!--\s*strategy_summary:\s*(.+?)\s*-->`)

// SSE event payloads. Closed via isPayload().
type (
	TextPayload struct {
		Text string `json:"text"`
	}
	ToolCallPayload struct {
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	}
	// RefusalPayload is emitted when the LLM classifier returns off_topic.
	// It is sent before DonePayload so SSE consumers can render a refusal
	// immediately rather than waiting for the stream to close.
	RefusalPayload struct {
		Reason string `json:"reason"`
	}
	// StrategySavedPayload is emitted whenever the capture observer records a
	// verified strategy version. The chat UI consumes it to show the user
	// which strategy was saved from this turn and to fetch the generated source
	// files via GET /v1/strategies/{id}/versions/{rev}.
	StrategySavedPayload struct {
		StrategyID string `json:"strategy_id"`
		Revision   string `json:"revision"`
		ModuleName string `json:"module_name"`
		Summary    string `json:"summary"`
	}
	DonePayload struct {
		Reason string `json:"reason"`
	}
	ErrorPayload struct {
		Message string `json:"message"`
	}
)

func (TextPayload) isPayload()          {}
func (ToolCallPayload) isPayload()      {}
func (RefusalPayload) isPayload()       {}
func (StrategySavedPayload) isPayload() {}
func (DonePayload) isPayload()          {}
func (ErrorPayload) isPayload()         {}

// --- provider-config ---

type setProviderConfigReq struct {
	Provider     string `json:"provider"`
	APIKey       string `json:"api_key"`
	DefaultModel string `json:"default_model"`
	BaseURL      string `json:"base_url,omitempty"`
}

func (s *Server) handleSetProviderConfig(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	var body setProviderConfigReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if !s.allowedProviders[body.Provider] {
		http.Error(w, "unsupported provider", http.StatusBadRequest)
		return
	}
	if body.BaseURL != "" && !strings.HasPrefix(body.BaseURL, "https://") && os.Getenv("AGENT_ALLOW_HTTP_BASE_URL") != "1" {
		http.Error(w, "base_url must be an https:// URL (or set AGENT_ALLOW_HTTP_BASE_URL=1 for local testing)", http.StatusBadRequest)
		return
	}
	if err := s.configs.Set(r.Context(), p.UserID, body.Provider, body.APIKey, body.DefaultModel, body.BaseURL); err != nil {
		slog.Error("store provider config failed", "user_id", p.UserID, "provider", body.Provider, "error", err)
		http.Error(w, "failed to store provider config", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// providerConfigResp is the snake_case wire shape for one row of
// GET /v1/provider-config. The api_key is shipped masked; the real
// value never leaves the KMS layer.
type providerConfigResp struct {
	Provider     string `json:"provider"`
	DefaultModel string `json:"default_model"`
	BaseURL      string `json:"base_url,omitempty"`
	APIKeyMasked string `json:"api_key_masked"`
}

func (s *Server) handleListProviderConfig(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	entries, err := s.configs.List(r.Context(), p.UserID)
	if err != nil {
		slog.Error("list provider configs failed", "user_id", p.UserID, "error", err)
		http.Error(w, "failed to list provider configs", http.StatusInternalServerError)
		return
	}
	out := make([]providerConfigResp, len(entries))
	for i, e := range entries {
		out[i] = providerConfigResp{
			Provider:     e.Provider,
			DefaultModel: e.DefaultModel,
			BaseURL:      e.BaseURL,
			APIKeyMasked: e.APIKeyMasked,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		// Body already started; nothing useful to do besides log.
		slog.Error("encode list provider config", "error", err)
	}
}

func (s *Server) handleDeleteProviderConfig(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	provider := r.PathValue("provider")
	if err := s.configs.Delete(r.Context(), p.UserID, provider); err != nil {
		// Treat missing as 404.
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- sessions ---
// sessionResp is the snake_case wire shape for a session. The store layer holds
// the title as sql.NullString, which encoding/json would leak as
// {"String":"","Valid":false}; the DTO hides pgx wrappers and emits snake_case
// keys (AGENTS.md mandates snake_case JSON).
type sessionResp struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Title     string    `json:"title,omitempty"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// messageResp is the snake_case wire shape for a stored message. The store
// holds tool_calls as raw JSON bytes, which encoding/json would base64-encode;
// json.RawMessage emits them as JSON. Nullable tool_call_id becomes a string.
type messageResp struct {
	ID         int64           `json:"id"`
	SessionID  string          `json:"session_id"`
	Seq        int             `json:"seq"`
	Role       string          `json:"role"`
	Content    string          `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

func toSessionResp(s session.Session) sessionResp {
	return sessionResp{
		ID: s.ID, UserID: s.UserID, Title: s.Title.String,
		Provider: s.Provider, Model: s.Model,
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}
}

func toMessageResp(m session.StoredMessage) messageResp {
	return messageResp{
		ID: m.ID, SessionID: m.SessionID, Seq: m.Seq,
		Role: m.Role, Content: m.Content,
		ToolCallID: m.ToolCallID.String,
		ToolCalls:  json.RawMessage(m.ToolCalls),
		CreatedAt:  m.CreatedAt,
	}
}

// strategyLink is the slim wire shape returned by GET /v1/sessions/{id}
// for each strategy this session produced. The chatui uses the pair
// (id, head_revision) to fetch the source files via the existing
// GET /v1/strategies/{id}/versions/{revision} endpoint.
type strategyLink struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	HeadRevision string `json:"head_revision"`
	CreatedAt    string `json:"created_at"`
}

type createSessionReq struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
	Title    string `json:"title,omitempty"`
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	var body createSessionReq
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Provider == "" || body.Model == "" {
		http.Error(w, "request must include provider and model", http.StatusConflict)
		return
	}
	if _, _, _, err := s.configs.GetDecrypted(r.Context(), p.UserID, body.Provider); err != nil {
		if errors.Is(err, providerconfig.ErrNotFound) {
			http.Error(w, fmt.Sprintf("provider %q not configured for user", body.Provider), http.StatusConflict)
			return
		}
		slog.Error("get provider config failed", "user_id", p.UserID, "provider", body.Provider, "error", err)
		http.Error(w, "failed to load provider config", http.StatusInternalServerError)
		return
	}
	sess, err := s.sessions.CreateSession(r.Context(), p.UserID, body.Provider, body.Model, body.Title)
	if err != nil {
		slog.Error("create session failed", "user_id", p.UserID, "error", err)
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": sess.ID})
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	list, err := s.sessions.ListSessions(r.Context(), p.UserID)
	if err != nil {
		http.Error(w, "failed to list sessions", http.StatusInternalServerError)
		return
	}
	out := make([]sessionResp, len(list))
	for i := range list {
		out[i] = toSessionResp(list[i])
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	sess, msgs, err := s.sessions.GetSession(r.Context(), p.UserID, id)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get session", http.StatusInternalServerError)
		return
	}
	msgsOut := make([]messageResp, len(msgs))
	for i := range msgs {
		msgsOut[i] = toMessageResp(msgs[i])
	}
	// Enrich with every strategy this session produced, newest first.
	// The chatui renders each as a "view code" panel so historically
	// captured strategies are recoverable even when the SSE
	// strategy_saved event was missed (binary restart, event missed,
	// or the strategy was captured before the SSE wiring landed).
	// Strategies is best-effort: when the strategies service is not
	// wired (e.g. in test harnesses) the list is empty
	// rather than failing the whole session fetch.
	strats := []strategyLink{}
	if s.strategies != nil {
		linked, lerr := s.strategies.ListBySession(r.Context(), p.UserID, id)
		if lerr != nil {
			http.Error(w, "failed to list strategies", http.StatusInternalServerError)
			return
		}
		for _, st := range linked {
			strats = append(strats, strategyLink{
				ID:           st.ID,
				Name:         st.Name,
				HeadRevision: string(st.HeadRevision),
				CreatedAt:    st.CreatedAt.Format(iso8601),
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session":    toSessionResp(sess),
		"messages":   msgsOut,
		"strategies": strats,
	})
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	if err := s.sessions.DeleteSession(r.Context(), p.UserID, id); err != nil {
		if errors.Is(err, session.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to delete session", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- messages (SSE) — handler added in Task 6 ---

// totalContentLen returns the sum of character lengths of all history
// messages. Used as a cheap proxy for token count to decide when to compact.
func totalContentLen(history []session.StoredMessage) int {
	n := 0
	for _, m := range history {
		n += len(m.Content)
	}
	return n
}

// compactSessionHistory replaces raw history with a compressed form when
// a stored summary exists or the raw history is large enough to risk a
// context-window error. Returns the (possibly compacted) history slice.
func (s *Server) compactSessionHistory(
	ctx context.Context, sess session.Session, history []session.StoredMessage,
	sessionID, userID string,
) []session.StoredMessage {
	// Stored summary from a prior turn: replace raw history with summary + last 2.
	if sess.Summary.Valid && sess.Summary.String != "" {
		return compactWithSummary(history, sess.Summary.String, "Session summary (previous turn): ")
	}
	// No summary but raw history is large: trigger one-shot summarization.
	if s.providerFactory != nil && totalContentLen(history) > s.compactThreshold(sess.Model) {
		summary := s.compactHistory(ctx, userID, sess.Provider, sess.Model, history)
		if summary != "" {
			if err := s.sessions.UpdateSummary(ctx, sessionID, summary); err != nil {
				slog.Warn("failed to persist compaction summary", "session", sessionID, "err", err)
			}
			return compactWithSummary(history, summary, "Session summary: ")
		}
	}
	return history
}

// compactWithSummary replaces raw history with a summary system message
// plus the most recent user+assistant exchange.
func compactWithSummary(history []session.StoredMessage, summary, prefix string) []session.StoredMessage {
	keep := 6
	if keep > len(history) {
		keep = len(history)
	}
	out := []session.StoredMessage{{
		Role:    "system",
		Content: prefix + summary,
	}}
	if keep > 0 {
		out = append(out, history[len(history)-keep:]...)
	}
	return out
}

const summaryCap = 2048

// compactThreshold returns the history character count at which
// proactive summarization triggers. Targets 75% of the model's
// context window, converting tokens to chars (≈4 chars/token).
const (
	fallbackCompactChars = 32 * 1024
	compactFraction      = 3 // ctxWindow * 3 chars ≈ 75% of context window
)

func (s *Server) compactThreshold(model string) int {
	if s.modelCaps.Default == 0 {
		return fallbackCompactChars
	}
	ctxWindow := s.modelCaps.ContextWindowFor(model)
	// 75% of context window in tokens, ×4 for chars ≈ tokens * 3
	return ctxWindow * compactFraction
}

// compactHistory builds a one-shot summarization provider and asks the
// LLM to compress the conversation into a 200-char summary. Falls back
// to a simple truncation if the summarization call fails.
func (s *Server) compactHistory(ctx context.Context, userID, provider, model string, history []session.StoredMessage) string {
	p, _, err := s.providerFactory(ctx, userID, provider, model)
	if err != nil {
		slog.Warn("compact: failed to build summarizer", "err", err)
		return ""
	}
	// Build a short summarization request. The system prompt is minimal
	// and we only send user+assistant roles (no tools).
	var sb strings.Builder
	for _, m := range history {
		if m.Role == "user" || m.Role == "assistant" {
			sb.WriteString(m.Role)
			sb.WriteString(": ")
			sb.WriteString(m.Content)
			sb.WriteString("\n")
		}
	}
	msgs := []llm.Message{
		{Role: llm.RoleSystem, Content: "Summarize this conversation about a trading strategy in under 200 characters. " +
			"Include: strategy name, key parameters, and what was accomplished."},
		{Role: llm.RoleUser, Content: sb.String()},
	}
	ch, err := p.Stream(ctx, llm.Request{Model: model, Messages: msgs})
	if err != nil {
		slog.Warn("compact: stream failed", "err", err)
		return ""
	}
	var out strings.Builder
	for ev := range ch {
		switch e := ev.(type) {
		case llm.TextDelta:
			out.WriteString(e.Text)
		case llm.ErrorEvent:
			slog.Warn("compact: stream error", "err", e.Err)
			return ""
		case llm.DoneEvent:
		}
	}
	summary := strings.TrimSpace(out.String())
	if len(summary) > summaryCap {
		summary = summary[:summaryCap]
	}
	return summary
}

func (s *Server) handlePostMessage(w http.ResponseWriter, r *http.Request) {
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")

	var body struct {
		Prompt   string `json:"prompt"`
		Provider string `json:"provider,omitempty"`
		Model    string `json:"model,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if body.Prompt == "" {
		http.Error(w, "prompt is required", http.StatusBadRequest)
		return
	}

	sess, history, err := s.sessions.GetSession(r.Context(), p.UserID, id)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to load session", http.StatusInternalServerError)
		return
	}
	if s.agentRunner == nil {
		http.Error(w, "agent runtime unavailable", http.StatusServiceUnavailable)
		return
	}

	// Keep the context window bounded via session summarization.
	history = s.compactSessionHistory(r.Context(), sess, history, id, p.UserID)

	provider := body.Provider
	if provider == "" {
		provider = sess.Provider
	}
	model := body.Model
	if model == "" {
		model = sess.Model
	}

	// Persist the user prompt before streaming.
	if err := s.sessions.AppendMessage(r.Context(), id, "user", body.Prompt); err != nil {
		http.Error(w, "failed to persist prompt", http.StatusInternalServerError)
		return
	}

	sse, err := NewSSEWriter(w)
	if err != nil {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	emit := func(e EventPayload) {
		switch v := e.(type) {
		case TextPayload:
			_ = sse.Write("delta", v)
		case ToolCallPayload:
			_ = sse.Write("tool_call", v)
		case RefusalPayload:
			_ = sse.Write("refusal", v)
		case StrategySavedPayload:
			_ = sse.Write("strategy_saved", v)
		case DonePayload:
			_ = sse.Write("done", v)
		case ErrorPayload:
			_ = sse.Write("error", v)
		}
	}

	text, _ := s.agentRunner.Run(r.Context(), p.UserID, id, provider, model, history, body.Prompt, emit)

	// Persist the assistant turn. On error the runner still returns the
	// partial text it accumulated; the SSE error event is the runner's
	// responsibility via emit.
	assistantText := text
	if assistantText != "" {
		_ = s.sessions.AppendMessage(r.Context(), id, "assistant", assistantText)
		// Extract and store the strategy summary for the next turn.
		// Format: <!-- strategy_summary: one-line summary -->
		if sm := summaryRE.FindStringSubmatch(assistantText); sm != nil {
			if err := s.sessions.UpdateSummary(r.Context(), id, strings.TrimSpace(sm[1])); err != nil {
				slog.Warn("failed to store session summary", "session", id, "err", err)
			}
		}
	}
}

// writeJSON writes v as JSON with the given status. The status param is
// retained so callers can return non-200 (e.g. 201/202/204) as the API grows;
// the current call sites all return 200, which unparam rightly flags but is
// not actionable while the API has no non-200 success path here.
//
//	ever stops flagging it after a future caller passes a non-200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
