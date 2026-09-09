package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
)

// RestartClearer clears a strategy's per-window restart budget. The
// orchestrator (Plan 3) owns the budget; this is the minimal seam the
// restart handler needs so the HTTP layer does not import the
// orchestrator package. Wired via WithRestartClearer; nil => 501.
type RestartClearer interface {
	ClearRestartBudget(ctx context.Context, userID, strategyID string) error
}

// haltResumeReq is the optional JSON body for the halt/resume endpoints.
// reason is the human-readable justification recorded in the audit log.
type haltResumeReq struct {
	Reason string `json:"reason,omitempty"`
}

// readReason decodes the optional halt/resume body and returns the
// reason, falling back to the query string then a default. Bodies it
// cannot decode are ignored (the endpoints tolerate an empty body).
func readReason(r *http.Request, fallback string) string {
	if r.Body != nil && r.ContentLength != 0 {
		var req haltResumeReq
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil && req.Reason != "" {
			return req.Reason
		}
	}
	if q := r.URL.Query().Get("reason"); q != "" {
		return q
	}
	return fallback
}

// haltStrategy sets the kill switch for the authenticated user. The
// strategy id is path-scoped but the kill switch is per-user (spec §6.6:
// one halt stops every live strategy the user owns). POST
// /v1/strategies/{id}/halt.
func (s *Server) haltStrategy(w http.ResponseWriter, r *http.Request) {
	if s.safetyKernel == nil {
		http.Error(w, "safety kernel unavailable", http.StatusServiceUnavailable)
		return
	}
	userID := PrincipalFromCtx(r.Context()).UserID
	if userID == "" {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	reason := readReason(r, "halted via HTTP")
	if err := s.safetyKernel.Halt(r.Context(), userID, reason); err != nil {
		slog.Warn("halt failed", "user", userID, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.auditStrategyAction(r.Context(), userID, string(audit.ActionStrategyHalted), reason)
	w.WriteHeader(http.StatusNoContent)
}

// resumeStrategy clears the kill switch for the authenticated user.
// POST /v1/strategies/{id}/resume.
func (s *Server) resumeStrategy(w http.ResponseWriter, r *http.Request) {
	if s.safetyKernel == nil {
		http.Error(w, "safety kernel unavailable", http.StatusServiceUnavailable)
		return
	}
	userID := PrincipalFromCtx(r.Context()).UserID
	if userID == "" {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	if err := s.safetyKernel.Resume(r.Context(), userID); err != nil {
		slog.Warn("resume failed", "user", userID, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.auditStrategyAction(r.Context(), userID, string(audit.ActionStrategyResumed), "")
	w.WriteHeader(http.StatusNoContent)
}

// restartStrategy clears the strategy's restart budget. The per-strategy
// restart budget is owned by the orchestrator (Plan 3); this handler is
// wired via WithRestartClearer when the orchestrator lands. Until then
// (nil clearer) it returns 501. POST /v1/strategies/{id}/restart.
func (s *Server) restartStrategy(w http.ResponseWriter, r *http.Request) {
	if s.restartClearer == nil {
		http.Error(w, "restart not yet wired", http.StatusNotImplemented)
		return
	}
	userID := PrincipalFromCtx(r.Context()).UserID
	if userID == "" {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	strategyID := r.PathValue("id")
	if err := s.restartClearer.ClearRestartBudget(r.Context(), userID, strategyID); err != nil {
		slog.Warn("restart failed", "user", userID, "strategy", strategyID, "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// auditStrategyAction records an audit row best-effort; a nil audit
// writer (tests) is a no-op.
func (s *Server) auditStrategyAction(ctx context.Context, userID, action, detail string) {
	if s.audit == nil {
		return
	}
	payload := []byte("{}")
	if detail != "" {
		b, _ := json.Marshal(map[string]string{"reason": detail})
		payload = b
	}
	if err := s.audit.Insert(ctx, userID, action, payload); err != nil {
		slog.Warn("audit insert failed", "user", userID, "action", action, "error", err)
	}
}
