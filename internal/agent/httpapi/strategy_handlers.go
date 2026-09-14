package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// --- strategy/version wire DTOs (snake_case, AGENTS.md) ---

type strategyResp struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	HeadRevision    string `json:"head_revision"`
	SourceSessionID string `json:"source_session_id"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

type versionSummaryResp struct {
	Revision       string `json:"revision"`
	ParentRevision string `json:"parent_revision,omitempty"`
	CreatedAt      string `json:"created_at"`
	ModuleName     string `json:"module_name"`
	Summary        string `json:"summary"`
	Target         string `json:"target"`
	WasmRef        string `json:"wasm_ref,omitempty"`
	ManifestHash   string `json:"manifest_hash,omitempty"`
}

type versionResp struct {
	Revision       string                     `json:"revision"`
	ParentRevision string                     `json:"parent_revision,omitempty"`
	CreatedAt      string                     `json:"created_at"`
	Meta           map[string]json.RawMessage `json:"meta"`
	Files          map[string]string          `json:"files"`
}

type headSummaryResp struct {
	Revision     string `json:"revision"`
	CreatedAt    string `json:"created_at"`
	ModuleName   string `json:"module_name"`
	Summary      string `json:"summary"`
	Target       string `json:"target"`
	ImageRef     string `json:"image_ref,omitempty"`
	WasmRef      string `json:"wasm_ref,omitempty"`
	ManifestHash string `json:"manifest_hash,omitempty"`
}

type listStrategiesResp struct {
	Strategies []strategyResp `json:"strategies"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type listVersionsResp struct {
	Versions   []versionSummaryResp `json:"versions"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// iso8601 renders time.Time as RFC3339 (JSON-friendly, matches spec §8).
const iso8601 = "2006-01-02T15:04:05Z07:00"

func toStrategyResp(s strategies.Strategy) strategyResp {
	return strategyResp{
		ID:              s.ID,
		Name:            s.Name,
		HeadRevision:    string(s.HeadRevision),
		SourceSessionID: s.SourceSessionID,
		CreatedAt:       s.CreatedAt.Format(iso8601),
		UpdatedAt:       s.UpdatedAt.Format(iso8601),
	}
}

func toVersionSummaryResp(v strategies.VersionSummary) versionSummaryResp {
	return versionSummaryResp{
		Revision:       string(v.Revision),
		ParentRevision: string(v.ParentRevision),
		CreatedAt:      v.CreatedAt.Format(iso8601),
		ModuleName:     v.ModuleName,
		Summary:        v.Summary,
		Target:         v.Target,
		WasmRef:        v.WasmRef,
		ManifestHash:   v.ManifestHash,
	}
}

func toVersionResp(v strategies.Version) versionResp {
	return versionResp{
		Revision:       string(v.Revision),
		ParentRevision: string(v.ParentRevision),
		CreatedAt:      v.CreatedAt.Format(iso8601),
		Meta:           metaToMap(v.Meta),
		Files:          v.Files,
	}
}

// metaToMap renders a strategies.Meta as a stable JSON object. Validation is
// already raw JSON (may be null); the scalar fields are wrapped as JSON strings.
func metaToMap(m strategies.Meta) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"provider":    jsonRawString(m.Provider),
		"model":       jsonRawString(m.Model),
		"module_name": jsonRawString(m.ModuleName),
		"summary":     jsonRawString(m.Summary),
		"rationale":   jsonRawString(m.Rationale),
		"validation":  m.Validation,
	}
}

func jsonRawString(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

func parsePage(r *http.Request) strategies.Page {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	return strategies.Page{Limit: limit, Cursor: r.URL.Query().Get("cursor")}
}

// requireStrategies returns false (and writes 503) when no strategy store is
// wired, mirroring the agent-runner-unavailable guard in handlePostMessage.
func (s *Server) requireStrategies(w http.ResponseWriter) bool {
	if s.strategies == nil {
		http.Error(w, "strategy store unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// --- handlers ---

// handleListStrategies: GET /v1/strategies
func (s *Server) handleListStrategies(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	items, next, err := s.strategies.ListStrategies(r.Context(), p.UserID, parsePage(r))
	if err != nil {
		http.Error(w, "failed to list strategies", http.StatusInternalServerError)
		return
	}
	out := listStrategiesResp{Strategies: make([]strategyResp, 0, len(items)), NextCursor: next}
	for _, st := range items {
		out.Strategies = append(out.Strategies, toStrategyResp(st))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetStrategy: GET /v1/strategies/{id}
func (s *Server) handleGetStrategy(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	st, err := s.strategies.GetStrategy(r.Context(), p.UserID, id)
	if err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	head := headSummaryResp{}
	if st.HeadRevision != "" {
		// Best-effort: enrich with the head version summary. A missing head
		// version is unexpected (head always resolves), so omit rather than
		// fail the whole request if the lookup errors.
		if v, err := s.strategies.GetVersion(r.Context(), id, st.HeadRevision); err == nil {
			head = headSummaryResp{
				Revision:     string(v.Revision),
				CreatedAt:    v.CreatedAt.Format(iso8601),
				ModuleName:   v.Meta.ModuleName,
				Summary:      v.Meta.Summary,
				Target:       v.Target,
				ImageRef:     v.ImageRef,
				WasmRef:      v.WasmRef,
				ManifestHash: v.ManifestHash,
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"strategy": toStrategyResp(st),
		"head":     head,
	})
}

// handleListVersions: GET /v1/strategies/{id}/versions
func (s *Server) handleListVersions(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, id); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	items, next, err := s.strategies.ListVersions(r.Context(), id, parsePage(r))
	if err != nil {
		http.Error(w, "failed to list versions", http.StatusInternalServerError)
		return
	}
	out := listVersionsResp{Versions: make([]versionSummaryResp, 0, len(items)), NextCursor: next}
	for _, v := range items {
		out.Versions = append(out.Versions, toVersionSummaryResp(v))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetVersion: GET /v1/strategies/{id}/versions/{revision}
func (s *Server) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, id); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	v, err := s.strategies.GetVersion(r.Context(), id, strategies.Revision(r.PathValue("revision")))
	if err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get version", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, toVersionResp(v))
}

// handleRollback: POST /v1/strategies/{id}/rollback
func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	if _, err := s.strategies.GetStrategy(r.Context(), p.UserID, id); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get strategy", http.StatusInternalServerError)
		return
	}
	var body struct {
		Revision string `json:"revision"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Revision == "" {
		http.Error(w, "revision is required", http.StatusBadRequest)
		return
	}
	if err := s.strategies.SetHead(r.Context(), id, strategies.Revision(body.Revision)); err != nil {
		if errors.Is(err, strategies.ErrNotFound) {
			http.Error(w, "revision not in history", http.StatusBadRequest)
			return
		}
		http.Error(w, "rollback failed", http.StatusInternalServerError)
		return
	}
	if s.audit != nil {
		_ = s.audit.Insert(r.Context(), p.UserID, audit.ActionStrategyRollback, []byte(`{}`))
	}
	st, _ := s.strategies.GetStrategy(r.Context(), p.UserID, id)
	writeJSON(w, http.StatusOK, toStrategyResp(st))
}

// handleSaveSession: POST /v1/sessions/{id}/save
//
// Consumes the session's stashed pending capture (set by a failed auto-capture
// during a turn) and records it as a version. The stash is deleted on capture,
// so a second save finds nothing pending and returns 404 (idempotent consume).
func (s *Server) handleSaveSession(w http.ResponseWriter, r *http.Request) {
	if !s.requireStrategies(w) {
		return
	}
	p := PrincipalFromCtx(r.Context())
	id := r.PathValue("id")
	if _, _, err := s.sessions.GetSession(r.Context(), p.UserID, id); err != nil {
		if errors.Is(err, session.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get session", http.StatusInternalServerError)
		return
	}
	v, err := s.strategies.CapturePending(r.Context(), id)
	if err != nil {
		if errors.Is(err, strategies.ErrNoPending) {
			http.Error(w, "no pending strategy to save", http.StatusNotFound)
			return
		}
		http.Error(w, "save failed", http.StatusInternalServerError)
		return
	}
	if s.audit != nil {
		_ = s.audit.Insert(r.Context(), p.UserID, audit.ActionStrategyVersion, []byte(`{}`))
	}
	writeJSON(w, http.StatusOK, toVersionResp(v))
}
