package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// testUserID is the principal seeded into requests by the invoke helper.
const testUserID = "u-1"

// testStrategyID is the default strategy id used across tests.
const testStrategyID = "s1"

// fakeStrategies implements the strategies.Store methods exercised by the
// handlers. Unexercised methods hit the embedded interface and panic if
// reached — a standard test pattern that keeps the fake honest.

// fakeStrategies implements the strategies.Store methods exercised by the
// handlers. Unexercised methods hit the embedded interface and panic if
// reached — a standard test pattern that keeps the fake honest.
type fakeStrategies struct {
	strategies.Store
	strategiesByID    map[string]strategies.Strategy
	versionsByRev     map[string]strategies.Version
	summariesByStrat  map[string][]strategies.VersionSummary
	strategiesByUser  map[string][]strategies.Strategy
	head              map[string]strategies.Revision
	setHeadErr        error
	capturePending    strategies.Version
	capturePendingErr error
}

func newFakeStrategies() *fakeStrategies {
	return &fakeStrategies{
		strategiesByID:   map[string]strategies.Strategy{},
		versionsByRev:    map[string]strategies.Version{},
		summariesByStrat: map[string][]strategies.VersionSummary{},
		strategiesByUser: map[string][]strategies.Strategy{},
		head:             map[string]strategies.Revision{},
	}
}

func (f *fakeStrategies) GetStrategy(_ context.Context, userID, id string) (strategies.Strategy, error) {
	st, ok := f.strategiesByID[id]
	if !ok || st.UserID != userID {
		return strategies.Strategy{}, strategies.ErrNotFound
	}
	return st, nil
}

func (f *fakeStrategies) GetVersion(_ context.Context, strategyID string, revision strategies.Revision) (strategies.Version, error) {
	v, ok := f.versionsByRev[string(revision)]
	if !ok {
		return strategies.Version{}, strategies.ErrNotFound
	}
	return v, nil
}

func (f *fakeStrategies) ListVersions(_ context.Context, strategyID string, _ strategies.Page) ([]strategies.VersionSummary, string, error) {
	return f.summariesByStrat[strategyID], "", nil
}

func (f *fakeStrategies) ListStrategies(_ context.Context, userID string, _ strategies.Page) ([]strategies.Strategy, string, error) {
	return f.strategiesByUser[userID], "", nil
}

func (f *fakeStrategies) SetHead(_ context.Context, strategyID string, revision strategies.Revision) error {
	if f.setHeadErr != nil {
		return f.setHeadErr
	}
	f.head[strategyID] = revision
	// Mirror the Postgres store: SetHead updates the strategy row's
	// head_revision, so a subsequent GetStrategy returns the new head.
	if st, ok := f.strategiesByID[strategyID]; ok {
		st.HeadRevision = revision
		f.strategiesByID[strategyID] = st
	}
	return nil
}

func (f *fakeStrategies) CapturePending(_ context.Context, sessionID string) (strategies.Version, error) {
	if f.capturePendingErr != nil {
		return strategies.Version{}, f.capturePendingErr
	}
	return f.capturePending, nil
}

// newStrategyServer wires a Server with the fake strategy store + a session
// store whose GetSession always succeeds for the seeded principal.
func newStrategyServer(t *testing.T) (*Server, *fakeStrategies) {
	t.Helper()
	f := newFakeStrategies()
	srv := &Server{
		sessions:   &fakeSessionStore{},
		strategies: f,
	}
	return srv, f
}

// seedStrategy inserts a strategy for the test principal with the given id and
// head revision.
func seedStrategy(f *fakeStrategies, id, headRev string) {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	st := strategies.Strategy{
		ID:              id,
		UserID:          testUserID,
		Name:            id,
		HeadRevision:    strategies.Revision(headRev),
		SourceSessionID: "sess-src",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	f.strategiesByID[id] = st
	f.strategiesByUser[testUserID] = append(f.strategiesByUser[testUserID], st)
}

// seedVersion inserts a full Version + its VersionSummary under the test
// strategy (id testStrategyID).
func seedVersion(f *fakeStrategies, rev, parent string, meta strategies.Meta, files map[string]string) {
	now := time.Date(2026, 7, 30, 12, len(rev), 0, 0, time.UTC)
	v := strategies.Version{
		Revision:       strategies.Revision(rev),
		ParentRevision: strategies.Revision(parent),
		CreatedAt:      now,
		Meta:           meta,
		Files:          files,
	}
	f.versionsByRev[rev] = v
	f.summariesByStrat[testStrategyID] = append(f.summariesByStrat[testStrategyID], strategies.VersionSummary{
		Revision:       v.Revision,
		ParentRevision: v.ParentRevision,
		CreatedAt:      v.CreatedAt,
		ModuleName:     meta.ModuleName,
		Summary:        meta.Summary,
	})
}

// invoke runs a handler method directly with a path-value-bearing request,
// bypassing the auth/rate-limit middleware (principal is seeded via
// withPrincipal). Mirrors the existing handler-test style in handlers_test.go.
func invoke(
	t *testing.T,
	h func(http.ResponseWriter, *http.Request),
	method, target, body string,
	pathVals map[string]string,
	userID ...string,
) *httptest.ResponseRecorder {
	t.Helper()
	var rdr interface{ Read([]byte) (int, error) } = http.NoBody
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := withPrincipal(httptest.NewRequestWithContext(t.Context(), method, target, rdr), userID...)
	for k, v := range pathVals {
		req.SetPathValue(k, v)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&m); err != nil {
		t.Fatalf("decode body: %v (body=%q)", err, rec.Body.String())
	}
	return m
}

// ----- list strategies -----

func TestListStrategies_Pagination(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "")
	seedStrategy(f, "s2", "")

	rec := invoke(t, srv.handleListStrategies, http.MethodGet, "/v1/strategies?limit=10", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	arr, _ := body["strategies"].([]any)
	if len(arr) != 2 {
		t.Fatalf("strategies len: want 2, got %d", len(arr))
	}
	first, _ := arr[0].(map[string]any)
	if _, ok := first["head_revision"]; !ok {
		t.Errorf("want snake_case head_revision key, got %v", first)
	}
	if _, ok := first["source_session_id"]; !ok {
		t.Errorf("want snake_case source_session_id key, got %v", first)
	}
	if _, ok := body["next_cursor"]; ok {
		t.Errorf("want no next_cursor on last page, got %q", body["next_cursor"])
	}
}

func TestListStrategies_StoreUnavailable_503(t *testing.T) {
	srv := &Server{sessions: &fakeSessionStore{}} // no strategies store
	rec := invoke(t, srv.handleListStrategies, http.MethodGet, "/v1/strategies", "", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code: want 503, got %d", rec.Code)
	}
}

// ----- get strategy -----

func TestGetStrategy_NotFound(t *testing.T) {
	srv, _ := newStrategyServer(t)
	rec := invoke(t, srv.handleGetStrategy, http.MethodGet, "/v1/strategies/missing", "", map[string]string{"id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

func TestGetStrategy_Ownership404(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "")
	// Request as a different user: GetStrategy("u-2", "s1") -> ErrNotFound.
	rec := invoke(t, srv.handleGetStrategy, http.MethodGet, "/v1/strategies/s1", "", map[string]string{"id": "s1"}, "u-2")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404 for non-owner, got %d", rec.Code)
	}
}

func TestGetStrategy_WithHeadSummary(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev2")
	seedVersion(f, "rev1", "", strategies.Meta{ModuleName: "mod1", Summary: "first"}, nil)
	seedVersion(f, "rev2", "rev1", strategies.Meta{ModuleName: "mod2", Summary: "second"}, nil)

	rec := invoke(t, srv.handleGetStrategy, http.MethodGet, "/v1/strategies/s1", "", map[string]string{"id": "s1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	head, _ := body["head"].(map[string]any)
	if head == nil {
		t.Fatalf("want head summary, got nil")
	}
	if head["module_name"] != "mod2" {
		t.Errorf("head module_name: want mod2, got %v", head["module_name"])
	}
	strat, _ := body["strategy"].(map[string]any)
	if strat["head_revision"] != "rev2" {
		t.Errorf("head_revision: want rev2, got %v", strat["head_revision"])
	}
}

// ----- list versions -----

func TestGetStrategy_WasmHeadIncludesWasmRef(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev-wasm")
	seedVersion(f, "rev-wasm", "", strategies.Meta{ModuleName: "mod-wasm", Summary: "w"}, nil)
	// Enrich the head version with WASM artifact fields.
	v := f.versionsByRev["rev-wasm"]
	v.Target = "go-wasm"
	v.WasmRef = "wasm-sha"
	v.ManifestHash = "manifest-sha"
	f.versionsByRev["rev-wasm"] = v

	rec := invoke(t, srv.handleGetStrategy, http.MethodGet, "/v1/strategies/s1", "", map[string]string{"id": "s1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	head, _ := body["head"].(map[string]any)
	if head == nil {
		t.Fatalf("want head summary, got nil")
	}
	if head["target"] != "go-wasm" {
		t.Errorf("target: want go-wasm, got %v", head["target"])
	}
	if head["wasm_ref"] != "wasm-sha" {
		t.Errorf("wasm_ref: want wasm-sha, got %v", head["wasm_ref"])
	}
	if head["manifest_hash"] != "manifest-sha" {
		t.Errorf("manifest_hash: want manifest-sha, got %v", head["manifest_hash"])
	}
	if head["image_ref"] != nil && head["image_ref"] != "" {
		t.Errorf("image_ref: want empty for wasm, got %v", head["image_ref"])
	}
}

func TestListVersions_NewestFirst(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev2")
	seedVersion(f, "rev1", "", strategies.Meta{ModuleName: "m1"}, nil)
	seedVersion(f, "rev2", "rev1", strategies.Meta{ModuleName: "m2"}, nil)
	// Store lists newest-first, matching the Postgres keyset order.
	f.summariesByStrat["s1"] = []strategies.VersionSummary{
		{Revision: "rev2", ParentRevision: "rev1", ModuleName: "m2"},
		{Revision: "rev1", ModuleName: "m1"},
	}

	rec := invoke(t, srv.handleListVersions, http.MethodGet, "/v1/strategies/s1/versions", "", map[string]string{"id": "s1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	versions, _ := body["versions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("versions len: want 2, got %d", len(versions))
	}
	first, _ := versions[0].(map[string]any)
	if first["revision"] != "rev2" {
		t.Errorf("first revision: want rev2 (newest), got %v", first["revision"])
	}
	second, _ := versions[1].(map[string]any)
	if _, ok := second["parent_revision"]; ok {
		t.Errorf("rev1 parent_revision should be omitempty (empty), got %v", second["parent_revision"])
	}
}

func TestListVersions_WasmDeployabilityFields(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev-wasm")
	f.summariesByStrat["s1"] = []strategies.VersionSummary{
		{Revision: "rev-wasm", ModuleName: "wasm-strategy", Summary: "compiled", Target: "go-wasm", WasmRef: "wasm-sha", ManifestHash: "manifest-sha"},
	}

	rec := invoke(t, srv.handleListVersions, http.MethodGet, "/v1/strategies/s1/versions", "", map[string]string{"id": "s1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	versions, _ := body["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("versions len: want 1, got %d", len(versions))
	}
	first, _ := versions[0].(map[string]any)
	if first["target"] != "go-wasm" {
		t.Errorf("target: want go-wasm, got %v", first["target"])
	}
	if first["wasm_ref"] != "wasm-sha" {
		t.Errorf("wasm_ref: want wasm-sha, got %v", first["wasm_ref"])
	}
	if first["manifest_hash"] != "manifest-sha" {
		t.Errorf("manifest_hash: want manifest-sha, got %v", first["manifest_hash"])
	}
}

func TestListVersions_StrategyNotFound404(t *testing.T) {
	srv, _ := newStrategyServer(t)
	rec := invoke(t, srv.handleListVersions, http.MethodGet, "/v1/strategies/missing/versions", "", map[string]string{"id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

// ----- get version -----

func TestGetVersion_FilesAndMeta(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev1")
	seedVersion(f, "rev1", "", strategies.Meta{
		Provider: "openai", Model: "gpt-4o", ModuleName: "mymod", Summary: "s", Rationale: "r",
	}, map[string]string{"strategy.go": "package main"})

	rec := invoke(t, srv.handleGetVersion, http.MethodGet, "/v1/strategies/s1/versions/rev1", "",
		map[string]string{"id": "s1", "revision": "rev1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["revision"] != "rev1" {
		t.Errorf("revision: want rev1, got %v", body["revision"])
	}
	files, _ := body["files"].(map[string]any)
	if files["strategy.go"] != "package main" {
		t.Errorf("files[strategy.go]: want 'package main', got %v", files["strategy.go"])
	}
	meta, _ := body["meta"].(map[string]any)
	if meta == nil {
		t.Fatalf("want meta object, got nil")
	}
	if meta["module_name"] != "mymod" {
		t.Errorf("meta module_name: want mymod, got %v", meta["module_name"])
	}
}

func TestGetVersion_NotFound404(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev1")

	rec := invoke(t, srv.handleGetVersion, http.MethodGet, "/v1/strategies/s1/versions/nonexistent", "",
		map[string]string{"id": "s1", "revision": "nonexistent"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

// ----- rollback -----

func TestRollback_Success(t *testing.T) {
	srv, f := newStrategyServer(t)
	auditSpy := &fakeAuditWriter{}
	srv.audit = auditSpy
	seedStrategy(f, "s1", "rev2")
	seedVersion(f, "rev1", "", strategies.Meta{ModuleName: "m1"}, nil)

	rec := invoke(t, srv.handleRollback, http.MethodPost, "/v1/strategies/s1/rollback",
		`{"revision":"rev1"}`, map[string]string{"id": "s1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["head_revision"] != "rev1" {
		t.Errorf("head_revision after rollback: want rev1, got %v", body["head_revision"])
	}
	if f.head["s1"] != strategies.Revision("rev1") {
		t.Errorf("store head: want rev1, got %v", f.head["s1"])
	}
	if !auditSpy.has("strategy.rollback") {
		t.Errorf("want audit row strategy.rollback, got %v", auditSpy.rows)
	}
}

func TestRollback_RevisionNotInHistory400(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev2")
	f.setHeadErr = strategies.ErrNotFound

	rec := invoke(t, srv.handleRollback, http.MethodPost, "/v1/strategies/s1/rollback",
		`{"revision":"nope"}`, map[string]string{"id": "s1"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code: want 400, got %d", rec.Code)
	}
}

func TestRollback_StrategyNotFound404(t *testing.T) {
	srv, _ := newStrategyServer(t)
	rec := invoke(t, srv.handleRollback, http.MethodPost, "/v1/strategies/missing/rollback",
		`{"revision":"rev1"}`, map[string]string{"id": "missing"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

func TestRollback_MissingRevision400(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev2")
	rec := invoke(t, srv.handleRollback, http.MethodPost, "/v1/strategies/s1/rollback",
		`{"revision":""}`, map[string]string{"id": "s1"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code: want 400 for empty revision, got %d", rec.Code)
	}
}

func TestRollback_InvalidBody400(t *testing.T) {
	srv, f := newStrategyServer(t)
	seedStrategy(f, "s1", "rev2")
	rec := invoke(t, srv.handleRollback, http.MethodPost, "/v1/strategies/s1/rollback",
		`{bad`, map[string]string{"id": "s1"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code: want 400 for invalid JSON, got %d", rec.Code)
	}
}

// ----- manual save -----

func TestSave_CapturesPending(t *testing.T) {
	srv, f := newStrategyServer(t)
	auditSpy := &fakeAuditWriter{}
	srv.audit = auditSpy
	f.capturePending = strategies.Version{
		Revision: strategies.Revision("rev1"),
		Meta:     strategies.Meta{ModuleName: "savedmod"},
		Files:    map[string]string{"strategy.go": "package x"},
	}

	rec := invoke(t, srv.handleSaveSession, http.MethodPost, "/v1/sessions/sess-1/save", "",
		map[string]string{"id": "sess-1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code: want 200, got %d (body=%s)", rec.Code, rec.Body.String())
	}
	body := decodeBody(t, rec)
	if body["revision"] != "rev1" {
		t.Errorf("revision: want rev1, got %v", body["revision"])
	}
	meta, _ := body["meta"].(map[string]any)
	if meta["module_name"] != "savedmod" {
		t.Errorf("meta module_name: want savedmod, got %v", meta["module_name"])
	}
	if !auditSpy.has("strategy.version") {
		t.Errorf("want audit row strategy.version, got %v", auditSpy.rows)
	}
}

func TestSave_NoPending404(t *testing.T) {
	srv, f := newStrategyServer(t)
	f.capturePendingErr = strategies.ErrNoPending

	rec := invoke(t, srv.handleSaveSession, http.MethodPost, "/v1/sessions/sess-1/save", "",
		map[string]string{"id": "sess-1"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

func TestSave_SessionNotFound404(t *testing.T) {
	srv, _ := newStrategyServer(t)
	srv.sessions = &fakeSessionStore{getErr: session.ErrNotFound}

	rec := invoke(t, srv.handleSaveSession, http.MethodPost, "/v1/sessions/nope/save", "",
		map[string]string{"id": "nope"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code: want 404, got %d", rec.Code)
	}
}

func TestSave_StoreUnavailable503(t *testing.T) {
	srv := &Server{sessions: &fakeSessionStore{}} // no strategies store
	rec := invoke(t, srv.handleSaveSession, http.MethodPost, "/v1/sessions/sess-1/save", "",
		map[string]string{"id": "sess-1"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code: want 503, got %d", rec.Code)
	}
}
