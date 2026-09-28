package backtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	backtesttool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/backtest"
)

// fakeCancelStore is a minimal backtest.Store for cancel-tool tests.
// Get returns the seeded row; CancelIfRunning records the call and
// returns the seeded response. Cancel-already-terminal case is covered
// by setting cancelResp=false.
type fakeCancelStore struct {
	bt.Store
	mu         sync.Mutex
	rows       map[string]*bt.Backtest
	cancelled  []string
	cancelResp bool
	cancelErr  error
	getErr     error
}

func newFakeCancelStore() *fakeCancelStore {
	return &fakeCancelStore{rows: map[string]*bt.Backtest{}}
}

func (f *fakeCancelStore) Get(_ context.Context, id string) (*bt.Backtest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	row, ok := f.rows[id]
	if !ok {
		return nil, bt.ErrNotFound
	}
	cp := *row
	return &cp, nil
}

func (f *fakeCancelStore) CancelIfRunning(_ context.Context, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelErr != nil {
		return false, f.cancelErr
	}
	f.cancelled = append(f.cancelled, id)
	return f.cancelResp, nil
}

// TestCancelTool_Spec_NameAndSchema pins the spec contract: the LLM
// dispatches by name, the schema tells the model which fields to send.
// Renaming is a model-facing change.
func TestCancelTool_Spec_NameAndSchema(t *testing.T) {
	t.Parallel()
	spec := backtesttool.CancelSpec()
	if spec.Name != "cancel_backtest" {
		t.Fatalf("tool name: got %q, want %q", spec.Name, "cancel_backtest")
	}
	var schema struct {
		Type       string                    `json:"type"`
		Properties map[string]map[string]any `json:"properties"`
		Required   []string                  `json:"required"`
	}
	if err := json.Unmarshal(spec.JSONSchema, &schema); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	if schema.Type != "object" {
		t.Fatalf("schema.type: got %q, want %q", schema.Type, "object")
	}
	if _, ok := schema.Properties["backtest_id"]; !ok {
		t.Fatalf("schema missing backtest_id property")
	}
	if len(schema.Required) != 1 || schema.Required[0] != "backtest_id" {
		t.Fatalf("schema.required: got %v, want [backtest_id]", schema.Required)
	}
}

// TestCancelTool_HappyPath_RunningFlips: a running backtest owned by
// the caller flips to cancelled and the envelope reports Cancelled:true.
// The store's CancelIfRunning was called with the right ID.
func TestCancelTool_HappyPath_RunningFlips(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	store.cancelResp = true
	store.rows["bt-1"] = &bt.Backtest{ID: "bt-1", UserID: "u-1", Status: bt.StatusRunning}
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	out, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":"bt-1"}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}
	var got struct {
		BacktestID string `json:"backtest_id"`
		Cancelled  bool   `json:"cancelled"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode output: %v (raw=%s)", err, out)
	}
	if got.BacktestID != "bt-1" {
		t.Errorf("backtest_id: got %q, want %q", got.BacktestID, "bt-1")
	}
	if !got.Cancelled {
		t.Errorf("cancelled: got false, want true (running row must flip)")
	}
	if len(store.cancelled) != 1 || store.cancelled[0] != "bt-1" {
		t.Errorf("CancelIfRunning calls: got %v, want [bt-1]", store.cancelled)
	}
}

// TestCancelTool_TerminalRow_Idempotent: a row that's already terminal
// returns Cancelled:false with no error -- calling cancel twice must
// not error, since the HTTP endpoint is documented as idempotent.
func TestCancelTool_TerminalRow_Idempotent(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	store.cancelResp = false
	store.rows["bt-1"] = &bt.Backtest{ID: "bt-1", UserID: "u-1", Status: bt.StatusSucceeded}
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	out, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":"bt-1"}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v (second cancel must be a no-op)", err)
	}
	var got struct {
		BacktestID string `json:"backtest_id"`
		Cancelled  bool   `json:"cancelled"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got.Cancelled {
		t.Errorf("cancelled: got true, want false (already-terminal row must report Cancelled:false)")
	}
	if len(store.cancelled) != 1 {
		t.Errorf("CancelIfRunning calls: got %d, want 1", len(store.cancelled))
	}
}

// TestCancelTool_WrongOwner: a row owned by a different user collapses
// to ErrNotFound -- the tool must not let one user enumerate or cancel
// another user's backtest. Same shape as the HTTP handler.
func TestCancelTool_WrongOwner(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	store.rows["bt-1"] = &bt.Backtest{ID: "bt-1", UserID: "someone-else", Status: bt.StatusRunning}
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":"bt-1"}`))
	if !errors.Is(err, bt.ErrNotFound) {
		t.Fatalf("error: got %v, want ErrNotFound (wrong-owner must collapse to not-found)", err)
	}
	if len(store.cancelled) != 0 {
		t.Errorf("CancelIfRunning must NOT be called for another user's row; got calls=%v", store.cancelled)
	}
}

// TestCancelTool_NotFound: nonexistent ID returns ErrNotFound, not a panic.
func TestCancelTool_NotFound(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":"does-not-exist"}`))
	if !errors.Is(err, bt.ErrNotFound) {
		t.Fatalf("error: got %v, want ErrNotFound", err)
	}
	if len(store.cancelled) != 0 {
		t.Errorf("CancelIfRunning must not be called for missing id; got calls=%v", store.cancelled)
	}
}

// TestCancelTool_EmptyBacktestID: tool must reject an empty id without a
// store hit.
func TestCancelTool_EmptyBacktestID(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":""}`))
	if err == nil {
		t.Fatalf("expected error for empty backtest_id, got nil")
	}
	if len(store.cancelled) != 0 {
		t.Errorf("CancelIfRunning must not be called for empty id; got calls=%v", store.cancelled)
	}
}

// TestCancelTool_MalformedInput: non-JSON must surface a parse error,
// not a panic.
func TestCancelTool_MalformedInput(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{not json`))
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if len(store.cancelled) != 0 {
		t.Errorf("CancelIfRunning must not be called on malformed input; got calls=%v", store.cancelled)
	}
}

// TestCancelTool_StoreErrorSurfaces: a CancelIfRunning storage failure
// surfaces as an error -- not silently swallowed.
func TestCancelTool_StoreErrorSurfaces(t *testing.T) {
	t.Parallel()
	store := newFakeCancelStore()
	store.rows["bt-1"] = &bt.Backtest{ID: "bt-1", UserID: "u-1", Status: bt.StatusRunning}
	store.cancelErr = errors.New("db is down")
	h := backtesttool.CancelHandler(store, &bt.Orchestrator{}, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"backtest_id":"bt-1"}`))
	if err == nil {
		t.Fatalf("expected error from store failure, got nil")
	}
}
