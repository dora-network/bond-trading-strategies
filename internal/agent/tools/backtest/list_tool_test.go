package backtest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	bt "github.com/dora-network/bond-trading-strategies/internal/agent/backtest"
	backtesttool "github.com/dora-network/bond-trading-strategies/internal/agent/tools/backtest"
)

// fakeListStore is a minimal in-memory backtest.Store tailored for the
// list_backtests tests. It only implements the methods the handler
// reaches; unexercised methods panic. listBacktests is the only one
// that matters -- and the rows returned are staged in slice order so
// tests can assert on the order the LLM would see.
type fakeListStore struct {
	bt.Store
	rows []bt.Backtest
	// capture records the args passed to ListBacktests so tests can
	// assert the tool forwarded ownership + filters correctly.
	captured struct {
		userID     string
		strategyID *string
		status     *bt.Status
		limit      int
	}
	// listErr lets tests simulate a DB failure.
	listErr error
}

func (f *fakeListStore) ListBacktests(_ context.Context, userID string, strategyID *string, status *bt.Status, limit int) ([]*bt.Backtest, error) {
	f.captured.userID = userID
	f.captured.strategyID = strategyID
	f.captured.status = status
	f.captured.limit = limit
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]*bt.Backtest, 0, len(f.rows))
	for i := range f.rows {
		out = append(out, &f.rows[i])
	}
	return out, nil
}

// TestListTool_Spec_NameAndSchema pins the spec contract: the LLM
// dispatches by name, the schema tells the model which fields to send.
// Renaming is a model-facing change.
func TestListTool_Spec_NameAndSchema(t *testing.T) {
	t.Parallel()
	spec := backtesttool.ListSpec()
	if spec.Name != "list_backtests" {
		t.Fatalf("tool name: got %q, want %q", spec.Name, "list_backtests")
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
	for _, field := range []string{"strategy_id", "status", "limit"} {
		if _, ok := schema.Properties[field]; !ok {
			t.Fatalf("schema missing %q property", field)
		}
	}
	if len(schema.Required) != 0 {
		t.Fatalf("schema.required: got %v, want [] (all fields optional)", schema.Required)
	}
}

// TestListTool_HappyPath_NoFilters: no strategy_id, no status, no
// limit. Store returns every row; handler emits an envelope with the
// same rows in order. Crucially: store.ListBacktests is called with
// userID=userID and nil strategy/status, so the SQL predicate is
// user-scoped.
func TestListTool_HappyPath_NoFilters(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	rows := []bt.Backtest{
		{ID: "bt-1", UserID: "u-1", Status: bt.StatusSucceeded, RequestedAt: now},
		{ID: "bt-2", UserID: "u-1", Status: bt.StatusRunning, RequestedAt: now.Add(-time.Minute)},
	}
	store := &fakeListStore{rows: rows}
	h := backtesttool.ListHandler(store, "u-1")

	out, err := h(t.Context(), "tool-call", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}

	// Capture assertions: ownership + no filters forwarded.
	if store.captured.userID != "u-1" {
		t.Errorf("userID: got %q, want %q", store.captured.userID, "u-1")
	}
	if store.captured.strategyID != nil {
		t.Errorf("strategyID: got %v, want nil", *store.captured.strategyID)
	}
	if store.captured.status != nil {
		t.Errorf("status: got %v, want nil", *store.captured.status)
	}

	var got struct {
		Backtests []map[string]any `json:"backtests"`
		Count     int              `json:"count"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got.Count != 2 {
		t.Errorf("count: got %d, want 2", got.Count)
	}
	if len(got.Backtests) != 2 {
		t.Fatalf("backtests: got %d entries, want 2", len(got.Backtests))
	}
	if got.Backtests[0]["id"] != "bt-1" {
		t.Errorf("backtests[0].id: got %v, want bt-1 (newest first)", got.Backtests[0]["id"])
	}
	if got.Backtests[1]["id"] != "bt-2" {
		t.Errorf("backtests[1].id: got %v, want bt-2", got.Backtests[1]["id"])
	}
}

// TestListTool_StatusFilter_ForwardsToStore: an LLM asking "is my
// backtest still running" passes status=running; the handler must
// forward that as a non-nil *bt.Status so the SQL query filters
// server-side (no in-memory second pass).
func TestListTool_StatusFilter_ForwardsToStore(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"status":"running"}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}
	if store.captured.status == nil {
		t.Fatal("status: got nil, want non-nil (the LLM passed status=running)")
	}
	if *store.captured.status != bt.StatusRunning {
		t.Errorf("status: got %q, want %q", *store.captured.status, bt.StatusRunning)
	}
}

// TestListTool_StatusFilter_RejectsUnknown: the status whitelist is
// the LLM's only signal that a typo (e.g. "runing") won't silently
// return zero rows. Without the explicit error the LLM has no way to
// tell "no rows match" from "unknown status -> SQL returned nothing".
func TestListTool_StatusFilter_RejectsUnknown(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"status":"runing"}`))
	if err == nil {
		t.Fatal("expected error for unknown status, got nil")
	}
	if !contains(err.Error(), "invalid") {
		t.Errorf("error: got %q, want one mentioning 'invalid'", err.Error())
	}
}

// TestListTool_StrategyFilter_ForwardsToStore: optional strategy_id
// is forwarded as a non-nil *string so the SQL adds
// `and strategy_id=$N`. Without it, the LLM would list ALL the
// caller's strategies' backtests, which is the wrong tool behavior
// when the user explicitly named a strategy.
func TestListTool_StrategyFilter_ForwardsToStore(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"strategy_id":"s-42"}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}
	if store.captured.strategyID == nil {
		t.Fatal("strategyID: got nil, want non-nil (the LLM passed strategy_id=s-42)")
	}
	if *store.captured.strategyID != "s-42" {
		t.Errorf("strategyID: got %q, want %q", *store.captured.strategyID, "s-42")
	}
}

// TestListTool_Limit_ForwardsToStore: the LLM can cap the page size
// (e.g. "give me the 5 most recent"). The store applies its own
// cap; the test pins the LLM's arg flows through.
func TestListTool_Limit_ForwardsToStore(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{"limit":5}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}
	if store.captured.limit != 5 {
		t.Errorf("limit: got %d, want 5", store.captured.limit)
	}
}

// TestListTool_StoreError_Surfaces: a DB failure must propagate as an
// error -- never silently return an empty list, which the LLM would
// read as "no backtests exist".
func TestListTool_StoreError_Surfaces(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{listErr: errors.New("db is down")}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error from store failure, got nil")
	}
}

// TestListTool_OwnershipIsEnforcedByStore: the tool never sees
// another user's rows because the store's user_id WHERE predicate
// filters them in SQL. The handler passes userID through verbatim;
// the test pins that -- no in-memory second pass, no leak.
func TestListTool_OwnershipIsEnforcedByStore(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-alice")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("handler: unexpected error: %v", err)
	}
	if store.captured.userID != "u-alice" {
		t.Errorf("userID: got %q, want %q (must be the caller's ID, not empty)",
			store.captured.userID, "u-alice")
	}
}

// TestListTool_MalformedInput_SurfacesParseError: bad JSON must not
// panic.
func TestListTool_MalformedInput_SurfacesParseError(t *testing.T) {
	t.Parallel()
	store := &fakeListStore{rows: nil}
	h := backtesttool.ListHandler(store, "u-1")

	_, err := h(t.Context(), "tool-call", json.RawMessage(`{not json`))
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
}

// contains is a tiny strings.Contains shim so this file doesn't need
// to import "strings" just for one call.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
