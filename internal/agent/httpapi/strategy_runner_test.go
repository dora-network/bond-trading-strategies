package httpapi

// strategy_runner_test.go exercises the StrategyRunner's version-capture
// behavior: a verified generate_strategy records a version (Capture); a capture
// failure stashes the artifact (StashPending) and audits, without failing the
// turn.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
)

func TestStrategyRunner_CaptureOnVerified(t *testing.T) {
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: json.RawMessage(`{}`)}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	gen := &fakeGenerateHandler{out: verifiedArtifact("mod-x")}
	cap := &captureSpy{}
	audit := &fakeAuditWriter{}

	runner := NewStrategyRunner(providerFactoryFor(provider), cls.factory(),
		toolFactoryFor([]llm.ToolSpec{{Name: "generate_strategy"}},
			map[string]llm.ToolHandler{"generate_strategy": gen.invoke}),
		audit, cap, nilRepairer(), config.ModelCaps{Default: 128000}, 0, 0)
	em := &collectingEmit{}
	text, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "m", nil, "build a strategy", em.emit)
	if err != nil || !strings.Contains(text, "done") {
		t.Fatalf("Run: err=%v text=%q", err, text)
	}
	if cap.calls != 1 {
		t.Errorf("capture calls: want 1, got %d", cap.calls)
	}
	if cap.lastModule != "mod-x" {
		t.Errorf("captured module: want mod-x, got %q", cap.lastModule)
	}
	// First version (ParentRevision == "") records strategy.created.
	if !audit.has("strategy.created") {
		t.Errorf("want strategy.created audit row; got %+v", audit.rows)
	}
}

func TestStrategyRunner_CaptureFailure_StashesAndCompletes(t *testing.T) {
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: json.RawMessage(`{}`)}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	gen := &fakeGenerateHandler{out: verifiedArtifact("mod-x")}
	cap := &captureSpy{fail: true}
	audit := &fakeAuditWriter{}
	em := &collectingEmit{}

	runner := NewStrategyRunner(providerFactoryFor(provider), cls.factory(),
		toolFactoryFor([]llm.ToolSpec{{Name: "generate_strategy"}},
			map[string]llm.ToolHandler{"generate_strategy": gen.invoke}),
		audit, cap, nilRepairer(), config.ModelCaps{Default: 128000}, 0, 0)
	text, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "m", nil, "build a strategy", em.emit)
	if err != nil || !strings.Contains(text, "done") {
		t.Fatalf("turn should still complete: err=%v text=%q", err, text)
	}
	if cap.stashed != 1 {
		t.Errorf("stash calls: want 1, got %d", cap.stashed)
	}
	if !audit.has("strategy.capture_failed") {
		t.Errorf("want strategy.capture_failed audit row; got %+v", audit.rows)
	}
	if !em.hasEventType("error") {
		t.Error("capture failure must emit a non-fatal error SSE notice")
	}
}

// captureSpy is a minimal strategies.Store recording capture/stash calls.
// Embedding the interface panics on any unimplemented method, which is fine —
// only Capture + StashPending are exercised here.
type captureSpy struct {
	strategies.Store
	calls, stashed                           int
	lastModule, lastImageRef, lastStrategyID string
	lastRevision                             strategies.Revision
	fail                                     bool
	// WASM capture state.
	wasmCalls                     int
	lastWasmRef, lastManifestHash string
}

func (c *captureSpy) Capture(_ context.Context, _, _, _, _ string, m strategies.Meta, _ map[string]string, imageRef string) (strategies.Version, error) {
	c.calls++
	c.lastModule = m.ModuleName
	c.lastImageRef = imageRef
	if c.fail {
		return strategies.Version{}, errCaptureInjected
	}
	v := strategies.Version{StrategyID: "strat-1", Revision: "rev", Meta: m}
	c.lastStrategyID = v.StrategyID
	c.lastRevision = v.Revision
	return v, nil
}

func (c *captureSpy) CaptureWASM(_ context.Context, _, _, _, _ string, m strategies.Meta, _ map[string]string, wasmRef, manifestHash string) (strategies.Version, error) {
	c.wasmCalls++
	c.lastModule = m.ModuleName
	c.lastWasmRef = wasmRef
	c.lastManifestHash = manifestHash
	if c.fail {
		return strategies.Version{}, errCaptureInjected
	}
	v := strategies.Version{StrategyID: "strat-1", Revision: "rev-wasm", Meta: m}
	c.lastStrategyID = v.StrategyID
	c.lastRevision = v.Revision
	return v, nil
}

func (c *captureSpy) StashPending(_ context.Context, _, _, _, _ string, _ strategies.Meta, _ map[string]string, _ string) error {
	c.stashed++
	return nil
}

var errCaptureInjected = errors.New("injected capture failure")

func verifiedArtifact(module string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"module_name": module, "verified": true, "summary": "s",
		"rationale": "r", "files": map[string]string{"main.go": "package main"},
		"validation": map[string]any{"build_ok": true},
	})
	return b
}

// validGenInput is a minimal generate_strategy payload that passes
// ValidateInput (module_name, summary, rationale, main.go, go.mod, a test).
var validGenInput = json.RawMessage(`{` +
	`"module_name":"x","summary":"s","rationale":"r",` +
	`"files":[{"path":"main.go","content":"package main"},{"path":"go.mod","content":"module x"},{"path":"strategy.go","content":"package main"}]}`)

// TestStrategyRunner_RepairWritesAuditRows asserts the runner wires the
// Repairer.OnRepair callback so every failed repair attempt emits a
// strategy.repair audit row carrying the attempt number, the first
// diagnostic's category, and its message (spec §10, §16.5).
func TestStrategyRunner_RepairWritesAuditRows(t *testing.T) {
	// Validator sequence: fail, fail, success -> two failed attempts
	// (attempt 0 and attempt 1), each emitting a strategy.repair row.
	fail := generate.Result{Diagnostics: []generate.Diagnostic{{
		Category: "build_error", Message: "undefined: foo",
	}}}
	validator := &scriptValidator{results: []generate.Result{fail, fail}}
	repairer := generate.NewRepairer(validator, 2)

	gen := generate.NewHandler(repairer, nil, migration.New(), generate.Config{MaxFiles: 50, MaxBytes: 1 << 20, MaxRepairs: 2})
	provider := &scriptProvider{scripts: [][]llm.Event{
		// Iteration 1: model requests generate_strategy with valid input.
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: validGenInput}}, llm.DoneEvent{}},
		// Iteration 2: model ends the turn.
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	cap := &captureSpy{}

	runner := NewStrategyRunner(
		providerFactoryFor(provider), cls.factory(),
		toolFactoryFor(gen.Tools(), map[string]llm.ToolHandler{"generate_strategy": gen.Invoke}),
		audit, cap, repairer,
		config.ModelCaps{Default: 128000},
		0, 0,
	)

	em := &collectingEmit{}
	if _, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "m", nil, "build a strategy", em.emit); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Collect strategy.repair rows in order.
	var repairs []auditRow
	for _, r := range audit.rows {
		if r.action == "strategy.repair" {
			repairs = append(repairs, r)
		}
	}
	if len(repairs) != 2 {
		t.Fatalf("strategy.repair rows: want 2, got %d (%+v)", len(repairs), audit.rows)
	}
	for i, r := range repairs {
		var d struct {
			Attempt  int    `json:"attempt"`
			Category string `json:"category"`
			Message  string `json:"message"`
		}
		if err := json.Unmarshal(r.detail, &d); err != nil {
			t.Fatalf("repair row %d detail: %v", i, err)
		}
		if d.Attempt != i {
			t.Errorf("repair row %d attempt: want %d, got %d", i, i, d.Attempt)
		}
		if d.Category != "build_error" {
			t.Errorf("repair row %d category: want build_error, got %q", i, d.Category)
		}
		if d.Message != "undefined: foo" {
			t.Errorf("repair row %d message: want %q, got %q", i, "undefined: foo", d.Message)
		}
	}
}

// TestCaptureObserver_WrapPatchesStrategyIDAndRevision: the LLM-visible
// tool result must include strategy_id + revision_id so the LLM can
// chain into run_backtest without a separate get_strategy lookup.
// Otherwise the LLM invents a strategy_id (the prior bug).
func TestCaptureObserver_WrapPatchesStrategyIDAndRevision(t *testing.T) {
	t.Parallel()
	cap := &captureSpy{}
	observer := &captureObserver{
		audit:     &fakeAuditWriter{},
		versions:  cap,
		userID:    "u-1",
		sessionID: "sess-1",
		provider:  "openai",
		model:     "gpt",
		emit:      func(EventPayload) {},
	}
	inner := func(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
		return verifiedArtifact("mod-x"), nil
	}
	wrapped := observer.wrap(inner)
	out, err := wrapped(t.Context(), "generate_strategy", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("wrapped invoke: %v", err)
	}
	if !strings.Contains(string(out), `"strategy_id":"strat-1"`) {
		t.Errorf("LLM-visible result missing strategy_id; got %q", out)
	}
	if !strings.Contains(string(out), `"revision_id":"rev"`) {
		t.Errorf("LLM-visible result missing revision_id; got %q", out)
	}
	if cap.calls != 1 {
		t.Errorf("capture calls: want 1, got %d", cap.calls)
	}
}

func TestStrategyRunner_Capture_RecordsImageRef(t *testing.T) {
	// A verified artifact carrying image_ref flows into Meta.ImageRef
	// on the Capture call. The captureSpy stores it for assertion.
	gen := &fakeGenerateHandler{out: verifiedArtifactWithImageRef("mod-x", "strategy-rev-7")}
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: verifiedArtifactWithImageRef("mod-x", "strategy-rev-7")}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	cap := &captureSpy{}
	audit := &fakeAuditWriter{}
	runner := NewStrategyRunner(providerFactoryFor(provider), cls.factory(),
		toolFactoryFor([]llm.ToolSpec{{Name: "generate_strategy"}},
			map[string]llm.ToolHandler{"generate_strategy": gen.invoke}),
		audit, cap, nilRepairer(), config.ModelCaps{Default: 128000}, 0, 0)
	em := &collectingEmit{}
	if _, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "m", nil, "build a strategy", em.emit); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cap.calls != 1 {
		t.Fatalf("capture calls: want 1, got %d", cap.calls)
	}
	if cap.lastImageRef != "strategy-rev-7" {
		t.Errorf("Meta.ImageRef on Capture: want strategy-rev-7, got %q", cap.lastImageRef)
	}
}

// verifiedArtifactWithImageRef builds a verified artifact carrying an
// image_ref field, mirroring the validator's Result surface for tests.
func verifiedArtifactWithImageRef(module, imageRef string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"module_name": module, "verified": true, "summary": "s",
		"rationale": "r", "files": map[string]string{"main.go": "package main"},
		"validation": map[string]any{"build_ok": true, "image_ref": imageRef},
		"image_ref":  imageRef,
	})
	return b
}

func TestStrategyRunner_Capture_RecordsWasmRef(t *testing.T) {
	// A verified WASM artifact carrying wasm_ref + manifest_hash flows
	// into CaptureWASM, not Capture.
	gen := &fakeGenerateHandler{out: verifiedArtifactWithWasmRef("mod-wasm", "wasm-sha", "manifest-sha")}
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: verifiedArtifactWithWasmRef("mod-wasm", "wasm-sha", "manifest-sha")}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	cap := &captureSpy{}
	audit := &fakeAuditWriter{}
	em := &collectingEmit{}
	runner := NewStrategyRunner(providerFactoryFor(provider), cls.factory(),
		toolFactoryFor([]llm.ToolSpec{{Name: "generate_strategy"}},
			map[string]llm.ToolHandler{"generate_strategy": gen.invoke}),
		audit, cap, nilRepairer(), config.ModelCaps{Default: 128000}, 0, 0)
	if _, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "m", nil, "build a strategy", em.emit); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cap.calls != 0 {
		t.Errorf("Capture (docker) calls: want 0, got %d", cap.calls)
	}
	if cap.wasmCalls != 1 {
		t.Fatalf("CaptureWASM calls: want 1, got %d", cap.wasmCalls)
	}
	if cap.lastWasmRef != "wasm-sha" {
		t.Errorf("wasm_ref: want wasm-sha, got %q", cap.lastWasmRef)
	}
	if cap.lastManifestHash != "manifest-sha" {
		t.Errorf("manifest_hash: want manifest-sha, got %q", cap.lastManifestHash)
	}
}

// verifiedArtifactWithWasmRef builds a verified WASM artifact carrying
// wasm_ref + manifest_hash fields, mirroring the validator's Result.
func verifiedArtifactWithWasmRef(module, wasmRef, manifestHash string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"module_name": module, "verified": true, "summary": "s",
		"rationale": "r", "files": map[string]string{"main.go": "package main"},
		"validation":    map[string]any{"build_ok": true, "wasm_ref": wasmRef, "manifest_hash": manifestHash},
		"wasm_ref":      wasmRef,
		"manifest_hash": manifestHash,
	})
	return b
}

// capturingProvider records the MaxTokens the agent passes to
// provider.Stream so the regression test below can assert the
// runner forwarded r.modelCaps into the request.
type capturingProvider struct {
	script     [][]llm.Event
	calls      int
	lastMaxTok int
}

func (p *capturingProvider) Stream(_ context.Context, req llm.Request) (<-chan llm.Event, error) {
	p.calls++
	p.lastMaxTok = req.MaxTokens
	idx := p.calls - 1
	if idx >= len(p.script) {
		ch := make(chan llm.Event)
		close(ch)
		return ch, nil
	}
	events := p.script[idx]
	ch := make(chan llm.Event, len(events))
	for _, e := range events {
		ch <- e
	}
	close(ch)
	return ch, nil
}

// TestStrategyRunner_ForwardsModelCaps asserts that StrategyRunner.Run
// passes its modelCaps field into the agent loop so the strategy
// hop sets a non-zero MaxTokens on each request. The strategy5 session
// hit max_tokens=0 in production because this wiring was missing
// (r.modelCaps stayed on the runner but never reached turnDeps, so
// d.modelCaps was the zero value and MaxTokensFor returned 0).
func TestStrategyRunner_ForwardsModelCaps(t *testing.T) {
	// Two-stream script: first the classifier hop (tools=0, messages=2)
	// returns a short OK verdict; second the strategy hop
	// (tools=9, messages=6) returns a short assistant turn.
	cp := &capturingProvider{script: [][]llm.Event{
		{llm.TextDelta{Text: "ok"}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	cap := &captureSpy{}
	audit := &fakeAuditWriter{}

	// Per-model table with claude-sonnet-5 at 128k. If the runner
	// forwards this to turnDeps, the agent's request to provider.Stream
	// will carry MaxTokens=128000. Without the forwarding, the request
	// carries MaxTokens=0.
	caps := config.ModelCaps{
		Default: 16384,
		Models: []config.ModelCap{
			{Match: "test-model", MaxTokens: 128000},
		},
	}
	runner := NewStrategyRunner(
		providerFactoryFor(cp),
		cls.factory(),
		toolFactoryFor(nil, map[string]llm.ToolHandler{}),
		audit, cap, nilRepairer(), caps,
		0, 0,
	)

	if _, err := runner.Run(t.Context(), "u-1", "sess-1", "openai", "claude-sonnet-5", nil, "build a strategy",
		func(_ EventPayload) {}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The classifier stub returns the verdict synchronously so the
	// provider only sees the strategy hop: cp.calls == 1.
	if cp.calls != 1 {
		t.Fatalf("provider.Stream calls: want 1 (strategy hop), got %d", cp.calls)
	}
	if cp.lastMaxTok != 128000 {
		t.Errorf("strategy-hop MaxTokens: want 128000 (claude-sonnet-5 cap), got %d", cp.lastMaxTok)
	}
}
