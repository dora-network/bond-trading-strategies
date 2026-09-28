package httpapi

// turn_test.go drives the shared strategy turn driver (runStrategyTurn)
// directly. Each case drives the driver with the same inputs the runner
// tests used, asserting the same audit rows and SSE outcomes. The behavior
// is unchanged; only the entry point moved.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
)

// newTurnDeps builds a turnDeps wired with the standard fakes and a tool set
// built from an always-ok validator. Tests override fields as needed.
func newTurnDeps(t *testing.T, provider llm.Provider, cls *fakeClassifierFactory, audit AuditWriter) turnDeps {
	t.Helper()
	rep := generate.NewRepairer(&okValidator{}, 0)
	gen := generate.NewHandler(rep, nil, migration.New(), generate.Config{MaxFiles: 50, MaxBytes: 1 << 20, MaxRepairs: 2})
	return turnDeps{
		userID: "u-1", sessionID: "sess-1", provider: "openai", model: "m",
		prompt:        "build a bond strategy",
		newProvider:   providerFactoryFor(provider),
		newClassifier: cls.factory(),
		newTools:      toolFactoryFor(gen.Tools(), map[string]llm.ToolHandler{"generate_strategy": gen.Invoke}),
		audit:         audit,
		modelCaps:     config.ModelCaps{Default: 128000},
	}
}

func TestTurn_FilterRejectsOversize(t *testing.T) {
	provider := &scriptProvider{}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, &fakeClassifierFactory{}, audit)
	d.prompt = strings.Repeat("a", sanitize.MaxPromptBytes+1)

	em := &collectingEmit{}
	d.emit = em.emit
	text, err := runStrategyTurn(t.Context(), d)

	if err == nil {
		t.Fatal("want error on oversize prompt")
	}
	if text != "" {
		t.Errorf("text: want empty, got %q", text)
	}
	if !em.hasEventType("error") {
		t.Error("want error SSE event on filter rejection")
	}
	if !audit.has("message.rejected") {
		t.Errorf("want message.rejected audit row; got %+v", audit.rows)
	}
	if provider.calls != 0 {
		t.Errorf("provider must not be called on filter rejection; calls=%d", provider.calls)
	}
}

func TestTurn_FilterRejectsControlChars(t *testing.T) {
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, &scriptProvider{}, &fakeClassifierFactory{}, audit)
	d.prompt = "hello\x00world"

	em := &collectingEmit{}
	d.emit = em.emit
	_, err := runStrategyTurn(t.Context(), d)
	if err == nil {
		t.Fatal("want error on control chars")
	}
	if !audit.has("message.rejected") {
		t.Error("want message.rejected audit row")
	}
}

func TestTurn_ProviderError(t *testing.T) {
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, &scriptProvider{}, &fakeClassifierFactory{}, audit)
	d.newProvider = func(_ context.Context, _, _, _ string) (llm.Provider, string, error) {
		return nil, "", errors.New("no provider config")
	}

	em := &collectingEmit{}
	d.emit = em.emit
	_, err := runStrategyTurn(t.Context(), d)
	if err == nil {
		t.Fatal("want error on provider failure")
	}
	if !em.hasEventType("error") {
		t.Error("provider failure must emit error SSE")
	}
}

func TestTurn_ClassifierOK_ProceedsToStrategy(t *testing.T) {
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.TextDelta{Text: "building strategy"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)

	em := &collectingEmit{}
	d.emit = em.emit
	text, err := runStrategyTurn(t.Context(), d)
	if err != nil {
		t.Fatalf("runStrategyTurn: %v", err)
	}
	if !strings.Contains(text, "building strategy") {
		t.Errorf("text: want 'building strategy', got %q", text)
	}
	if !em.hasEventType("delta") || !em.hasEventType("done") {
		t.Errorf("want delta+done events; got %+v", em.events)
	}
}

func TestTurn_ClassifierOffTopic_PersistsRefusal(t *testing.T) {
	provider := &scriptProvider{}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "off_topic", Reason: "not about bonds"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)

	em := &collectingEmit{}
	d.emit = em.emit
	text, err := runStrategyTurn(t.Context(), d)
	if err != nil {
		t.Fatalf("runStrategyTurn: %v", err)
	}
	if text == "" {
		t.Error("off_topic must persist a one-line refusal as returned text")
	}
	if !em.hasEventType("done") {
		t.Error("off_topic turn must end with done")
	}
	if !em.hasEventType("refusal") {
		t.Error("off_topic turn must emit refusal SSE event before done")
	}

	if em.hasEventType("error") {
		t.Error("off_topic must NOT emit error")
	}
	if !audit.has("strategy.refused") {
		t.Errorf("want strategy.refused audit row; got %+v", audit.rows)
	}
	if provider.calls != 0 {
		t.Errorf("strategy hop must not run on off_topic; calls=%d", provider.calls)
	}
}

func TestTurn_ClassifierAdversarial_ClosesWithError(t *testing.T) {
	provider := &scriptProvider{}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "adversarial", Reason: "jailbreak"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)

	em := &collectingEmit{}
	d.emit = em.emit
	text, err := runStrategyTurn(t.Context(), d)
	if err == nil {
		t.Fatal("adversarial must return an error")
	}
	if text != "" {
		t.Errorf("adversarial must not persist model output; got %q", text)
	}
	if !em.hasEventType("error") {
		t.Error("adversarial must close with error SSE")
	}
	if !audit.has("message.rejected") {
		t.Errorf("want message.rejected audit row; got %+v", audit.rows)
	}
	if provider.calls != 0 {
		t.Errorf("strategy hop must not run on adversarial; calls=%d", provider.calls)
	}
}

func TestTurn_ClassifierError_ClosesWithError(t *testing.T) {
	provider := &scriptProvider{}
	cls := &fakeClassifierFactory{err: errors.New("classify boom")}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)

	em := &collectingEmit{}
	d.emit = em.emit
	_, err := runStrategyTurn(t.Context(), d)
	if err == nil {
		t.Fatal("want error on classifier failure")
	}
	if !em.hasEventType("error") {
		t.Error("classifier failure must emit error SSE")
	}
	if !audit.has("message.rejected") {
		t.Error("classifier failure must write message.rejected")
	}
}

func TestClassifyUserMessage_ProviderErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "provider stream event error",
			err:  fmt.Errorf("sanitize: classify stream event: [openai] provider_error: 402 Payment Required"),
			want: "upstream LLM error: [openai] provider_error: 402 Payment Required",
		},
		{
			name: "provider stream sync error",
			err:  fmt.Errorf("sanitize: classify stream: timeout"),
			want: "upstream LLM error: timeout",
		},
		{
			name: "classifier parse error",
			err:  fmt.Errorf("sanitize: parse classify JSON: unexpected token"),
			want: "could not classify the message",
		},
		{
			name: "unknown verdict error",
			err:  fmt.Errorf("sanitize: unknown classify verdict \"hack\""),
			want: "could not classify the message",
		},
		{
			name: "bare errors.New",
			err:  errors.New("classify boom"),
			want: "could not classify the message",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyUserMessage(tt.err)
			if got != tt.want {
				t.Errorf("classifyUserMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTurn_GenerateFailed_FeedsBackToLLM: a tool-handler error is
// NOT terminal anymore -- the agent loop now feeds the error back
// to the LLM as a tool_result so it can decide what to do next
// (retry with different input, fall back to a different tool,
// give up). With a single-sequence provider the LLM has no chance
// to recover, so the loop hits "no sequence for call" on the
// second iteration and returns. The test asserts the turn
// completes without panicking and emits the tool_call event so
// the SSE stream stays well-formed.
func TestTurn_GenerateFailed_FeedsBackToLLM(t *testing.T) {
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: []byte(`{}`)}}, llm.DoneEvent{}},
		{llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)
	d.newTools = toolFactoryFor(nil, map[string]llm.ToolHandler{
		"generate_strategy": (&fakeGenerateHandler{err: errors.New("gen boom")}).invoke,
	})

	em := &collectingEmit{}
	d.emit = em.emit
	// The agent loop continues past the tool error. The second
	// iteration's provider call returns an error (no sequence),
	// which the loop surfaces as the run's return value.
	_, err := runStrategyTurn(t.Context(), d)
	if err == nil {
		t.Fatal("run should error when provider has no second sequence")
	}
	if !em.eventsContain(ToolCallPayload{}) {
		t.Error("tool_call event must be emitted before the error surfaces")
	}
}

func TestTurn_GenerateFailed_ObserverAudits(t *testing.T) {
	// With the capture observer wired (as the production runner does), a
	// generate handler error writes a strategy.failed audit row before the
	// terminal error surfaces.
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: []byte(`{}`)}}, llm.DoneEvent{}},
		{llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)
	d.newTools = toolFactoryFor(nil, map[string]llm.ToolHandler{
		"generate_strategy": (&fakeGenerateHandler{err: errors.New("gen boom")}).invoke,
	})
	obs := &captureObserver{audit: audit, versions: nilStore{}}
	d.observeGenerate = obs.wrap

	em := &collectingEmit{}
	d.emit = em.emit
	_, _ = runStrategyTurn(t.Context(), d)
	if !audit.has("strategy.failed") {
		t.Errorf("generate handler error must write strategy.failed; got %+v", audit.rows)
	}
}

func TestTurn_GenerateSourcePattern_ObserverRejects(t *testing.T) {
	// With an observer wired (as the production runner does), a source_pattern
	// output writes message.rejected before capture is reached.
	patOut := mustMarshal(t, map[string]any{
		"error": "source_pattern", "file": "main.go", "line": 3, "pattern": "exec.Command",
	})
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: []byte(`{}`)}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "sorry"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	d := newTurnDeps(t, provider, cls, audit)
	d.newTools = toolFactoryFor(nil, map[string]llm.ToolHandler{
		"generate_strategy": (&fakeGenerateHandler{out: patOut}).invoke,
	})
	obs := &captureObserver{audit: audit, versions: nilStore{}}
	d.observeGenerate = obs.wrap

	em := &collectingEmit{}
	d.emit = em.emit
	_, _ = runStrategyTurn(t.Context(), d)
	if !audit.has("message.rejected") {
		t.Errorf("source_pattern hit must write message.rejected; got %+v", audit.rows)
	}
}

func TestTurn_GenerateSuccess_ObserverCaptures(t *testing.T) {
	// With an observer wired, a verified artifact is captured. The captureSpy
	// records the call; the observer audits strategy.created (first version).
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: []byte(`{}`)}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	cap := &captureSpy{}
	d := newTurnDeps(t, provider, cls, audit)
	d.newTools = toolFactoryFor(nil, map[string]llm.ToolHandler{
		"generate_strategy": (&fakeGenerateHandler{out: verifiedArtifact("mod-t")}).invoke,
	})
	em := &collectingEmit{}
	obs := &captureObserver{audit: audit, versions: cap, emit: em.emit}
	d.observeGenerate = obs.wrap
	d.emit = em.emit
	text, err := runStrategyTurn(t.Context(), d)
	if err != nil {
		t.Fatalf("runStrategyTurn: %v", err)
	}
	if !strings.Contains(text, "done") {
		t.Errorf("text: want 'done', got %q", text)
	}
	if cap.calls != 1 {
		t.Errorf("capture calls: want 1, got %d", cap.calls)
	}
	if !audit.has("strategy.created") {
		t.Errorf("want strategy.created audit row; got %+v", audit.rows)
	}
	// The observer must surface StrategySavedPayload so the chat UI can
	// fetch the version's source files. captureSpy records the strategyID
	// + revision returned by Capture; the payload must carry the same pair.
	if !em.eventsContain(StrategySavedPayload{}) {
		t.Fatalf("want StrategySavedPayload emitted; got %v", em.events)
	}
	for _, ev := range em.events {
		p, ok := ev.(StrategySavedPayload)
		if !ok {
			continue
		}
		if p.StrategyID != cap.lastStrategyID || p.Revision != string(cap.lastRevision) {
			t.Errorf("strategy_saved payload (id=%q rev=%q) does not match capture (id=%q rev=%q)",
				p.StrategyID, p.Revision, cap.lastStrategyID, cap.lastRevision)
		}
	}
}

func TestTurn_GenerateUnverified_ObserverAuditsFailed(t *testing.T) {
	// An unverified artifact (validation failed) is not captured, but the
	// observer must record strategy.failed so the drop is not silent (spec §10).
	unverified := mustMarshal(t, map[string]any{
		"module_name": "mod-u", "verified": false, "summary": "s", "files": map[string]string{},
	})
	provider := &scriptProvider{scripts: [][]llm.Event{
		{llm.ToolCallEvent{Call: llm.ToolCall{ID: "c1", Name: "generate_strategy", Input: []byte(`{}`)}}, llm.DoneEvent{}},
		{llm.TextDelta{Text: "done"}, llm.DoneEvent{}},
	}}
	cls := &fakeClassifierFactory{verdict: sanitize.ClassifyVerdict{Verdict: "ok"}}
	audit := &fakeAuditWriter{}
	cap := &captureSpy{}
	d := newTurnDeps(t, provider, cls, audit)
	d.newTools = toolFactoryFor(nil, map[string]llm.ToolHandler{
		"generate_strategy": (&fakeGenerateHandler{out: unverified}).invoke,
	})
	obs := &captureObserver{audit: audit, versions: cap}
	d.observeGenerate = obs.wrap

	em := &collectingEmit{}
	d.emit = em.emit
	_, _ = runStrategyTurn(t.Context(), d)
	if cap.calls != 0 {
		t.Errorf("unverified artifact must not be captured; got %d calls", cap.calls)
	}
	if !audit.has("strategy.failed") {
		t.Errorf("unverified artifact must write strategy.failed; got %+v", audit.rows)
	}
}

// nilStore is a strategies.Store whose capture path is never reached by the
// source-pattern test; it exists only so the observer's versions field is
// non-nil. Embedding the interface satisfies the type without implementing the
// unused methods.
type nilStore struct{ strategies.Store }

// TestMaxTokensForModel pins the per-model table loaded from
// configs/model_caps.json. The substring match must resolve
// OpenRouter's "provider/model" prefix to the same ceiling as the
// bare model name. Unknown models fall back to the configured
// default.
func TestMaxTokensForModel(t *testing.T) {
	caps := config.ModelCaps{
		Default: 128000,
		Models: []config.ModelCap{
			{Match: "claude-sonnet-5", MaxTokens: 128000},
			{Match: "claude-sonnet-4-5", MaxTokens: 128000},
			{Match: "claude-opus-4-5", MaxTokens: 128000},
			{Match: "claude-3-5-sonnet", MaxTokens: 8192},
			{Match: "claude-3-haiku", MaxTokens: 4096},
			{Match: "gpt-4o", MaxTokens: 16384},
			{Match: "gpt-4o-mini", MaxTokens: 16384},
			{Match: "gpt-5", MaxTokens: 32768},
			{Match: "o1", MaxTokens: 32768},
			{Match: "o3-mini", MaxTokens: 16384},
		},
	}
	cases := []struct {
		model string
		want  int
	}{
		{"claude-sonnet-5", 128000},
		{"claude-sonnet-4-5", 128000},
		{"claude-sonnet-4", 128000},
		{"claude-opus-4-5", 128000},
		{"claude-3-5-sonnet", 8192},
		{"claude-3-haiku", 4096},
		{"gpt-4o", 16384},
		{"gpt-4o-mini", 16384},
		{"gpt-5", 32768},
		{"o1", 32768},
		{"o3-mini", 16384},
		// OpenRouter-prefixed forms.
		{"anthropic/claude-sonnet-5", 128000},
		{"anthropic/claude-opus-4-5", 128000},
		{"openai/gpt-4o", 16384},
		// Case-insensitive.
		{"Claude-Sonnet-5", 128000},
		// Unknown model falls back to default (128000).
		{"some-future-model", 128000},
		{"", 128000},
	}
	for _, tc := range cases {
		got := caps.MaxTokensFor(tc.model)
		if got != tc.want {
			t.Errorf("MaxTokensFor(%q) = %d, want %d", tc.model, got, tc.want)
		}
	}
}
