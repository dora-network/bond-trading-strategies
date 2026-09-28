package httpapi

// Shared test fakes for the production runner and the shared strategy-turn
// driver. Migrated from the original runner tests; both the strategy runner
// tests (strategy_runner_test.go) and the shared-driver tests (turn_test.go)
// compose these.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate"
)

// fakeAuditWriter records every audit insert.
type fakeAuditWriter struct {
	rows []auditRow
}

type auditRow struct {
	userID string
	action string
	detail []byte
}

func (f *fakeAuditWriter) Insert(_ context.Context, userID, action string, detail []byte) error {
	f.rows = append(f.rows, auditRow{userID: userID, action: action, detail: detail})
	return nil
}

func (f *fakeAuditWriter) has(action string) bool {
	for _, r := range f.rows {
		if r.action == action {
			return true
		}
	}
	return false
}

// fakeClassifierFactory returns a fixed Classifier for any (provider, model).
type fakeClassifierFactory struct {
	verdict sanitize.ClassifyVerdict
	err     error
	prompt  string
}

func (f *fakeClassifierFactory) factory() ClassifierFactory {
	return func(_ llm.Provider, _ string) Classifier {
		return &fakeClassifier{verdict: f.verdict, err: f.err, record: &f.prompt}
	}
}

type fakeClassifier struct {
	verdict sanitize.ClassifyVerdict
	err     error
	record  *string
}

func (c *fakeClassifier) Classify(_ context.Context, prompt, _ string) (sanitize.ClassifyVerdict, error) {
	*c.record = prompt
	return c.verdict, c.err
}

// collectingEmit captures emitted SSE payloads.
type collectingEmit struct {
	events []EventPayload
}

func (c *collectingEmit) emit(e EventPayload) { c.events = append(c.events, e) }

func (c *collectingEmit) hasEventType(want string) bool {
	for _, e := range c.events {
		switch e.(type) {
		case TextPayload:
			if want == "delta" {
				return true
			}
		case DonePayload:
			if want == "done" {
				return true
			}
		case ErrorPayload:
			if want == "error" {
				return true
			}
		case RefusalPayload:
			if want == "refusal" {
				return true
			}
		}
	}
	return false
}

// eventsContain reports whether any captured event has the same dynamic
// type as want. Used by tests that want to assert a specific payload
// type was emitted without caring about its fields.
func (c *collectingEmit) eventsContain(want EventPayload) bool {
	wantType := fmt.Sprintf("%T", want)
	for _, e := range c.events {
		if fmt.Sprintf("%T", e) == wantType {
			return true
		}
	}
	return false
}

// scriptProvider replays a fixed sequence of event channels, one per Stream
// call, letting a test script multi-iteration tool loops.
type scriptProvider struct {
	scripts [][]llm.Event
	calls   int
}

func (p *scriptProvider) Stream(_ context.Context, _ llm.Request) (<-chan llm.Event, error) {
	idx := p.calls
	p.calls++
	ch := make(chan llm.Event, len(p.scripts[idx]))
	for _, e := range p.scripts[idx] {
		ch <- e
	}
	close(ch)
	return ch, nil
}

// providerFactoryFor wraps a fixed provider behind the ProviderFactory
// signature, ignoring provider/model selection.
func providerFactoryFor(p llm.Provider) ProviderFactory {
	return func(_ context.Context, _, _, _ string) (llm.Provider, string, error) {
		return p, "test-model", nil
	}
}

// toolFactoryFor wraps a fixed tool set behind the ToolFactory signature,
// ignoring the per-request Dora API key and userID (tests inject canned handlers).
func toolFactoryFor(specs []llm.ToolSpec, handlers map[string]llm.ToolHandler) ToolFactory {
	return func(_, _ string) ([]llm.ToolSpec, map[string]llm.ToolHandler) {
		return specs, handlers
	}
}

// fakeGenerateHandler returns a canned tool output and records the call.
type fakeGenerateHandler struct {
	out json.RawMessage
	err error
}

func (f *fakeGenerateHandler) invoke(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
	return f.out, f.err
}

// okValidator always passes validation.
type okValidator struct{}

func (okValidator) Validate(_ context.Context, _ map[string]string) (generate.Result, error) {
	return generate.Result{BuildOK: true, VetOK: true, TestsOK: true, GoVersion: "1.26.5"}, nil
}

// nilRepairer returns a fresh Repairer over an always-ok validator. Tests that
// inject a canned generate_strategy output never drive the repair loop, so the
// validator never runs; nil-safe when the production runner wires OnRepair.
func nilRepairer() *generate.Repairer {
	return generate.NewRepairer(&okValidator{}, 0)
}

// scriptValidator returns canned Result values from a slice, in order. Once
// the slice is exhausted it returns a bare success. Used to drive the
// repair loop through a scripted sequence of failures.
type scriptValidator struct {
	results []generate.Result
	calls   int
}

func (v *scriptValidator) Validate(_ context.Context, _ map[string]string) (generate.Result, error) {
	idx := v.calls
	v.calls++
	if idx >= len(v.results) {
		return generate.Result{BuildOK: true, VetOK: true, TestsOK: true}, nil
	}
	return v.results[idx], nil
}

// mustMarshal is the test helper variant of mustMarshalDetail; it fails the
// test on a marshal error (which should never happen for map/struct inputs).
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("mustMarshal: %v", err)
	}
	return b
}
