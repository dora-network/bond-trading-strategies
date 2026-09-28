package llm

import (
	"strings"
	"testing"
)

// Compile-time assertions that each concrete event type implements the
// sealed Event interface. If a future change removes isEvent() from any
// of these types, the build fails with "does not implement Event".
var (
	_ Event = TextDelta{}
	_ Event = ToolCallEvent{}
	_ Event = DoneEvent{}
	_ Event = ErrorEvent{}
)

// ToolCall also appears in the sealed surface (ToolCallEvent.Call).
var _ = ToolCall{ID: "1", Name: "n"}

// Explicit field-roundtrip test that still asserts behavior, not just types.
// This covers the actual sealed surface via a single concrete type.
func TestTextDelta_Fields(t *testing.T) {
	d := TextDelta{Text: "hi"}
	if d.Text != "hi" {
		t.Errorf("TextDelta.Text: want \"hi\", got %q", d.Text)
	}
	if d := (DoneEvent{Reason: "complete"}); d.Reason != "complete" {
		t.Errorf("DoneEvent.Reason round-trip failed: %+v", d)
	}
	if e := (ErrorEvent{}); e.Err != nil {
		t.Errorf("ErrorEvent default Err: want nil, got %v", e.Err)
	}
}

func TestNewRecoveryHintError_FallbackOnMarshalFailure(t *testing.T) {
	// channels are not JSON-serialisable; NewRecoveryHintError must
	// return a RecoveryError with a nil Hint, not panic.
	got := NewRecoveryHintError("msg", "tool", make(chan int))
	if got.Hint() != nil {
		t.Fatalf("want nil Hint on marshal failure, got %s", string(got.Hint()))
	}
	if got.RecoveryTool() != "tool" {
		t.Fatalf("want RecoveryTool()=tool, got %q", got.RecoveryTool())
	}
	if got.Error() != "msg" {
		t.Fatalf("want Error()=msg, got %q", got.Error())
	}
}

func TestNewRecoveryHintError_MarshalsValidPayload(t *testing.T) {
	got := NewRecoveryHintError("msg", "tool", struct {
		Action     string `json:"action"`
		StrategyID string `json:"strategy_id"`
	}{Action: "rebuild", StrategyID: "sid"})
	hint := string(got.Hint())
	if hint == "" {
		t.Fatal("expected non-empty Hint on serialisable payload")
	}
	if !strings.Contains(hint, `"action":"rebuild"`) || !strings.Contains(hint, `"strategy_id":"sid"`) {
		t.Fatalf("hint missing expected fields: %s", hint)
	}
}
