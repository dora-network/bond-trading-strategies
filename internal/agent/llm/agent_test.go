package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeProvider returns a canned event sequence per Stream() call.
// recorded captures every Request the agent sends, so a test can
// inspect the messages the agent fed back to the LLM in
// subsequent iterations (used by the recovery-hint test below).
type fakeProvider struct {
	sequences [][]Event
	calls     int
	recorded  []Request
}

func (f *fakeProvider) Stream(ctx context.Context, req Request) (<-chan Event, error) {
	f.recorded = append(f.recorded, req)
	if f.calls >= len(f.sequences) {
		return nil, errors.New("fakeProvider: no sequence for call")
	}
	events := f.sequences[f.calls]
	f.calls++
	ch := make(chan Event)
	go func() {
		defer close(ch)
		for _, e := range events {
			select {
			case <-ctx.Done():
				return
			case ch <- e:
			}
		}
	}()
	return ch, nil
}

func collect(t *testing.T, a *Agent, msgs []Message) ([]Event, string, error) {
	t.Helper()
	var got []Event
	emit := func(e Event) { got = append(got, e) }
	text, err := a.Run(t.Context(), msgs, emit)
	return got, text, err
}

func TestRun_SinglePassText(t *testing.T) {
	fp := &fakeProvider{sequences: [][]Event{
		{TextDelta{Text: "hel"}, TextDelta{Text: "lo"}, DoneEvent{}},
	}}
	a := New(fp, "m")

	got, text, err := collect(t, a, []Message{{Role: RoleUser, Content: "hi"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if text != "hello" {
		t.Errorf("text: want hello, got %q", text)
	}
	if len(got) != 3 {
		t.Errorf("events: want 3, got %d", len(got))
	}
}

func TestRun_ToolCallLoop(t *testing.T) {
	fp := &fakeProvider{sequences: [][]Event{
		{ToolCallEvent{Call: ToolCall{ID: "c1", Name: "echo", Input: []byte(`{"x":"y"}`)}}},
		{TextDelta{Text: "done"}, DoneEvent{}},
	}}
	handlerCalled := false
	handlers := map[string]ToolHandler{
		"echo": func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
			handlerCalled = true
			return json.RawMessage(`{"ok":true}`), nil
		},
	}
	a := New(fp, "m", WithTools([]ToolSpec{{Name: "echo"}}, handlers))

	got, text, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if text != "done" {
		t.Errorf("text: want done, got %q", text)
	}
	if !handlerCalled {
		t.Error("tool handler was not called")
	}
	// Should have: ToolCallEvent, then TextDelta, then DoneEvent.
	if len(got) != 3 {
		t.Errorf("events: want 3, got %d (%v)", len(got), got)
	}
	if fp.calls != 2 {
		t.Errorf("provider calls: want 2, got %d", fp.calls)
	}
}

func TestRun_ToolCallNoHandler(t *testing.T) {
	fp := &fakeProvider{sequences: [][]Event{
		{ToolCallEvent{Call: ToolCall{ID: "c1", Name: "missing", Input: nil}}},
	}}
	a := New(fp, "m", WithTools(nil, nil))

	_, _, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err == nil || !strings.Contains(err.Error(), `no handler for tool "missing"`) {
		t.Errorf("err: want no-handler, got %v", err)
	}
}

// TestRun_HandlerError is folded into TestRun_ToolErrorFeedsBackAsToolResult
// below: the agent loop now feeds tool errors back to the LLM instead
// of bailing, so the previous "return error to the caller" semantics
// no longer apply.

// TestRun_ToolErrorFeedsBackAsToolResult: when a tool handler returns
// an error, the agent loop must continue instead of bailing. The
// error flows to the LLM as a tool_result message so the LLM can
// see what went wrong and decide what to do next (retry, fall back
// to a different tool, give up). Without this the LLM never gets
// a chance to react to the failure -- the run terminates the first
// time any tool returns an error.
func TestRun_ToolErrorFeedsBackAsToolResult(t *testing.T) {
	// First iteration: the LLM calls "boom" which errors. Second
	// iteration: the LLM sees the error in its tool_result and
	// calls "recover" with a different argument. Third iteration:
	// the LLM emits a final text delta and DoneEvent (clean exit).
	fp := &fakeProvider{sequences: [][]Event{
		{
			ToolCallEvent{Call: ToolCall{ID: "c1", Name: "boom", Input: json.RawMessage(`{}`)}},
			TextDelta{Text: "trying boom"},
			DoneEvent{},
		},
		{
			ToolCallEvent{Call: ToolCall{ID: "c2", Name: "recover", Input: json.RawMessage(`{"v":1}`)}},
			TextDelta{Text: "ok"},
			DoneEvent{},
		},
		{TextDelta{Text: "done"}, DoneEvent{}},
	}}
	handlers := map[string]ToolHandler{
		"boom": func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
			return nil, errors.New("kapow")
		},
		"recover": func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"v":"recovered"}`), nil
		},
	}
	a := New(fp, "m", WithTools([]ToolSpec{
		{Name: "boom"}, {Name: "recover"},
	}, handlers))

	got, text, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err != nil {
		t.Fatalf("err: want nil, got %v", err)
	}
	if !strings.Contains(text, "done") && !strings.Contains(text, "ok") {
		t.Errorf("text: want non-empty recovery output, got %q", text)
	}
	// The error from the first tool must NOT emit an ErrorEvent;
	// the loop must continue and let the LLM see the error via
	// the tool_result message.
	for _, e := range got {
		if _, ok := e.(ErrorEvent); ok {
			t.Errorf("first tool error must NOT emit ErrorEvent (the LLM should be able to recover); got %v", e)
		}
	}
}

func TestRun_MaxIterations(t *testing.T) {
	// Each iteration emits a tool call whose handler triggers another
	// tool call. With a tiny budget we hit ErrMaxIterations fast.
	toolEvent := ToolCallEvent{Call: ToolCall{ID: "c1", Name: "loop", Input: nil}}
	fp := &fakeProvider{sequences: [][]Event{
		{toolEvent}, {toolEvent}, {toolEvent},
	}}
	handlers := map[string]ToolHandler{
		"loop": func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		},
	}
	a := New(fp, "m", WithTools([]ToolSpec{{Name: "loop"}}, handlers), WithMaxIters(2))

	_, _, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if !errors.Is(err, ErrMaxIterations) {
		t.Errorf("err: want ErrMaxIterations, got %v", err)
	}
}

func TestRun_PropagatesStreamError(t *testing.T) {
	fp := &fakeProvider{sequences: [][]Event{
		{TextDelta{Text: "par"}, ErrorEvent{Err: errors.New("stream broke")}},
	}}
	a := New(fp, "m")
	_, _, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err == nil || err.Error() != "stream broke" {
		t.Errorf("err: want stream broke, got %v", err)
	}
}

func TestRun_InitialStreamError(t *testing.T) {
	a := New(&errProvider{}, "m")
	_, _, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err == nil || err.Error() != "no stream" {
		t.Errorf("err: want no stream, got %v", err)
	}
}

type errProvider struct{}

func (errProvider) Stream(ctx context.Context, _ Request) (<-chan Event, error) {
	return nil, errors.New("no stream")
}

// hangProvider never sends, but does honor ctx so the agent's per-iteration
// timeout fires and the run loop exits cleanly. Used by TestRun_LLMTimeout.
type hangProvider struct{}

func (hangProvider) Stream(ctx context.Context, _ Request) (<-chan Event, error) {
	ch := make(chan Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func TestRun_LLMTimeout(t *testing.T) {
	// 50ms is enough headroom for CI but short enough to keep the
	// test fast. WithLLMTimeout overrides the 90s DefaultLLMTimeout.
	a := New(hangProvider{}, "m", WithLLMTimeout(50*time.Millisecond))
	events, text, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err == nil {
		t.Fatalf("expected timeout error, got nil (events=%v, text=%q)", events, text)
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("err: want timeout-flavored message, got %v", err)
	}
	// The agent must surface the timeout as an ErrorEvent so the chat UI
	// sees a clear user-visible message rather than a silent hang.
	sawError := false
	for _, e := range events {
		if _, ok := e.(ErrorEvent); ok {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Errorf("expected an ErrorEvent in %v", events)
	}
}

// TestRun_ParentCancelSurfaces covers the bug where the chat UI or a proxy
// cancelled the HTTP request mid-stream: the agent's per-iteration ctx
// inherited the parent cancel, the shim closed its output channel cleanly,
// and the post-loop check must emit an ErrorEvent rather than returning
// ("", nil) silently so the SSE stream closes without a user-visible error.
func TestRun_ParentCancelSurfaces(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(t.Context())
	a := New(hangProvider{}, "m", WithLLMTimeout(2*time.Second))

	done := make(chan struct{})
	var events []Event
	var runErr error
	go func() {
		defer close(done)
		var gotText string
		gotText, runErr = a.Run(parentCtx, []Message{{Role: RoleUser, Content: "go"}}, func(e Event) {
			events = append(events, e)
		})
		_ = gotText
	}()

	// Cancel the parent mid-stream; the shim's hangProvider honors ctx.Done
	// and closes its channel. The post-loop check must distinguish this
	// from a normal completion.
	time.Sleep(20 * time.Millisecond)
	parentCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after parent cancel")
	}
	if runErr == nil {
		t.Fatalf("expected parent-cancel error, got nil (events=%v)", events)
	}
	if !strings.Contains(runErr.Error(), "cancelled by parent") {
		t.Errorf("err: want parent-cancel message, got %v", runErr)
	}
	sawError := false
	for _, e := range events {
		if _, ok := e.(ErrorEvent); ok {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Errorf("expected an ErrorEvent in %v", events)
	}
}

// TestRun_EmptyResponseSurfaces covers the case where the upstream returned
// cleanly (DoneEvent, no ctx error, no errs value) but emitted no text and
// no tool calls. The shim now logs "llm stream empty response" with the
// finish_reason that explains the cause; the agent must also surface an
// ErrorEvent so the chat UI sees a clear message instead of an SSE stream
// that closes silently. The user has no other signal that anything went
// wrong and no way to retry the turn otherwise. The MaxTokens fix in
// turn.go addresses the most common cause (provider truncating the
// response at the boundary), but this test pins the empty-response
// ErrorEvent so future regressions cannot silently swallow empty output.
func TestRun_EmptyResponseSurfaces(t *testing.T) {
	fp := &fakeProvider{sequences: [][]Event{
		{DoneEvent{}}, // single iteration: clean exit, no text, no calls
	}}
	a := New(fp, "m")
	events, text, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err == nil {
		t.Fatalf("expected empty-response error, got nil (events=%v, text=%q)", events, text)
	}
	if !strings.Contains(err.Error(), "empty response") {
		t.Errorf("err: want empty-response message, got %v", err)
	}
	if text != "" {
		t.Errorf("text: want empty, got %q", text)
	}
	sawError := false
	for _, e := range events {
		if _, ok := e.(ErrorEvent); ok {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Errorf("expected an ErrorEvent in %v", events)
	}
}

func TestRecoveryError_HintPayload_Appended(t *testing.T) {
	// End-to-end: drive Agent.Run with a fake provider, a tool
	// handler that returns a RecoveryHintError, and a second
	// iteration that emits a clean DoneEvent. Assert the second
	// request's messages contain a RoleTool entry whose Content
	// carries BOTH the prose error and the JSON hint payload.
	//
	// This test would fail if Agent.Run stopped appending the hint
	// (the previous reconstruction-in-a-closure version would
	// not have caught that regression).
	stale := &struct {
		Action     string `json:"action"`
		StrategyID string `json:"strategy_id"`
	}{Action: "rebuild", StrategyID: "sid-123"}

	const callID = "call-1"
	fp := &fakeProvider{sequences: [][]Event{
		{ToolCallEvent{Call: ToolCall{ID: callID, Name: "stale_tool", Input: json.RawMessage(`{}`)}}, DoneEvent{}},
		{TextDelta{Text: "ok"}, DoneEvent{}},
	}}
	handlers := map[string]ToolHandler{
		"stale_tool": func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
			return nil, NewRecoveryHintError(
				"strategy %q was built on target=go-docker",
				"generate_strategy",
				stale,
			)
		},
	}
	a := New(fp, "m", WithTools([]ToolSpec{{Name: "stale_tool"}}, handlers))

	_, _, err := collect(t, a, []Message{{Role: RoleUser, Content: "go"}})
	if err != nil {
		t.Fatalf("Agent.Run err: %v", err)
	}
	if len(fp.recorded) < 2 {
		t.Fatalf("want 2 recorded requests (first + second iteration), got %d", len(fp.recorded))
	}
	// The second request's messages array carries the tool_result
	// the agent loop built from the hint-bearing error.
	var toolMsg *Message
	for i := range fp.recorded[1].Messages {
		m := &fp.recorded[1].Messages[i]
		if m.Role == RoleTool && m.ToolCallID == callID {
			toolMsg = m
			break
		}
	}
	if toolMsg == nil {
		t.Fatalf("no RoleTool message for call_id=%q in second request; got %+v", callID, fp.recorded[1].Messages)
	}
	if !strings.Contains(toolMsg.Content, "strategy %q was built on target=go-docker") {
		t.Errorf("tool_result missing prose error: %q", toolMsg.Content)
	}
	if !strings.Contains(toolMsg.Content, `"action":"rebuild"`) {
		t.Errorf("tool_result missing hint action: %q", toolMsg.Content)
	}
	if !strings.Contains(toolMsg.Content, `"strategy_id":"sid-123"`) {
		t.Errorf("tool_result missing hint strategy_id: %q", toolMsg.Content)
	}
	// And the prose prefix must precede the hint (sanity pin on
	// the append ordering, not just substring containment).
	proseIdx := strings.Index(toolMsg.Content, "strategy %q")
	hintIdx := strings.Index(toolMsg.Content, `"action":"rebuild"`)
	if proseIdx < 0 || hintIdx < 0 || proseIdx >= hintIdx {
		t.Errorf("hint must follow prose in tool_result; got %q", toolMsg.Content)
	}
}
