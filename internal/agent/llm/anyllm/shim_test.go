package anyllm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	anyllm "github.com/mozilla-ai/any-llm-go"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

// fakeStreamProvider implements the shim's streamProvider interface by
// returning canned chunk and error channels.
type fakeStreamProvider struct {
	chunks []anyllm.ChatCompletionChunk
	err    error
}

func (f *fakeStreamProvider) CompletionStream(_ context.Context, _ anyllm.CompletionParams) (<-chan anyllm.ChatCompletionChunk, <-chan error) {
	ch := make(chan anyllm.ChatCompletionChunk, len(f.chunks))
	for _, c := range f.chunks {
		ch <- c
	}
	close(ch)
	errs := make(chan error, 1)
	errs <- f.err
	close(errs)
	return ch, errs
}

func delta(s string) anyllm.ChatCompletionChunk {
	return anyllm.ChatCompletionChunk{
		Choices: []anyllm.ChunkChoice{{Delta: anyllm.ChunkDelta{Content: s}}},
	}
}

func TestShim_TextDeltasAndDone(t *testing.T) {
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		delta("hel"), delta("lo"),
	}, err: nil}
	shim := New(fp)

	ch, err := shim.Stream(t.Context(), llm.Request{
		Model:    "gpt-4o-mini",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var text string
	var sawDone bool
	for ev := range ch {
		switch e := ev.(type) {
		case llm.TextDelta:
			text += e.Text
		case llm.DoneEvent:
			sawDone = true
		}
	}
	if text != "hello" {
		t.Errorf("text: want hello, got %q", text)
	}
	if !sawDone {
		t.Error("expected DoneEvent after chunks")
	}
}

func TestShim_PropagatesError(t *testing.T) {
	fp := &fakeStreamProvider{chunks: nil, err: context.DeadlineExceeded}
	shim := New(fp)

	ch, _ := shim.Stream(t.Context(), llm.Request{Model: "m"})
	var sawErr bool
	for ev := range ch {
		if _, ok := ev.(llm.ErrorEvent); ok {
			sawErr = true
		}
	}
	if !sawErr {
		t.Error("expected ErrorEvent from propagated stream error")
	}
}

func TestShim_EmptyDeltaNotEmitted(t *testing.T) {
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		delta(""), delta("x"),
	}, err: nil}
	shim := New(fp)

	ch, _ := shim.Stream(t.Context(), llm.Request{Model: "m"})
	var n int
	for range ch {
		n++
		// DoneEvent is emitted after chunks; one TextDelta ("x") + DoneEvent = 2.
	}
	if n != 2 {
		t.Errorf("events: want 2 (one non-empty delta + done), got %d", n)
	}
}

// toolDelta builds a streaming chunk carrying one tool-call fragment (id +
// name may repeat empty on later chunks; arguments is one JSON fragment).
func toolDelta(id, name, args string) anyllm.ChatCompletionChunk {
	return anyllm.ChatCompletionChunk{
		Choices: []anyllm.ChunkChoice{{
			Delta: anyllm.ChunkDelta{
				ToolCalls: []anyllm.ToolCall{{
					ID:       id,
					Type:     "function",
					Function: anyllm.FunctionCall{Name: name, Arguments: args},
				}},
			},
		}},
	}
}

// TestShim_ToolCallArgsConcatenated guards the streaming assembly: a large
// tool-call argument (e.g. generate_strategy carrying several source files)
// arrives split across many chunks as incremental JSON fragments. The shim
// must concatenate the fragments; keeping the longest fragment (the old
// behaviour) truncates the JSON so the handler never sees the files.
func TestShim_ToolCallArgsConcatenated(t *testing.T) {
	full := `{"module_name":"bondspread","summary":"s","rationale":"r",` +
		`"files":[{"path":"main.go","content":"package main\n\nfunc main() {}\n"},` +
		`{"path":"go.mod","content":"module bondspread\n\ngo 1.26.5\n"}]}`
	// Split into three fragments at arbitrary byte boundaries (mimics how
	// OpenAI/Anthropic stream a large arguments object in pieces).
	frags := []string{
		full[:len(full)/3],
		full[len(full)/3 : 2*len(full)/3],
		full[2*len(full)/3:],
	}
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		toolDelta("call_1", "generate_strategy", frags[0]),
		toolDelta("call_1", "generate_strategy", frags[1]),
		toolDelta("call_1", "generate_strategy", frags[2]),
	}, err: nil}
	shim := New(fp)

	ch, err := shim.Stream(t.Context(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var got llm.ToolCallEvent
	var sawDone bool
	for ev := range ch {
		switch e := ev.(type) {
		case llm.ToolCallEvent:
			got = e
		case llm.DoneEvent:
			sawDone = true
		}
	}
	if !sawDone {
		t.Error("expected DoneEvent after the tool call")
	}
	if got.Call.Name != "generate_strategy" {
		t.Fatalf("tool name: want generate_strategy, got %q", got.Call.Name)
	}
	var parsed struct {
		ModuleName string `json:"module_name"`
		Files      []struct {
			Path, Content string
		}
	}
	if err := json.Unmarshal(got.Call.Input, &parsed); err != nil {
		t.Fatalf("unmarshal assembled input: %v\ninput=%q", err, string(got.Call.Input))
	}
	if parsed.ModuleName != "bondspread" {
		t.Errorf("module_name: want bondspread, got %q", parsed.ModuleName)
	}
	if len(parsed.Files) != 2 {
		t.Fatalf("files: want 2, got %d (input=%q)", len(parsed.Files), string(got.Call.Input))
	}
	wantMain := "package main\n\nfunc main() {}\n"
	if parsed.Files[0].Content != wantMain {
		t.Errorf("main.go content mismatch: want %q, got %q", wantMain, parsed.Files[0].Content)
	}
}

func TestShim_ToolCallArgsCumulative(t *testing.T) {
	// Anthropic re-emits the full accumulated arguments on every delta chunk
	// (each chunk is a growing prefix of the final JSON), unlike OpenAI's
	// incremental fragments. The shim must replace, not concatenate, when the
	// incoming string starts with what it already has.
	full := `{"order_book_id":"GOOG-USD","side":"buy"}`
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		toolDelta("call_c", "dora_get_order_book", full[:1]),
		toolDelta("call_c", "dora_get_order_book", full[:14]),
		toolDelta("call_c", "dora_get_order_book", full),
	}, err: nil}
	shim := New(fp)

	ch, err := shim.Stream(t.Context(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var got llm.ToolCallEvent
	for ev := range ch {
		if e, ok := ev.(llm.ToolCallEvent); ok {
			got = e
		}
	}
	if got.Call.Name != "dora_get_order_book" {
		t.Fatalf("tool name: want dora_get_order_book, got %q", got.Call.Name)
	}
	var parsed struct {
		OrderBookID string `json:"order_book_id"`
	}
	if err := json.Unmarshal(got.Call.Input, &parsed); err != nil {
		t.Fatalf("cumulative delivery must assemble to valid JSON; got %v\ninput=%q", err, string(got.Call.Input))
	}
	if parsed.OrderBookID != "GOOG-USD" {
		t.Errorf("order_book_id: want GOOG-USD, got %q (input=%q)", parsed.OrderBookID, string(got.Call.Input))
	}
}

// TestShim_ToolCallArgs_TruncatedCumulative covers the streaming failure
// seen in production: Anthropic re-emits growing JSON until the model's
// tool_use block ends, but the very last emission can arrive truncated
// (provider cuts a framing byte, the SSE stream closes mid-token, etc.)
// The shim must repair the truncated buffer so the assembled tool_use
// `input` is always a valid JSON object, otherwise the provider rejects
// the next turn with `messages.N.content.0.tool_use.input: Input should
// be an object` (Anthropic 400) and the model never sees the failure.
func TestShim_ToolCallArgs_TruncatedCumulative(t *testing.T) {
	// Full valid object minus the closing `}` -- the same shape we saw
	// in the regression: `args_len` larger than what the user-visible
	// preview cut off, no clear close brace.
	truncated := `{"module_name":"x","summary":"s","rationale":"r","files":[{"path":"main.go","content":"package main`
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		toolDelta("call_c", "generate_strategy", truncated[:len(truncated)/3]),
		toolDelta("call_c", "generate_strategy", truncated[len(truncated)/3:2*len(truncated)/3]),
		toolDelta("call_c", "generate_strategy", truncated[2*len(truncated)/3:]),
	}, err: nil}
	shim := New(fp)

	ch, err := shim.Stream(t.Context(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var got llm.ToolCallEvent
	for ev := range ch {
		if e, ok := ev.(llm.ToolCallEvent); ok {
			got = e
		}
	}
	input := string(got.Call.Input)
	if input == "" || input == "null" {
		t.Fatalf("assembled input must be a valid JSON object; got %q", input)
	}
	// Anthropic / OpenAI both reject a non-object `tool_use.input`.
	var parsed map[string]any
	if err := json.Unmarshal(got.Call.Input, &parsed); err != nil {
		t.Fatalf("assembled input must unmarshal as a JSON object (provider rejects otherwise); got err=%v input=%q", err, input)
	}
}

// TestShim_ToolCall_EmptyID_OpenAIStreaming pins the fix for the
// strategy4 session: OpenAI splits a tool call across chunks where
// the first chunk carries id+name+arguments="" and later chunks
// carry id="" name="" with new argument fragments. The original
// shim keyed the buffer by id, so the later chunks collided on key
// "" and emitted a tool call with name="" -- which the agent's
// handler map then failed to dispatch ("llm: no handler for tool
// \"\""). The fix merges continuation chunks into the most
// recent buffered entry and only overwrites id/name from a chunk
// when the chunk actually carries them.
func TestShim_ToolCall_EmptyID_OpenAIStreaming(t *testing.T) {
	// First chunk: id=call_1, name=dora_list_order_books, args=`{"`.
	// Second chunk: id="", name="", args=`limit":50}`. Concat is the
	// valid JSON object `{"limit":50}`.
	fp := &fakeStreamProvider{chunks: []anyllm.ChatCompletionChunk{
		toolDelta("call_1", "dora_list_order_books", `{"`),
		toolDelta("", "", `limit":50}`),
	}, err: nil}
	shim := New(fp)
	ch, err := shim.Stream(t.Context(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	var calls []llm.ToolCallEvent
	var sawDone bool
	for ev := range ch {
		switch e := ev.(type) {
		case llm.ToolCallEvent:
			calls = append(calls, e)
		case llm.DoneEvent:
			sawDone = true
		}
	}
	if !sawDone {
		t.Error("expected DoneEvent after the tool call")
	}
	if len(calls) != 1 {
		t.Fatalf("tool calls: want 1 (continuation merged), got %d (%+v)", len(calls), calls)
	}
	got := calls[0].Call
	if got.Name != "dora_list_order_books" {
		t.Errorf("tool name: want dora_list_order_books, got %q", got.Name)
	}
	if got.ID != "call_1" {
		t.Errorf("tool id: want call_1, got %q", got.ID)
	}
	if !strings.Contains(string(got.Input), "limit") {
		t.Errorf("assembled input must carry the limit fragment; got %q", string(got.Input))
	}
}
