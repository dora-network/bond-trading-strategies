// Package anyllm wraps github.com/mozilla-ai/any-llm-go behind the
// internal/llm Provider interface, translating streaming chunks into our
// sealed Event channel.
package anyllm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	anyllm "github.com/mozilla-ai/any-llm-go"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

// previewForLog returns the first 512 bytes of s plus a length suffix, so
// debug logs show tool-call argument structure without flooding on large
// (generate_strategy) payloads.
func previewForLog(s string) string {
	const max = 512
	if len(s) <= max {
		return s
	}
	return fmt.Sprintf("%s...(%d bytes)", s[:max], len(s))
}

// chunkDiag accumulates per-chunk finish reasons and any text the shim
// forwarded via TextDelta. After the upstream chunk channel closes the
// caller logs an "empty response" warn when the diagnostic finds no
// usable events -- the smoking-gun signal that an Anthropic refusal or
// content_filter reached us untranslated. Kept as a small struct so the
// shim's per-chunk bookkeeping stays isolated.
type chunkDiag struct {
	contentLen    int
	finishReasons []string
}

func (d *chunkDiag) record(choice anyllm.ChunkChoice) {
	if choice.FinishReason != "" {
		d.finishReasons = append(d.finishReasons, choice.FinishReason)
	}
	if choice.Delta.Content != "" {
		d.contentLen += len(choice.Delta.Content)
	}
}

// streamProvider is the subset of any-llm-go's Provider that the shim uses.
// any-llm-go providers (openai, anthropic, ...) satisfy it structurally; tests
// supply a fake.
type streamProvider interface {
	CompletionStream(ctx context.Context, params anyllm.CompletionParams) (<-chan anyllm.ChatCompletionChunk, <-chan error)
}

// Shim adapts an any-llm-go provider to llm.Provider.
type Shim struct {
	p streamProvider
}

// New wraps an any-llm-go provider (passed as the structural streamProvider
// interface so tests can substitute a fake).
func New(p streamProvider) *Shim { return &Shim{p: p} }

// Stream translates a Request into an any-llm-go CompletionStream and pumps
// the resulting chunks onto an llm.Event channel. Text deltas become
// TextDelta; tool calls become ToolCallEvent; a nil terminal error becomes
// DoneEvent; a non-nil one becomes ErrorEvent. The channel closes afterwards.
//
// method once the empty-response diagnostic stabilises; the inline body keeps
// the diagnostic close to the chunk loop it observes.
//
//nolint:funlen // TODO(funlen): extract the pump goroutine into a separate
func (s *Shim) Stream(ctx context.Context, req llm.Request) (<-chan llm.Event, error) {
	params := anyllm.CompletionParams{
		Model:    req.Model,
		Messages: toAnyllmMessages(req.Messages),
		Tools:    toAnyllmTools(req.Tools),
	}
	if req.MaxTokens > 0 {
		mt := req.MaxTokens
		params.MaxTokens = &mt
	}
	if req.ToolChoice != "" {
		params.ToolChoice = map[string]any{
			"type":     "function",
			"function": map[string]string{"name": req.ToolChoice},
		}
	}
	chunks, errs := s.p.CompletionStream(ctx, params)

	out := make(chan llm.Event)
	streamStart := time.Now()
	slog.Debug("llm stream start", "model", req.Model, "tools", len(req.Tools), "messages", len(req.Messages))
	go func() {
		// The instrumentation here exists so a hung upstream (open SSE
		// socket, stalled chunk pump, no errs value) is visible in the
		// agent log instead of leaving the chat UI waiting forever for
		// the next event. Every exit path records `exit` and `ctx_err`
		// so the next debug session can pinpoint whether the goroutine
		// bailed on parent-cancel, errs error, or normal completion.
		var exitReason string
		var chunkCount int
		var emitErr error
		defer func() {
			slog.Debug("llm stream end", "model", req.Model,
				"duration_ms", time.Since(streamStart).Milliseconds(),
				"chunks", chunkCount, "exit", exitReason,
				"err", errString(emitErr), "ctx_err", ctx.Err())
			close(out)
		}()

		// Buffer tool calls by ID across chunks. The LLM streams the
		// tool call input as a sequence of partial JSON fragments (the
		// first chunk often has input="{}" with later chunks adding
		// fields). We accumulate by ID and emit one ToolCallEvent per
		// call only after the stream ends, so the agent loop never
		// sees a half-formed call.
		calls := make(map[string]anyllm.ToolCall)
		callOrder := make([]string, 0)
		// chunkDiag captures per-chunk finish_reason and content so the
		// empty-response warn below can name what Anthropic actually sent
		// when the chat UI sees an empty SSE stream.
		var diag chunkDiag
		for chunk := range chunks {
			chunkCount++
			if len(chunk.Choices) == 0 {
				continue
			}
			choice := chunk.Choices[0]
			delta := choice.Delta
			diag.record(choice)
			if d := delta.Content; d != "" {
				select {
				case <-ctx.Done():
					exitReason = "ctx_done_text"
					return
				case out <- llm.TextDelta{Text: d}:
				}
			}
			for _, tc := range delta.ToolCalls {
				sanitizedID := sanitizeToolCallID(tc.ID)
				// OpenAI streams a tool call across multiple chunks.
				// The first chunk carries id+name+arguments=""; later
				// chunks carry id="" name="" and a new argument
				// fragment. The shim must merge those into one
				// buffered entry -- starting a new one under key
				// "" would emit a tool call with name="" which the
				// agent's handler map cannot dispatch. Key by id
				// when present, then by name, then fall back to
				// the most recent buffered call.
				key := sanitizedID
				if key == "" && tc.Function.Name != "" {
					key = "name:" + tc.Function.Name
				} else if key == "" && len(callOrder) > 0 {
					key = callOrder[len(callOrder)-1]
				}
				if key == "" {
					// No prior buffered call to attach
					// this fragment to; the agent's
					// handler map has no useful target.
					slog.Debug("llm stream tool delta dropped: no buffered call",
						"args_len", len(tc.Function.Arguments))
					continue
				}
				existing, ok := calls[key]
				if !ok {
					callOrder = append(callOrder, key)
				}
				// Providers stream tool-call arguments two ways: OpenAI emits
				// incremental JSON fragments (append); Anthropic re-emits the
				// full accumulated arguments on each chunk (replace). If the
				// incoming string starts with what we have, it is the
				// cumulative form -- replace; otherwise append.
				newArgs := tc.Function.Arguments
				cumulative := strings.HasPrefix(newArgs, existing.Function.Arguments)
				if cumulative {
					existing.Function.Arguments = newArgs
				} else {
					existing.Function.Arguments += newArgs
				}
				slog.Debug("llm stream tool delta", "id", sanitizedID,
					"name", tc.Function.Name, "args_len", len(newArgs), "cumulative", cumulative)
				// Only set id and name from the chunk when present;
				// continuation chunks carry id="" name="" and
				// would clobber the buffered values.
				if sanitizedID != "" {
					existing.ID = sanitizedID
				}
				existing.Type = "function"
				if tc.Function.Name != "" {
					existing.Function.Name = tc.Function.Name
				}
				calls[key] = existing
			}
			slog.Debug("llm stream chunk", "i", chunkCount,
				"role", delta.Role,
				"content_len", len(delta.Content),
				"tool_calls", len(delta.ToolCalls),
				"finish_reason", choice.FinishReason)
		}
		// chunks channel closed by upstream; check errs for terminal status.
		if err := <-errs; err != nil {
			emitErr = err
			select {
			case <-ctx.Done():
				exitReason = "ctx_done_after_errs"
			case out <- llm.ErrorEvent{Err: err}:
				exitReason = "errs_error"
			}
			return
		}
		exitReason = "errs_nil"
		// Degenerate-response diagnostic: the upstream returned cleanly (errs
		// was nil) but emitted no text and no tool calls. Surface every
		// per-chunk finish_reason the shim observed so the next debug session
		// can tell content_filter from a stop_reason-only end_turn from an
		// empty-but-valid response. Warn level is intentional: silent empty
		// responses are the user-visible bug we are hunting.
		if len(calls) == 0 && diag.contentLen == 0 && chunkCount > 0 {
			slog.Warn("llm stream empty response",
				"model", req.Model,
				"chunks", chunkCount,
				"finish_reasons", diag.finishReasons,
				"accumulated_content_len", diag.contentLen,
				"messages", len(req.Messages))
		}
		// Emit each buffered tool call once, with the accumulated
		// arguments. Order is the order the IDs first appeared.
		for _, id := range callOrder {
			tc := calls[id]
			args := toolCallInput(tc.Function.Arguments)
			slog.Debug("llm tool call assembled", "id", tc.ID, "name", tc.Function.Name,
				"args_len", len(args), "preview", previewForLog(string(args)))
			select {
			case <-ctx.Done():
				exitReason = "ctx_done_emit_tool"
				return
			case out <- llm.ToolCallEvent{Call: llm.ToolCall{
				ID:    tc.ID,
				Name:  tc.Function.Name,
				Input: args,
			}}:
			}
		}
		select {
		case <-ctx.Done():
			exitReason = "ctx_done_emit_done"
		case out <- llm.DoneEvent{}:
			exitReason = "emit_done"
		}
	}()
	return out, nil
}

// errString returns err.Error() or "" for a nil error.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// toAnyllmTools converts our ToolSpec list to any-llm-go's Tool format.
// The JSONSchema is passed through as the function parameters.
func toAnyllmTools(specs []llm.ToolSpec) []anyllm.Tool {
	if len(specs) == 0 {
		return nil
	}
	out := make([]anyllm.Tool, len(specs))
	for i, s := range specs {
		var params map[string]any
		if len(s.JSONSchema) > 0 {
			_ = json.Unmarshal(s.JSONSchema, &params)
		}
		out[i] = anyllm.Tool{
			Type: "function",
			Function: anyllm.Function{
				Name:        s.Name,
				Description: s.Description,
				Parameters:  params,
			},
		}
	}
	return out
}

func toAnyllmMessages(msgs []llm.Message) []anyllm.Message {
	out := make([]anyllm.Message, 0, len(msgs))
	for _, m := range msgs {
		am := anyllm.Message{
			Role:       toAnyllmRole(m.Role),
			Content:    m.Content,
			ToolCallID: sanitizeToolCallID(m.ToolCallID),
		}
		for _, tc := range m.ToolCalls {
			am.ToolCalls = append(am.ToolCalls, anyllm.ToolCall{
				ID:       sanitizeToolCallID(tc.ID),
				Type:     "function",
				Function: anyllm.FunctionCall{Name: tc.Name, Arguments: string(tc.Input)},
			})
		}
		out = append(out, am)
	}
	return out
}

func toAnyllmRole(r llm.Role) string {
	switch r {
	case llm.RoleUser:
		return anyllm.RoleUser
	case llm.RoleAssistant:
		return anyllm.RoleAssistant
	default:
		return string(r)
	}
}

// sanitizeToolCallID keeps the tool_use_id passed back to the model
// within the [a-zA-Z0-9_-]+ character class that the OpenAI / Anthropic
// tool_result reference requires. SDKs typically return server-minted
// IDs that already match; this is a belt for unexpected shapes.
func sanitizeToolCallID(id string) string {
	if id == "" {
		return id
	}
	out := make([]byte, len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '_', c == '-':
			out[i] = c
		default:
			out[i] = '_'
		}
	}
	return string(out)
}

// toolCallInput normalizes the streaming tool-argument string and
// guarantees the assembled tool_use `input` is a valid JSON object.
// Two failure modes:
//
//  1. The model emitted "", "null", or whitespace as a no-arg tool
//     call. Map to "{}".
//  2. The stream was truncated mid-token: e.g.
//     `{"module_name":"x","files":[{"path":"main.go","content":"package main`
//     without the closing `}]`. The previous behaviour emitted this
//     raw, which Anthropic rejects with
//     `messages.N.content.0.tool_use.input: Input should be an object`
//     (400), aborting the entire turn before the tool handler could
//     see the malformed input. Repair at the provider boundary so
//     the shim always ships a valid object downstream. We try the
//     cheapest repair first (one `}` append), then fall back to
//     "{}" for any other shape (string, number, array, garbage).
func toolCallInput(raw string) json.RawMessage {
	if raw == "" || raw == "null" || len(strings.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`)
	}
	if isValidObject(raw) {
		return json.RawMessage(raw)
	}
	if isValidObject(raw + "}") {
		return json.RawMessage(raw + "}")
	}
	return json.RawMessage(`{}`)
}

// isValidObject reports whether raw unmarshals as a non-null JSON
// object. Any other shape (string, number, array, partial JSON,
// trailing truncation) fails. Cheap enough for the streaming
// path: each call unmarshals a single small input.
func isValidObject(raw string) bool {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return false
	}
	_, ok := v.(map[string]any)
	return ok
}
