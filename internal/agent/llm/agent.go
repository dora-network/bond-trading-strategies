package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrMaxIterations is returned when the tool-call loop exceeds its limit.
var ErrMaxIterations = errors.New("llm: max tool-call iterations exceeded")

// DefaultLLMTimeout caps a single LLM round-trip. A hung upstream or stalled
// model would otherwise leave the chat UI waiting indefinitely for the
// DoneEvent that never comes. Callers may override per Agent via
// WithLLMTimeout.
const DefaultLLMTimeout = 90 * time.Second

// Agent drives a conversation turn against a Provider.
type Agent struct {
	provider   Provider
	model      string
	tools      []ToolSpec
	handlers   map[string]ToolHandler
	maxIters   int
	maxTokens  int
	llmTimeout time.Duration
}

// Option configures an Agent.
type Option func(*Agent)

// WithTools registers tools (specs + handlers).
func WithTools(tools []ToolSpec, handlers map[string]ToolHandler) Option {
	return func(a *Agent) {
		a.tools = tools
		a.handlers = handlers
	}
}

// WithMaxIters overrides the default 10-iteration tool-call guard.
func WithMaxIters(n int) Option {
	return func(a *Agent) { a.maxIters = n }
}

// WithMaxTokens caps the model's output tokens per request (0 = provider
// default). Strategy generation needs a large budget to emit full source.
func WithMaxTokens(n int) Option {
	return func(a *Agent) { a.maxTokens = n }
}

// WithLLMTimeout overrides the per-iteration LLM round-trip deadline. A value
// <= 0 restores DefaultLLMTimeout. Useful for tests that exercise the
// timeout path without burning DefaultLLMTimeout of wall time.
func WithLLMTimeout(d time.Duration) Option {
	return func(a *Agent) { a.llmTimeout = d }
}

// defaultMaxIters bounds the tool-call loop per Run when no option overrides it.
const defaultMaxIters = 10

// New constructs an Agent for the given provider and model.
func New(provider Provider, model string, opts ...Option) *Agent {
	a := &Agent{
		provider:   provider,
		model:      model,
		maxIters:   defaultMaxIters,
		handlers:   map[string]ToolHandler{},
		llmTimeout: DefaultLLMTimeout,
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Run executes one assistant turn over messages (history + new user prompt as
// the last entry). Events are emitted as they arrive. Returns the accumulated
// assistant text. Each LLM round-trip is bounded by a.llmTimeout so a hung
// upstream surfaces as an ErrorEvent (and a user-visible chat UI error)
// instead of blocking the agent loop indefinitely.
func (a *Agent) Run(ctx context.Context, messages []Message, emit func(Event)) (string, error) {
	timeout := a.llmTimeout
	if timeout <= 0 {
		timeout = DefaultLLMTimeout
	}
	working := append([]Message(nil), messages...)
	// forceTool is set when a tool error carries a RecoveryError hint.
	// The next iteration's request forces ToolChoice so the model calls
	// the recovery tool instead of retrying the failing one with bad args.
	forceTool := ""
	for iter := range a.maxIters {
		req := Request{Model: a.model, Messages: working, Tools: a.tools, MaxTokens: a.maxTokens}
		if forceTool != "" {
			req.ToolChoice = forceTool
			forceTool = "" // one-shot: only force the immediately-next iteration
		}
		// Per-iteration deadline. Honors any parent-cancel ctx.Done() too;
		// if the parent is cancelled, deadline fires immediately.
		streamCtx, cancel := context.WithTimeout(ctx, timeout)
		ch, err := a.provider.Stream(streamCtx, req)
		if err != nil {
			cancel()
			emit(ErrorEvent{Err: err})
			return "", err
		}
		var text strings.Builder
		var calls []ToolCall
		for ev := range ch {
			switch e := ev.(type) {
			case TextDelta:
				emit(e)
				text.WriteString(e.Text)
			case ToolCallEvent:
				emit(e)
				calls = append(calls, e.Call)
			case DoneEvent:
				emit(e)
			case ErrorEvent:
				emit(e)
				cancel()
				return text.String(), e.Err
			}
		}
		// Snapshot ctx state BEFORE cancel() so the post-loop check sees the
		// real cause of the channel close. cancel() sets Err() to Canceled
		// unconditionally, which would mask the underlying timeout or
		// parent-cancel and trip a spurious error on the normal path.
		streamErr := streamCtx.Err()
		cancel()
		// Post-loop exit diagnosis: streamErr reports both parent-cancel
		// and timeout. A hung provider sends zero events, so the loop body
		// never sees ctx.Done; we have to look here. Parent-cancel is not a
		// timeout -- but it IS still a failure (the chat UI dropped, a proxy
		// timed out, the request deadline elapsed) and the user must see a
		// clear error rather than an empty SSE stream that closes silently.
		if streamErr != nil {
			var err error
			if streamErr == context.Canceled {
				err = fmt.Errorf("llm stream cancelled by parent (iteration %d)", iter)
			} else {
				err = fmt.Errorf("llm stream timed out after %s (iteration %d)", timeout, iter)
			}
			emit(ErrorEvent{Err: err})
			return text.String(), err
		}
		if len(calls) == 0 {
			// Empty-response diagnostic: the upstream returned cleanly (no
			// timeout, no parent-cancel) but emitted no text and no tool
			// calls. The shim's "llm stream empty response" Warn will tell
			// the operator why (content_filter, length, bare end_turn).
			// Surface a user-visible error here so the chat UI sees
			// "the LLM returned an empty response" rather than an SSE
			// stream that closes silently. Otherwise the user has no
			// signal that anything went wrong and no way to retry.
			if text.Len() == 0 {
				err := fmt.Errorf("llm: empty response (iteration %d); see agent log for finish_reason", iter)
				emit(ErrorEvent{Err: err})
				return "", err
			}
			return text.String(), nil
		}
		// Tool calls requested: dispatch and loop.
		working = append(working, Message{Role: RoleAssistant, Content: text.String(), ToolCalls: calls})
		for _, c := range calls {
			h, ok := a.handlers[c.Name]
			if !ok {
				err := fmt.Errorf("llm: no handler for tool %q", c.Name)
				emit(ErrorEvent{Err: err})
				return text.String(), err
			}
			out, herr := h(ctx, c.Name, c.Input)
			if herr != nil {
				// Feed the error back to the LLM as a tool_result so it
				// can react: retry with different input, fall back to a
				// different tool, or surface a user-visible error. We do
				// NOT emit ErrorEvent here -- the loop continues; the LLM
				// decides whether to recover. ErrorEvent still fires when
				// the LLM gives up (max iterations, empty response, etc.)
				// so the SSE stream closes with a user-visible message.
				// If the error carries a RecoveryError hint, force the
				// model to call the named recovery tool on the next
				// iteration so it can't retry the same bad call.
				var recovery RecoveryError
				if errors.As(herr, &recovery) {
					forceTool = recovery.RecoveryTool()
				}
				content := "error: " + herr.Error()
				if recovery != nil {
					if h := recovery.Hint(); h != nil {
						content = content + "\n" + string(h)
					}
				}
				working = append(working, Message{
					Role: RoleTool, Content: content,
					ToolCallID: c.ID,
				})
				continue
			}
			working = append(working, Message{Role: RoleTool, Content: string(out), ToolCallID: c.ID})
		}
	}
	emit(ErrorEvent{Err: ErrMaxIterations})
	return "", ErrMaxIterations
}
