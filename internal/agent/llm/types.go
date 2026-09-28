// Package llm provides the provider-agnostic LLM abstraction: message types,
// a sealed streaming-event sum type, the Provider interface, and the Agent
// loop that drives a conversation turn (including tool-call dispatch).
package llm

import (
	"context"
	"encoding/json"
)

// Role is the conversational role of a Message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ToolCall is a provider-requested tool invocation.
type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// Message is one conversational turn. ToolCallID is set when Role == tool;
// ToolCalls is set when an assistant turn requested tools.
type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

// ToolSpec declares a tool the agent may call.
type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	JSONSchema  json.RawMessage `json:"json_schema"`
}

// Request is sent to a Provider.
type Request struct {
	Model    string     `json:"model"`
	Messages []Message  `json:"messages"`
	Tools    []ToolSpec `json:"tools,omitempty"` // populated when tools are registered
	// MaxTokens, when > 0, caps the model's output tokens for this request;
	// zero lets the provider pick its default. Strategy generation needs a
	// large budget to emit full source files in one tool call.
	MaxTokens int `json:"max_tokens,omitempty"`
	// ToolChoice, when non-empty, forces the model to call the named tool
	// on the next iteration. The agent loop sets this after a tool error
	// that carries a RecoveryError hint so the LLM can't retry the same
	// failing tool with bad arguments.
	ToolChoice string `json:"tool_choice,omitempty"`
}

// RecoveryError is an error that names the tool the LLM should call next.
// The agent loop checks for this interface; if present, it forces
// ToolChoice on the next iteration so the model calls the recovery tool
// instead of retrying the failing one.
type RecoveryError interface {
	Error() string
	RecoveryTool() string
	Hint() json.RawMessage // nil when no hint
}

// recoveryErr is the concrete implementation of RecoveryError.
type recoveryErr struct {
	Message string
	Tool    string
	hint    json.RawMessage
}

func (e recoveryErr) Error() string         { return e.Message }
func (e recoveryErr) RecoveryTool() string  { return e.Tool }
func (e recoveryErr) Hint() json.RawMessage { return e.hint }

// NewRecoveryError creates a RecoveryError that hints the agent loop
// to force ToolChoice=tool on the next iteration.
func NewRecoveryError(message, tool string) RecoveryError {
	return recoveryErr{Message: message, Tool: tool}
}

// NewRecoveryHintError creates a RecoveryError that carries a JSON
// hint payload alongside the prose message. The agent loop
// concatenates the marshalled hint into the RoleTool message body so
// the LLM can read structured fields (strategy_id, etc.) without
// parsing prose. If hint fails to marshal, the returned RecoveryError
// has Hint() == nil.
func NewRecoveryHintError(message, tool string, hint any) RecoveryError {
	b, err := json.Marshal(hint)
	if err != nil {
		return recoveryErr{Message: message, Tool: tool}
	}
	return recoveryErr{Message: message, Tool: tool, hint: b}
}

// Event is a sealed sum type for streaming events. Only the concrete types
// below satisfy it (via the unexported isEvent method).
type Event interface {
	isEvent()
}

type (
	TextDelta     struct{ Text string } // maps to SSE "delta" frames
	ToolCallEvent struct{ Call ToolCall }
	DoneEvent     struct{ Reason string } // maps to SSE "done" frames
	ErrorEvent    struct{ Err error }     // maps to SSE "error" frames
)

func (TextDelta) isEvent()     {}
func (ToolCallEvent) isEvent() {}
func (DoneEvent) isEvent()     {}
func (ErrorEvent) isEvent()    {}

// Provider streams events for a Request. The returned channel closes on
// Done, Error, or ctx cancellation.
type Provider interface {
	Stream(ctx context.Context, req Request) (<-chan Event, error)
}

// ToolHandler executes a tool by name with JSON input, returning JSON output.
type ToolHandler func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error)
