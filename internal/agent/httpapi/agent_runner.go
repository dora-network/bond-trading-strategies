package httpapi

// agent_runner.go holds the shared LLM-event/SSE adapters used by the strategy
// turn driver (turn.go). The provider-construction seam is ProviderFactory
// (runner_types.go), wired by the production runner.

import (
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
)

// llmEmit adapts an SSE-payload emit into an llm.Event emit.
func llmEmit(emit func(EventPayload)) func(llm.Event) {
	return func(e llm.Event) {
		switch v := e.(type) {
		case llm.TextDelta:
			emit(TextPayload{Text: v.Text})
		case llm.ToolCallEvent:
			emit(ToolCallPayload{ID: v.Call.ID, Name: v.Call.Name, Input: v.Call.Input})
		case llm.DoneEvent:
			emit(DonePayload{Reason: v.Reason})
		case llm.ErrorEvent:
			emit(ErrorPayload{Message: v.Err.Error()})
		}
	}
}

// historyToLLM maps stored session messages to the llm.Message slice the agent
// consumes.
func historyToLLM(history []session.StoredMessage) []llm.Message {
	out := make([]llm.Message, 0, len(history))
	for _, m := range history {
		out = append(out, llm.Message{Role: llm.Role(m.Role), Content: m.Content})
	}
	return out
}

// lastAssistantContent returns the most recent assistant message body
// from history, or "" if there is none. Used to give the classifier
// enough context to interpret short conversational follow-ups (e.g. a
// bare "yes") against the prior turn, without exposing the full session
// history to the sealed classify hop.
func lastAssistantContent(history []session.StoredMessage) string {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == "assistant" {
			return history[i].Content
		}
	}
	return ""
}
