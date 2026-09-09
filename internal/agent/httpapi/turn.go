package httpapi

// turn.go is the shared strategy-turn driver (spec §16). It applies the
// four-layer sanitisation pass (HTTP filter, LLM classifier, system-prompt
// policy, source-pattern scan inside the generate_strategy handler), runs
// the classify hop, then drives the agent tool loop. StrategyRunner
// composes this and adds version capture; the strategy-turn tests drive
// it directly.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/config"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm/prompts"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
)

// turnDeps holds the inputs to one sanitised strategy turn. The production
// runner composes this; the strategy-turn tests drive it directly.
type turnDeps struct {
	userID, sessionID, provider, model string
	prompt                             string
	history                            []session.StoredMessage
	emit                               func(EventPayload)

	newProvider   ProviderFactory
	newClassifier ClassifierFactory
	// newTools builds the per-turn tool set. The Dora read tools bind the
	// user's per-request Dora API key (doraAPIKey), so the factory constructs
	// a fresh handler map each turn.
	newTools   ToolFactory
	doraAPIKey string
	audit      AuditWriter

	// modelCaps drives the strategy-generation hop's max_tokens budget;
	// the runner looks up model-specific caps from this table so the
	// provider doesn't reject the request with a 400.
	modelCaps config.ModelCaps

	// llmTimeout overrides the per-iteration LLM round-trip deadline for
	// the strategy-generation hop. Zero means use the default
	// (StrategyGenerationLLMTimeout).
	llmTimeout time.Duration

	// llmMaxIters caps the tool-call loop iterations for the
	// strategy-generation hop. Zero means use the agent default (10).
	llmMaxIters int

	// observeGenerate, if set, wraps the generate_strategy handler. The
	// production runner supplies its capture observer; tests pass nil to use
	// the handler as-is.
	observeGenerate func(llm.ToolHandler) llm.ToolHandler
}

// StrategyGenerationLLMTimeout caps each LLM round-trip on the
// strategy-generation hop. The strategy hop can legitimately take much
// longer than the agent's DefaultLLMTimeout (90s) when the model is
// emitting up to MaxTokensForModel output tokens (per the operator-edited
// configs/model_caps.json table); a
// slow Anthropic round-trip in the field hit the prior 90s boundary
// mid-generation, surfaced as "[anthropic] provider_error: context
// deadline exceeded" and left the user with no deliverable. 10m is a
// comfortable ceiling for that hop; bump higher if a real strategy
// ever needs more. The classifier hop (which only emits a short JSON
// verdict) stays at the agent's DefaultLLMTimeout -- no need to widen
// it.
const StrategyGenerationLLMTimeout = 10 * time.Minute

// runStrategyTurn applies the four sanitisation layers, the classify hop, the
// system-prompt policy, then drives the agent loop. Reject/refuse outcomes emit
// their own SSE events and audit rows.
func runStrategyTurn(ctx context.Context, d turnDeps) (string, error) {
	// Layer 1 — HTTP boundary filter (§16.1).
	if verdict, derr := sanitize.Filter(d.prompt); verdict != sanitize.VerdictOK {
		return rejectTurn(ctx, d,
			rejectDetail{Category: verdict.Category(), Reason: errString(derr)},
			"prompt rejected by input filter")
	}

	// Provider shared by the classify hop and the strategy hop.
	p, resolvedModel, err := d.newProvider(ctx, d.userID, d.provider, d.model)
	if err != nil {
		d.emit(ErrorPayload{Message: "provider config not set"})
		return "", fmt.Errorf("provider: %w", err)
	}

	// Layer 2 — LLM classify-and-respond (§16.2). The factory receives the
	// built provider (no second build). priorAssistant is the most recent
	// assistant reply from the session history, when present — it lets the
	// classifier make sense of short conversational follow-ups like "yes"
	// without the user needing to repeat the full request.
	priorAssistant := lastAssistantContent(d.history)
	verdict, verr := d.newClassifier(p, resolvedModel).Classify(ctx, d.prompt, priorAssistant)
	if verr != nil {
		msg := classifyUserMessage(verr)
		return rejectTurn(ctx, d,
			rejectDetail{Category: "classify_error", Reason: errString(verr)},
			msg)
	}
	switch verdict.Verdict {
	case "off_topic":
		return refuseTurn(ctx, d, verdict.Reason)
	case "adversarial":
		return rejectTurn(ctx, d,
			rejectDetail{Category: "adversarial", Reason: verdict.Reason},
			"request declined")
	case "ok":
		// proceed to the strategy-building hop
	default:
		return rejectTurn(ctx, d,
			rejectDetail{Category: "unknown_verdict", Reason: verdict.Verdict},
			"request declined")
	}

	// Layer 3 — system-prompt policy (§16.3): prepend StrategyGenerationSystemPrompt.
	msgs := append(
		[]llm.Message{{Role: llm.RoleSystem, Content: prompts.StrategyGenerationSystemPrompt}},
		append(historyToLLM(d.history), llm.Message{Role: llm.RoleUser, Content: d.prompt})...,
	)

	// Layer 4 — source-pattern scan runs inside the generate_strategy handler.
	// The runner observes outcomes (audit + capture) when an observer is wired.
	// Tools are built per turn so the Dora handlers bind the per-request key.
	toolSpecs, handlers := d.newTools(d.doraAPIKey, d.userID)
	if d.observeGenerate != nil {
		if g, ok := handlers["generate_strategy"]; ok {
			handlers = cloneHandlers(handlers)
			handlers["generate_strategy"] = d.observeGenerate(g)
		}
	}
	maxTokens := d.modelCaps.MaxTokensFor(resolvedModel)
	slog.Info("strategy generation token budget",
		"model", resolvedModel, "max_tokens", maxTokens)
	opts := []llm.Option{
		llm.WithTools(toolSpecs, handlers),
		llm.WithMaxTokens(maxTokens),
		llm.WithLLMTimeout(strategyGenTimeout(d.llmTimeout)),
	}
	if d.llmMaxIters > 0 {
		opts = append(opts, llm.WithMaxIters(d.llmMaxIters))
	}
	agent := llm.New(p, resolvedModel, opts...)
	return agent.Run(ctx, msgs, llmEmit(d.emit))
}

// strategyGenTimeout returns the per-iteration LLM deadline for the
// strategy-generation hop, falling back to the hardcoded default when
// no explicit timeout is configured.
func strategyGenTimeout(explicit time.Duration) time.Duration {
	if explicit > 0 {
		return explicit
	}
	return StrategyGenerationLLMTimeout
}

// classifyUserMessage produces a user-facing error message for a
// classifier failure. Provider errors (rate limits, credit exhaustion,
// model-not-found) are relayed so the user can act; parse / verdict
// errors keep the generic message since the user cannot fix those.
func classifyUserMessage(err error) string {
	msg := err.Error()
	if after, ok := strings.CutPrefix(msg, "sanitize: classify stream event: "); ok {
		return "upstream LLM error: " + after
	}
	if after, ok := strings.CutPrefix(msg, "sanitize: classify stream: "); ok {
		return "upstream LLM error: " + after
	}
	return "could not classify the message"
}

// rejectTurn emits an error SSE event, writes a message.rejected audit row, and
// returns an error with empty assistant text so the handler does not persist
// model output. Every short-circuit that is not off-topic refusal records
// message.rejected (§16.1, §16.2 adversarial, §16.4).
func rejectTurn(ctx context.Context, d turnDeps, detail rejectDetail, sseMessage string) (string, error) {
	d.emit(ErrorPayload{Message: sseMessage})
	_ = d.audit.Insert(ctx, d.userID, audit.ActionMessageRejected, mustMarshalDetail(detail))
	return "", fmt.Errorf("message.rejected: %s", detail.Category)
}

// refuseTurn persists a one-line refusal as the returned assistant text (the
// handler persists it), writes strategy.refused, and ends with done.
func refuseTurn(ctx context.Context, d turnDeps, reason string) (string, error) {
	refusal := "This agent builds and backtests trading strategies for the Dora Network. " +
		"Please describe the strategy you want to build or backtest."
	_ = d.audit.Insert(ctx, d.userID, audit.ActionStrategyRefused,
		mustMarshalDetail(refuseDetail{Reason: reason}))
	d.emit(RefusalPayload{Reason: reason})
	d.emit(DonePayload{})
	return refusal, nil
}

func cloneHandlers(in map[string]llm.ToolHandler) map[string]llm.ToolHandler {
	out := make(map[string]llm.ToolHandler, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// rejectDetail is the audit_log detail blob for a message.rejected row.
type rejectDetail struct {
	Category string `json:"category"`
	Reason   string `json:"reason"`
	File     string `json:"file,omitempty"`
	Line     int    `json:"line,omitempty"`
	Pattern  string `json:"pattern,omitempty"`
}

// refuseDetail is the audit_log detail blob for a strategy.refused row.
type refuseDetail struct {
	Reason string `json:"reason"`
}

// generateDetail is the audit_log detail blob for strategy.* rows.
type generateDetail struct {
	Reason   string `json:"reason,omitempty"`
	Verified bool   `json:"verified"`
}

// repairDetail is the audit_log detail blob for a strategy.repair row.
type repairDetail struct {
	Attempt  int    `json:"attempt"`
	Category string `json:"category"`
	Message  string `json:"message"`
}

// mustMarshalDetail marshals an audit detail struct to JSON, falling back to an
// empty object so an audit write never fails the turn. All detail structs are
// marshalable by construction; the fallback is defensive.
func mustMarshalDetail(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// errString returns err.Error() or "" for a nil error — used in audit detail.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
