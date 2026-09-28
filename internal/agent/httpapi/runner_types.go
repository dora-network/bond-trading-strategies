package httpapi

// runner_types.go holds the strategy-runner seam types shared by the turn
// driver (turn.go), the production runner (strategy_runner.go), and their
// tests. They were originally declared in the runner file; that file
// was removed when the runners consolidated on the shared driver.

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/internal/agent/audit"
	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
)

// ProviderFactory builds the llm.Provider used for both the classify hop and
// the strategy-building hop of a turn. It receives the per-turn userID (so the
// factory resolves the user's own saved config) and returns the provider and
// the resolved model name. Splitting construction out of the driver lets the
// driver share one provider across both hops and lets tests inject a fake
// provider without touching the provider-config store.
type ProviderFactory func(ctx context.Context, userID, provider, model string) (llm.Provider, string, error)

// Classifier abstracts the sealed LLM classify hop (spec §16.2). The concrete
// implementation is sanitize.Classifier; tests inject a fake.
type Classifier interface {
	Classify(ctx context.Context, prompt, priorAssistant string) (sanitize.ClassifyVerdict, error)
}

// ClassifierFactory builds a Classifier bound to the same provider+model the
// strategy-building hop uses (spec §16.2). The factory receives the built
// llm.Provider and resolved model from the driver — no need to rebuild them
// (which was the prior seam's cost).
type ClassifierFactory func(provider llm.Provider, model string) Classifier

// ToolFactory builds the per-turn tool specs and handlers. The Dora read tools
// need the user's per-request Dora API key, so the handlers are constructed
// fresh each turn after the runner resolves the credential. The userID is
// passed so per-user tools (e.g. get_backtest_result) can enforce ownership.
// Returns the tool specs (for the LLM's tool declarations) and the handler
// dispatch map.
type ToolFactory func(doraAPIKey, userID string) ([]llm.ToolSpec, map[string]llm.ToolHandler)

// AuditWriter abstracts audit_log writes so the runner is testable without a
// Postgres pool. The concrete adapter wraps audit.Insert.
type AuditWriter interface {
	Insert(ctx context.Context, userID, action string, detail []byte) error
}

// PoolAuditWriter adapts a *pgxpool.Pool to AuditWriter.
type PoolAuditWriter struct{ Pool *pgxpool.Pool }

// Insert delegates to audit.Insert.
func (w PoolAuditWriter) Insert(ctx context.Context, userID, action string, detail []byte) error {
	return audit.Insert(ctx, w.Pool, userID, action, detail)
}
