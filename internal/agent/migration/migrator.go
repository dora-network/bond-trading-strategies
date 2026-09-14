// Package migration owns the cross-package machinery for migrating
// legacy strategy versions to the current framework. The agent
// encounters two flavours of "this row was built on something older":
//
//  1. The Docker pipeline was removed on 2026-09-04; rows whose
//     target = 'go-docker' (or whose image_ref was set without a
//     target) have no compiled .wasm artifact. The LLM must rebuild
//     them on the current WASM framework before any backtest or
//     deployment can succeed.
//
//  2. The WASM framework itself (prompts.WasmFrameworkVersion) is
//     bumped when the strategywasm repo tags a new release; the host
//     rejects any plugin whose manifest.framework_version doesn't match.
//     The LLM is expected to rebuild on this typed error (existing
//     'REBUILD ON VERSION MISMATCH' prompt block handles case 2).
//
// This package owns case 1 — a pre-flight check on the resolved
// version + an in-flight map that gates concurrent rebuilds for the
// same (user, strategy). Case 2 continues to be handled by the
// host's ErrFrameworkVersionMismatch returning from Registry.Load.
package migration

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// reserveTTL is how long a Reserve() entry lives before auto-expiring.
// Default 5 minutes; long enough to cover a normal generate_strategy
// turn (validator + TinyGo build + capture) but short enough that a
// crashed goroutine doesn't permanently block the strategy.
const reserveTTL = 5 * time.Minute

// ErrStaleFramework is returned by tool handlers when the resolved
// version was built on a superseded framework (currently:
// target != "go-wasm"). The agent loop surfaces this as a
// RecoveryError so the next iteration forces generate_strategy.
//
// The err carries the strategy_id + current_revision_id in a JSON
// hint payload so the LLM doesn't need to round-trip back to
// get_strategy before rebuilding. MarshalJSON returns the hint;
// Error() returns the plain-prose form the user sees in chat UIs
// when the error bubbles up.
type ErrStaleFramework struct {
	StrategyID        string
	CurrentRevisionID string
	Target            string
}

func (e *ErrStaleFramework) Error() string {
	switch e.Target {
	case "go-wasm":
		return fmt.Sprintf(
			"strategy %q revision %q has no compiled wasm artifact "+
				"(target=%q, wasm_ref empty); call generate_strategy(strategy_id=%q) "+
				"to rebuild on the current framework, then retry with the new revision_id.",
			e.StrategyID, e.CurrentRevisionID, e.Target, e.StrategyID,
		)
	default:
		return fmt.Sprintf(
			"strategy %q revision %q was built on target=%q; "+
				"the current target is go-wasm. Call generate_strategy(strategy_id=%q) "+
				"to rebuild on the current framework, then retry with the new revision_id.",
			e.StrategyID, e.CurrentRevisionID, e.Target, e.StrategyID,
		)
	}
}

// MarshalJSON renders the structured hint payload the agent loop
// concatenates into the RoleTool message body so the LLM can read
// strategy_id without parsing prose.
func (e *ErrStaleFramework) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Action            string `json:"action"`
		StrategyID        string `json:"strategy_id"`
		CurrentRevisionID string `json:"current_revision_id"`
		Reason            string `json:"reason"`
	}{
		Action:            "rebuild",
		StrategyID:        e.StrategyID,
		CurrentRevisionID: e.CurrentRevisionID,
		Reason:            "stale_framework",
	})
}

// ErrRebuildInFlight is returned when the user (LLM) tries to
// run_backtest or deploy_strategy on a legacy row while a previous
// rebuild is still in flight. Mirrors bt.ErrSingleInflight's UX:
// "rebuild already in flight; call get_strategy to read the new
// revision_id, then retry the original tool".
var ErrRebuildInFlight = errors.New("migration: rebuild already in flight")

// Migrator is the cross-package facade. Stateless apart from the
// in-flight map.
type Migrator struct {
	mu       sync.Mutex
	inflight map[string]time.Time // key = userID + "|" + strategyID
	ttl      time.Duration
}

// New returns a Migrator with the default reserveTTL.
func New() *Migrator {
	return &Migrator{inflight: make(map[string]time.Time), ttl: reserveTTL}
}

// NeedsMigration reports whether v should be rebuilt before any
// run_backtest / deploy_strategy. Currently: target != "go-wasm"
// or empty WasmRef. Kept as a method (not a free function) so
// future migration axes (framework tag bump, capability gap, etc.)
// can extend the rule without changing every call site.
func (m *Migrator) NeedsMigration(v strategies.Version) bool {
	return v.Target != "go-wasm" || v.WasmRef == ""
}

// Reserve marks (userID, strategyID) as a rebuild-in-progress and
// returns ErrRebuildInFlight if it's already reserved. The
// generate_strategy tool calls this on entry and Release on the
// strategy-version capture (success or terminal failure). The TTL
// is the safety net for crashes between reserve and release.
func (m *Migrator) Reserve(userID, strategyID string) error {
	key := userID + "|" + strategyID
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if reservedAt, ok := m.inflight[key]; ok && now.Sub(reservedAt) < m.ttl {
		return ErrRebuildInFlight
	}
	m.inflight[key] = now
	return nil
}

// Release clears the reserve entry. Idempotent; safe to defer.
func (m *Migrator) Release(userID, strategyID string) {
	key := userID + "|" + strategyID
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight, key)
}
