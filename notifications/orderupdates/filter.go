// Package orderupdates owns the per-DORA-user subscription to DORA's
// /v1/user/{userID}/orders/all/updates/stream and forwards order
// state changes that belong to the user's currently-running strategies
// onto the existing notifications bus.
//
// The package has no cross-package type dependency on strategy/http:
// RunLookupFunc is the function-typed seam that lets callers inject a
// strategy_runs lookup callback without dragging in any strategy/http
// type (which would create an import cycle: strategy/http → notifications
// → notifications/orderupdates → strategy/http).
package orderupdates

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/dora-network/bond-trading-strategies/notifications"
)

// RunLookupFunc resolves a strategy_runs id to (doraUserID, status, found).
// found=false means the run is unknown — the Filter treats this as drop.
//
// Production wiring in cmd/strategy-server/main.go builds this by
// wrapping (*strategyhttp.PGRunStore).LookupRunByID.
type RunLookupFunc func(ctx context.Context, runID uuid.UUID) (doraUserID, status string, found bool)

// Filter translates DORA order-update entries into notifications.Event.
// It is the only place that knows about the client_order_id prefix contract.
//
// Lifecycle: one Filter per Manager.runSubscription goroutine. Safe to
// share only if the underlying RunLookupFunc is concurrency-safe (the
// production closure over *PGRunStore is).
type Filter struct {
	knownStrategyTypes map[string]struct{}
	lookup             RunLookupFunc
}

// NewFilter returns a Filter that recognises the supplied strategy-type
// prefixes as the leading segment of client_order_id. The slice is copied
// internally; later mutations do not affect the Filter.
func NewFilter(lookup RunLookupFunc, strategyTypes []string) *Filter {
	known := make(map[string]struct{}, len(strategyTypes))
	for _, t := range strategyTypes {
		known[t] = struct{}{}
	}
	return &Filter{knownStrategyTypes: known, lookup: lookup}
}

// clientOrderIDSegments is the number of "."-separated segments in a
// strategy-placed client_order_id: "<strategy_name>.<run_id>.<uuidv7>".
const clientOrderIDSegments = 3

// Translate parses val["client_order_id"], looks up the run, and returns
// the Event to publish. ok=false means the entry should be silently
// dropped.
//
// An entry is forwarded iff:
//   - val["client_order_id"] is a non-empty string shaped
//     "<strategy_name>.<run_id>.<uuidv7>"
//   - strategy_name is in knownStrategyTypes
//   - run_id parses as a uuid.UUID
//   - the lookup returns (doraUserID, status, true), the returned
//     doraUserID matches the subscription user, and the status is
//     in {"running","paused"}
func (f *Filter) Translate(
	ctx context.Context,
	val map[string]any,
	doraUserID string,
) (notifications.Event, bool) {
	raw, ok := val["client_order_id"].(string)
	if !ok || raw == "" {
		return notifications.Event{}, false
	}
	parts := strings.SplitN(raw, ".", clientOrderIDSegments)
	if len(parts) != clientOrderIDSegments {
		return notifications.Event{}, false
	}
	if _, ok := f.knownStrategyTypes[parts[0]]; !ok {
		return notifications.Event{}, false
	}
	runID, err := uuid.Parse(parts[1])
	if err != nil {
		return notifications.Event{}, false
	}

	runDORAUserID, status, found := f.lookup(ctx, runID)
	if !found {
		return notifications.Event{}, false
	}
	if runDORAUserID != doraUserID {
		return notifications.Event{}, false
	}
	if status != "running" && status != "paused" {
		return notifications.Event{}, false
	}

	return notifications.Event{
		Type:    notifications.EventOrderUpdate,
		UserID:  doraUserID,
		RunID:   runID.String(),
		Payload: val,
	}, true
}
