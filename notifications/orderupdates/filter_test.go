package orderupdates_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/notifications/orderupdates"
)

// lookupRow captures a single fake-run result for filter tests.
type lookupRow struct {
	doraUserID string
	status     string
}

// staticLookupFunc builds a RunLookupFunc backed by an in-memory map.
func staticLookupFunc(rows map[uuid.UUID]lookupRow) orderupdates.RunLookupFunc {
	return func(_ context.Context, id uuid.UUID) (string, string, bool) {
		row, ok := rows[id]
		if !ok {
			return "", "", false
		}
		return row.doraUserID, row.status, true
	}
}

func TestFilter_ForwardsKnownRun(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "running"},
		}),
		[]string{"mean_reversion", "copy_trading"},
	)

	evt, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
		"order_id":        "ord-1",
		"status":          "placed",
		"updated_at":      "2026-07-09T12:34:56Z",
	}, "alice")

	require.True(t, ok)
	assert.Equal(t, notifications.EventOrderUpdate, evt.Type)
	assert.Equal(t, "alice", evt.UserID)
	assert.Equal(t, runID.String(), evt.RunID)
	assert.NotEmpty(t, evt.ID, "Translate must populate ID for the bus log to parse it")
	assert.False(t, evt.Timestamp.IsZero(), "Translate must populate Timestamp")
	payload, ok := evt.Payload.(map[string]any)
	require.True(t, ok, "payload should be the original val map")
	assert.Equal(t, "ord-1", payload["order_id"])
}

func TestFilter_DropsUnknownStrategy(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "running"},
		}),
		[]string{"mean_reversion", "copy_trading"},
	)

	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "botnet." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
	}, "alice")
	assert.False(t, ok)
}

func TestFilter_DropsMalformedClientOrderID(t *testing.T) {
	t.Parallel()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{}),
		[]string{"mean_reversion", "copy_trading"},
	)

	for _, tc := range []string{
		"",
		"mean_reversion_notuuid_suffix",
		"mean_reversion.<bad uuid>.<suffix>",
		"mean_reversion.11111111-1111-1111-1111-111111111111",
		"only-two-parts",
	} {
		_, ok := f.Translate(context.Background(), map[string]any{
			"client_order_id": tc,
		}, "alice")
		assert.Falsef(t, ok, "expected drop for %q", tc)
	}
}

func TestFilter_DropsRunForOtherUser(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "running"},
		}),
		[]string{"mean_reversion"},
	)

	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
	}, "bob")
	assert.False(t, ok)
}

func TestFilter_DropsStoppedRun(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "stopped"},
		}),
		[]string{"mean_reversion"},
	)

	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
	}, "alice")
	assert.False(t, ok)
}

func TestFilter_PassesPausedRun(t *testing.T) {
	// Spec says paused runs still forward — they're "live" strategies.
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "paused"},
		}),
		[]string{"mean_reversion"},
	)

	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
	}, "alice")
	assert.True(t, ok, "paused runs should forward")
}

func TestFilter_DropsMissingRun(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{}),
		[]string{"mean_reversion"},
	)

	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".019ed20fcfc77167a3184b42d0582517",
	}, "alice")
	assert.False(t, ok)
}

func TestFilter_AllowsUnknownStrategyWhenPrefixListIsEmpty(t *testing.T) {
	// Pathological wiring: empty prefix list rejects everything. Test
	// the inverse contract: passing nil/empty drops EVERY prefix.
	t.Parallel()
	runID := uuid.New()
	f := orderupdates.NewFilter(
		staticLookupFunc(map[uuid.UUID]lookupRow{
			runID: {"alice", "running"},
		}),
		nil,
	)
	_, ok := f.Translate(context.Background(), map[string]any{
		"client_order_id": "mean_reversion." + runID.String() + ".x",
	}, "alice")
	assert.False(t, ok, "empty known-prefix list should drop everything")
}
