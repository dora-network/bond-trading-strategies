package exec_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/dora-client-go/doraclient"

	"github.com/dora-network/bond-trading-strategies/strategy"
	strategyfakes "github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

func ptr(s string) *string { return &s }

func newExecutor() *exec.Executor {
	return &exec.Executor{
		Name:  "twap",
		RunID: uuid.New(),
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newState(initial []exec.OrderEntry) *exec.RunState {
	return &exec.RunState{Orders: append([]exec.OrderEntry(nil), initial...)}
}

// TestExecutor_ImportOrphanedOrders_AppendsNew pins the crash-window
// recovery: DORA has two orders, persisted state has none; both are
// appended and TotalSubmitted bumps by the sum of their requested
// quantities. Without this, the next-chunk rebalance would treat
// them as un-submitted and over-place.
func TestExecutor_ImportOrphanedOrders_AppendsNew(t *testing.T) {
	t.Parallel()

	market := &strategyfakes.FakeMarketAPIClient{}
	market.ListOrdersByClientOrderIDPrefixStub = func(ctx context.Context, prefix string) ([]doraclient.Order, error) {
		return []doraclient.Order{
			{OrderId: "order-1", ClientOrderId: ptr("twap.run.aaa"), OriginalQuantity: "200", Status: "FILLED"},
			{OrderId: "order-2", ClientOrderId: ptr("twap.run.bbb"), OriginalQuantity: "300", Status: "OPEN"},
		}, nil
	}

	e := newExecutor()
	e.Market = market
	state := newState(nil)

	e.ImportOrphanedOrders(context.Background(), state, "twap.run.")

	require.Len(t, state.Orders, 2, "both DORA orders must be imported")
	assert.Equal(t, "order-1", state.Orders[0].OrderID)
	assert.Equal(t, "twap.run.aaa", state.Orders[0].ClientOrderID)
	assert.Equal(t, "200", state.Orders[0].RequestedQuantity.String())
	assert.Equal(t, "FILLED", state.Orders[0].Status)
	assert.Equal(t, "500", state.TotalSubmitted.String(),
		"TotalSubmitted must bump by the sum of imported requested quantities")
}

// TestExecutor_ImportOrphanedOrders_SkipsExisting pins the diff:
// an order already in persisted state (matched by client_order_id)
// is not re-imported, and its TotalSubmitted contribution is not
// double-counted.
func TestExecutor_ImportOrphanedOrders_SkipsExisting(t *testing.T) {
	t.Parallel()

	market := &strategyfakes.FakeMarketAPIClient{}
	market.ListOrdersByClientOrderIDPrefixStub = func(ctx context.Context, prefix string) ([]doraclient.Order, error) {
		return []doraclient.Order{
			{OrderId: "order-1", ClientOrderId: ptr("twap.run.aaa"), OriginalQuantity: "200", Status: "FILLED"},
			{OrderId: "order-2", ClientOrderId: ptr("twap.run.bbb"), OriginalQuantity: "300", Status: "OPEN"},
		}, nil
	}

	e := newExecutor()
	e.Market = market
	state := newState([]exec.OrderEntry{
		{OrderID: "order-1", ClientOrderID: "twap.run.aaa", RequestedQuantity: decimal.MustNew(200, 0), Status: "OPEN"},
	})
	state.TotalSubmitted = decimal.MustNew(200, 0)

	e.ImportOrphanedOrders(context.Background(), state, "twap.run.")

	require.Len(t, state.Orders, 2, "only the missing order must be added")
	assert.Equal(t, "order-2", state.Orders[1].OrderID, "existing order must not move position")
	assert.Equal(t, "500", state.TotalSubmitted.String(),
		"TotalSubmitted adds only the missing 300, not 200+300")
}

// TestExecutor_ImportOrphanedOrders_EmptyAndError pins the failure
// modes: empty DORA list is a no-op (no error), and a list error is
// logged but does not panic or fail the restart.
func TestExecutor_ImportOrphanedOrders_EmptyAndError(t *testing.T) {
	t.Parallel()

	t.Run("empty list is a no-op", func(t *testing.T) {
		t.Parallel()
		market := &strategyfakes.FakeMarketAPIClient{}
		market.ListOrdersByClientOrderIDPrefixReturns([]doraclient.Order{}, nil)
		e := newExecutor()
		e.Market = market
		state := newState(nil)
		e.ImportOrphanedOrders(context.Background(), state, "twap.run.")
		assert.Empty(t, state.Orders)
		assert.True(t, state.TotalSubmitted.IsZero())
	})

	t.Run("error is non-fatal", func(t *testing.T) {
		t.Parallel()
		market := &strategyfakes.FakeMarketAPIClient{}
		market.ListOrdersByClientOrderIDPrefixReturns(nil, errors.New("dora unavailable"))
		e := newExecutor()
		e.Market = market
		state := newState(nil)
		// Must not panic.
		e.ImportOrphanedOrders(context.Background(), state, "twap.run.")
		assert.Empty(t, state.Orders)
	})

	t.Run("guards", func(t *testing.T) {
		t.Parallel()
		e := newExecutor()
		// e.Market is nil
		state := newState(nil)
		e.ImportOrphanedOrders(context.Background(), state, "twap.run.")
		assert.Empty(t, state.Orders)

		e2 := newExecutor()
		e2.Market = &strategyfakes.FakeMarketAPIClient{}
		e2.ImportOrphanedOrders(context.Background(), state, "")
		assert.Empty(t, state.Orders)
	})
}

// TestBuildClientOrderIDPrefix_Format pins the prefix shape: every
// client_order_id the run generates starts with "<strategy>.<run_id>."
// and the trailing dot prevents a sibling run from matching.
func TestBuildClientOrderIDPrefix_Format(t *testing.T) {
	t.Parallel()
	runID := uuid.MustParse("01900000-0000-0000-0000-000000000001")
	got := strategy.BuildClientOrderIDPrefix("twap", runID)
	assert.Equal(t, "twap.01900000-0000-0000-0000-000000000001.", got)

	otherRun := uuid.MustParse("01900000-0000-0000-0000-000000000002")
	thisPrefix := strategy.BuildClientOrderIDPrefix("twap", runID)
	otherPrefix := strategy.BuildClientOrderIDPrefix("twap", otherRun)
	assert.NotEqual(t, thisPrefix, otherPrefix)
}
