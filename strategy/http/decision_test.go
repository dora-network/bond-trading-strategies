package http_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
)

// TestDecision_JSONFieldNames locks the public JSON field names of
// strategy.Decision. Adding a field without a tag, or renaming a tag,
// breaks the trading-decisions API contract — this test catches it.
func TestDecision_JSONFieldNames(t *testing.T) {
	t.Parallel()

	d := strategycore.Decision{
		RunID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Seq:                7,
		StrategyType:       "mean_reversion",
		OrderBookID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		Asset:              uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Side:               "BUY",
		Signal:             "buy",
		Kind:               strategycore.DecisionKindOpen,
		Quantity:           decimal.MustNew(100, 0),
		Price:              decimal.MustParse("98.5"),
		Leverage:           decimal.MustNew(1, 0),
		InverseLeverage:    decimal.MustNew(1, 0),
		FromGlobalPosition: false,
		Reason:             "z_score_entry",
		ReasonDetail:       "z=-2.4",
		ClientOrderID:      "mean_reversion.11111111-1111-1111-1111-111111111111.0",
		CreatedAt:          time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	b, err := json.Marshal(d)
	require.NoError(t, err)

	want := []string{
		`"run_id":`,
		`"seq":7`,
		`"strategy_type":"mean_reversion"`,
		`"order_book_id":`,
		`"asset":`,
		`"side":"BUY"`,
		`"signal":"buy"`,
		`"kind":"open"`,
		`"quantity":"100"`,
		`"price":"98.5"`,
		`"leverage":"1"`,
		`"inverse_leverage":"1"`,
		`"from_global_position":false`,
		`"reason":"z_score_entry"`,
		`"reason_detail":"z=-2.4"`,
		`"client_order_id":"mean_reversion.11111111-1111-1111-1111-111111111111.0"`,
		`"created_at":"2026-01-15T10:30:00Z"`,
	}
	encoded := string(b)
	for _, sub := range want {
		assert.Truef(t, strings.Contains(encoded, sub),
			"expected marshal output to contain %s, got %s", sub, encoded)
	}
}
