package breakout_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestStrategyTypeExported locks the breakout StrategyType constant.
// cmd/strategy-server passes this value into the orderupdates.Filter
// alongside meanreversion.StrategyType and copytrading.StrategyType;
// renaming or unexporting it breaks the wiring silently.
func TestStrategyTypeExported(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "breakout", breakout.StrategyType,
		"StrategyType must be exported and equal to the documented value")
}

// TestBreakoutDecision_SatisfiesDecision is a compile-time guard that
// breakout.Decision satisfies types.Decision. Mirrors the equivalent test
// for meanreversion.Decision in strategy/types/types_test.go.
func TestBreakoutDecision_SatisfiesDecision(t *testing.T) {
	t.Parallel()
	var _ types.Decision = breakout.Decision{}
	assert.True(t, true, "compile-time conformance")
}
