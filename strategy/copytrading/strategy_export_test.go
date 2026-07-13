package copytrading_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
)

func TestStrategyTypeExported(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "copy_trading", copytrading.StrategyType,
		"StrategyType must be exported and equal to the documented value")
}
