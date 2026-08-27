package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// TestMeanReversionDecision_SatisfiesDecision is a compile-time guard
// that meanreversion.Decision still satisfies types.Decision. Breaks
// the build if either interface changes incompatibly.
func TestMeanReversionDecision_SatisfiesDecision(t *testing.T) {
	t.Parallel()
	var _ types.Decision = meanreversion.Decision{}
	assert.True(t, true, "compile-time conformance")
}
