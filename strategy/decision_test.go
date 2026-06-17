package strategy

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildClientOrderID_Format pins the contract that the live-run
// client_order_id is formatted as <strategy_name>.<run_id>.<uuidv7>.
// The uuidv7 suffix is unique per call (verified by running the
// helper twice) and is a parseable UUID.
func TestBuildClientOrderID_Format(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	got := BuildClientOrderID("mean_reversion", runID)

	parts := strings.SplitN(got, ".", 3)
	require.Len(t, parts, 3, "expected <strategy_name>.<run_id>.<uuidv7>, got %q", got)
	assert.Equal(t, "mean_reversion", parts[0])
	assert.Equal(t, runID.String(), parts[1])
	_, err := uuid.Parse(parts[2])
	require.NoError(t, err, "uuidv7 segment must be a valid UUID: %q", parts[2])
}

// TestBuildClientOrderID_UniquePerCall verifies that two consecutive
// calls produce different client_order_ids (the uuidv7 segment is
// fresh each time).  Without this, a buggy implementation that
// cached the suffix would let two orders share an id and confuse
// DORA's dedup logic.
func TestBuildClientOrderID_UniquePerCall(t *testing.T) {
	t.Parallel()

	runID := uuid.New()
	first := BuildClientOrderID("copy_trading", runID)
	second := BuildClientOrderID("copy_trading", runID)
	assert.NotEqual(t, first, second, "client_order_id must be unique per order")
}
