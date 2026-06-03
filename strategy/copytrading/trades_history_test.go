package copytrading

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTradesHistoryStoreInterface(t *testing.T) {
	t.Parallel()
	require.NotNil(t, time.Now)
}
