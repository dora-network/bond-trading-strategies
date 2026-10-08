package http

import (
	"encoding/json"
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeMomentumConfigStopLossATR pins that the decoded
// momentum.Config carries StopLossATR/TakeProfitATR — the P1 review
// found the return literal dropped StopLossATR while the normalized
// JSON still echoed it, silently disabling stop-losses for every HTTP
// momentum run/backtest.
func TestDecodeMomentumConfigStopLossATR(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(map[string]any{
		"order_book_id":   "11111111-1111-1111-1111-111111111111",
		"stop_loss_atr":   2.5,
		"take_profit_atr": 4.0,
	})
	require.NoError(t, err)

	cfg, _, err := decodeMomentumConfig(raw, true)
	require.NoError(t, err)
	wantSL, err := decimal.NewFromFloat64(2.5)
	require.NoError(t, err)
	wantTP, err := decimal.NewFromFloat64(4.0)
	require.NoError(t, err)
	assert.Zero(t, cfg.StopLossATR.Cmp(wantSL),
		"decoded momentum.Config must carry the explicit stop_loss_atr; got %s", cfg.StopLossATR)
	assert.Zero(t, cfg.TakeProfitATR.Cmp(wantTP),
		"decoded momentum.Config must carry the explicit take_profit_atr; got %s", cfg.TakeProfitATR)
}
