package vwap

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/exec"
)

func validConfig() Config {
	return Config{
		OrderBookID:   "01900000-0000-0000-0000-000000000001",
		TotalAmount:   decimal.MustNew(1000, 0),
		Side:          "buy",
		StartTime:     time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
		EndTime:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		WindowDays:    30,
		BucketMinutes: 5,
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	t.Run("valid", func(t *testing.T) {
		require.NoError(t, validConfig().Validate())
	})
	t.Run("missing order_book_id", func(t *testing.T) {
		cfg := validConfig()
		cfg.OrderBookID = ""
		require.Error(t, cfg.Validate())
	})
	t.Run("zero total_amount", func(t *testing.T) {
		cfg := validConfig()
		cfg.TotalAmount = decimal.Zero
		require.Error(t, cfg.Validate())
	})
	t.Run("negative total_amount", func(t *testing.T) {
		cfg := validConfig()
		cfg.TotalAmount = decimal.MustNew(-100, 0)
		require.Error(t, cfg.Validate())
	})
	t.Run("invalid side", func(t *testing.T) {
		cfg := validConfig()
		cfg.Side = "hold"
		require.Error(t, cfg.Validate())
	})
	t.Run("end equal to start", func(t *testing.T) {
		cfg := validConfig()
		cfg.EndTime = cfg.StartTime
		require.Error(t, cfg.Validate())
	})
	t.Run("zero window_days", func(t *testing.T) {
		cfg := validConfig()
		cfg.WindowDays = 0
		require.Error(t, cfg.Validate())
	})
	t.Run("zero bucket_minutes", func(t *testing.T) {
		cfg := validConfig()
		cfg.BucketMinutes = 0
		require.Error(t, cfg.Validate())
	})
}

func TestConfigNumBuckets(t *testing.T) {
	t.Parallel()
	t.Run("one hour at 5-min buckets", func(t *testing.T) {
		cfg := validConfig()
		require.Equal(t, 12, cfg.NumBuckets())
	})
	t.Run("zero duration returns zero", func(t *testing.T) {
		cfg := validConfig()
		cfg.EndTime = cfg.StartTime
		require.Equal(t, 0, cfg.NumBuckets())
	})
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	d := DefaultConfig()
	require.Equal(t, 30, d.WindowDays)
	require.Equal(t, 5, d.BucketMinutes)
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status string
		want   bool
	}{
		{"OPEN", false},
		{"FILLED", true},
		{"PARTIAL_FILL", true},
		{"CANCELLED", true},
		{"UNKNOWN", true},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			require.Equal(t, c.want, exec.IsTerminal(c.status))
		})
	}
}
