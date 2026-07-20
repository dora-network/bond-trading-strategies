package twap

import (
	"testing"
	"time"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	validCfg := Config{
		OrderBookID:     "123e4567-e89b-12d3-a456-426614174000",
		TotalAmount:     decimal.MustNew(1000000, 0),
		Side:            "buy",
		StartTime:       time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC),
		EndTime:         time.Date(2025, 1, 15, 16, 0, 0, 0, time.UTC),
		IntervalSeconds: 300,
	}

	t.Run("valid config", func(t *testing.T) {
		err := validCfg.Validate()
		require.NoError(t, err)
	})

	t.Run("missing order_book_id", func(t *testing.T) {
		cfg := validCfg
		cfg.OrderBookID = ""
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "order_book_id is required")
	})

	t.Run("zero total_amount", func(t *testing.T) {
		cfg := validCfg
		cfg.TotalAmount = decimal.Zero
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "total_amount must be positive")
	})

	t.Run("negative total_amount", func(t *testing.T) {
		cfg := validCfg
		cfg.TotalAmount = decimal.MustParse("-1000")
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "total_amount must be positive")
	})

	t.Run("invalid side", func(t *testing.T) {
		cfg := validCfg
		cfg.Side = "hold"
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "side must be 'buy' or 'sell'")
	})

	t.Run("missing start_time", func(t *testing.T) {
		cfg := validCfg
		cfg.StartTime = time.Time{}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "start_time and end_time are required")
	})

	t.Run("missing end_time", func(t *testing.T) {
		cfg := validCfg
		cfg.EndTime = time.Time{}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "start_time and end_time are required")
	})

	t.Run("end before start", func(t *testing.T) {
		cfg := validCfg
		cfg.StartTime = time.Date(2025, 1, 15, 16, 0, 0, 0, time.UTC)
		cfg.EndTime = time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "end_time must be strictly after start_time")
	})

	t.Run("end equal to start", func(t *testing.T) {
		cfg := validCfg
		cfg.StartTime = time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC)
		cfg.EndTime = time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC)
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "end_time must be strictly after start_time")
	})

	t.Run("zero interval", func(t *testing.T) {
		cfg := validCfg
		cfg.IntervalSeconds = 0
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "interval_seconds must be positive")
	})
}

func TestConfigNumChunks(t *testing.T) {
	t.Run("one hour window with 5 minute intervals", func(t *testing.T) {
		cfg := Config{
			StartTime:       time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:         time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			IntervalSeconds: 300,
		}
		require.Equal(t, 12, cfg.NumChunks())
	})

	t.Run("one hour window with 10 minute intervals", func(t *testing.T) {
		cfg := Config{
			StartTime:       time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:         time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			IntervalSeconds: 600,
		}
		require.Equal(t, 6, cfg.NumChunks())
	})

	t.Run("zero duration returns zero chunks", func(t *testing.T) {
		cfg := Config{
			StartTime:       time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:         time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			IntervalSeconds: 300,
		}
		require.Equal(t, 0, cfg.NumChunks())
	})

	t.Run("negative duration returns zero chunks", func(t *testing.T) {
		cfg := Config{
			StartTime:       time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
			EndTime:         time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			IntervalSeconds: 300,
		}
		require.Equal(t, 0, cfg.NumChunks())
	})

	t.Run("short window less than interval returns one chunk", func(t *testing.T) {
		cfg := Config{
			StartTime:       time.Date(2025, 1, 15, 9, 0, 0, 0, time.UTC),
			EndTime:         time.Date(2025, 1, 15, 9, 4, 0, 0, time.UTC),
			IntervalSeconds: 300,
		}
		require.Equal(t, 1, cfg.NumChunks())
	})
}

func TestDefaultConfig(t *testing.T) {
	defaults := DefaultConfig()
	require.Equal(t, 300, defaults.IntervalSeconds)
}
