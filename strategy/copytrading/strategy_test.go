package copytrading

import (
	"testing"

	"github.com/govalues/decimal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewStrategy(t *testing.T) {
	followedTrader := uuid.New()
	poa := decimal.MustParse("0.1")
	lev := decimal.MustParse("3.0")

	cfg := Config{
		FollowedTrader:        followedTrader,
		PercentageOfAvailable: poa,
		Leverage:              lev,
		MinOrderSize:          100,
		MaxOrderSize:          10000,
	}

	s := New(cfg)
	require.Equal(t, followedTrader, s.cfg.FollowedTrader)
	require.Equal(t, poa, s.cfg.PercentageOfAvailable)
	require.Equal(t, lev, s.cfg.Leverage)
	require.Equal(t, 100, s.cfg.MinOrderSize)
	require.Equal(t, 10000, s.cfg.MaxOrderSize)
	require.Len(t, s.disallowedSet, 0)
}

func TestDisallowedSet(t *testing.T) {
	bond1 := uuid.New()
	bond2 := uuid.New()

	cfg := Config{
		FollowedTrader:        uuid.New(),
		PercentageOfAvailable: decimal.MustParse("0.1"),
		Leverage:              decimal.MustParse("1.0"),
		DisallowedBonds:       []uuid.UUID{bond1},
	}

	s := New(cfg)
	require.Contains(t, s.disallowedSet, bond1)
	require.NotContains(t, s.disallowedSet, bond2)
}

func TestCalculateOrderSize(t *testing.T) {
	tests := []struct {
		name         string
		available    decimal.Decimal
		percentage   decimal.Decimal
		leverage     decimal.Decimal
		minOrderSize int
		maxOrderSize int
		expected     decimal.Decimal
	}{
		{
			name:         "basic calculation",
			available:    decimal.MustParse("10000"),
			percentage:   decimal.MustParse("0.1"),
			leverage:     decimal.MustParse("3.0"),
			minOrderSize: 0,
			maxOrderSize: 0,
			expected:     decimal.MustParse("3000"),
		},
		{
			name:         "clamped by min",
			available:    decimal.MustParse("100"),
			percentage:   decimal.MustParse("0.1"),
			leverage:     decimal.MustParse("1.0"),
			minOrderSize: 50,
			maxOrderSize: 0,
			expected:     decimal.MustParse("50"),
		},
		{
			name:         "clamped by max",
			available:    decimal.MustParse("100000"),
			percentage:   decimal.MustParse("0.1"),
			leverage:     decimal.MustParse("3.0"),
			minOrderSize: 0,
			maxOrderSize: 1000,
			expected:     decimal.MustParse("1000"),
		},
		{
			name:         "clamped by both",
			available:    decimal.MustParse("100000"),
			percentage:   decimal.MustParse("0.1"),
			leverage:     decimal.MustParse("3.0"),
			minOrderSize: 500,
			maxOrderSize: 1000,
			expected:     decimal.MustParse("1000"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateOrderSize(tt.available, tt.percentage, tt.leverage, tt.minOrderSize, tt.maxOrderSize)
			require.True(t, result.Equal(tt.expected), "expected %s, got %s", tt.expected, result)
		})
	}
}
