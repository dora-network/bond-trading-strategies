package copytrading

import (
	"testing"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
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

func TestFromGlobalPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		side     doraclient.Side
		current  positionDirection
		leverage decimal.Decimal
		want     bool
	}{
		// Closes: never need DORA leverage, regardless of cfg.Leverage.
		{
			name:     "SELL closing a long → global (close)",
			side:     doraclient.SIDE_SELL,
			current:  positionLong,
			leverage: decimal.MustParse("1.0"),
			want:     true,
		},
		{
			name:     "SELL closing a long, with leverage → global (close)",
			side:     doraclient.SIDE_SELL,
			current:  positionLong,
			leverage: decimal.MustParse("2.0"),
			want:     true,
		},
		{
			name:     "BUY closing a short → global (close)",
			side:     doraclient.SIDE_BUY,
			current:  positionShort,
			leverage: decimal.MustParse("1.0"),
			want:     true,
		},
		{
			name:     "BUY closing a short, with leverage → global (close)",
			side:     doraclient.SIDE_BUY,
			current:  positionShort,
			leverage: decimal.MustParse("2.0"),
			want:     true,
		},

		// Opens of a long: fromGlobal depends on leverage.
		{
			name:     "BUY opening a long, no leverage → global",
			side:     doraclient.SIDE_BUY,
			current:  positionFlat,
			leverage: decimal.MustParse("1.0"),
			want:     true,
		},
		{
			name:     "BUY opening a long, leveraged → isolated",
			side:     doraclient.SIDE_BUY,
			current:  positionFlat,
			leverage: decimal.MustParse("2.0"),
			want:     false,
		},
		{
			name:     "BUY extending a long, no leverage → global",
			side:     doraclient.SIDE_BUY,
			current:  positionLong,
			leverage: decimal.MustParse("1.0"),
			want:     true,
		},
		{
			name:     "BUY extending a long, leveraged → isolated",
			side:     doraclient.SIDE_BUY,
			current:  positionLong,
			leverage: decimal.MustParse("2.0"),
			want:     false,
		},

		// Opens/extends of a short: always need DORA leverage.
		{
			name:     "SELL opening a short, no leverage → isolated (shorting requires leverage)",
			side:     doraclient.SIDE_SELL,
			current:  positionFlat,
			leverage: decimal.MustParse("1.0"),
			want:     false,
		},
		{
			name:     "SELL opening a short, leveraged → isolated",
			side:     doraclient.SIDE_SELL,
			current:  positionFlat,
			leverage: decimal.MustParse("2.0"),
			want:     false,
		},
		{
			name:     "SELL extending a short, no leverage → isolated",
			side:     doraclient.SIDE_SELL,
			current:  positionShort,
			leverage: decimal.MustParse("1.0"),
			want:     false,
		},
		{
			name:     "SELL extending a short, leveraged → isolated",
			side:     doraclient.SIDE_SELL,
			current:  positionShort,
			leverage: decimal.MustParse("2.0"),
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, fromGlobalPosition(tt.side, tt.current, tt.leverage))
		})
	}
}

func TestInverseLeverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		leverage decimal.Decimal
		want     string
	}{
		{name: "leverage 1.0 → 1.0", leverage: decimal.MustParse("1.0"), want: "1"},
		{name: "leverage 2.0 → 0.5", leverage: decimal.MustParse("2.0"), want: "0.5"},
		{name: "leverage 3.0 → 0.333...", leverage: decimal.MustParse("3.0"), want: "0.3333333333333333333"},
		{name: "leverage 0 → 1 (degenerate)", leverage: decimal.Zero, want: "1"},
		{name: "leverage 0.5 → 1 (degenerate, only leverage>1 triggers division)", leverage: decimal.MustParse("0.5"), want: "1"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inverse := decimal.One
			if tt.leverage.IsPos() && tt.leverage.Cmp(decimal.One) > 0 {
				inverse, _ = decimal.One.Quo(tt.leverage)
			}
			require.True(t, inverse.Equal(decimal.MustParse(tt.want)),
				"expected %s, got %s", tt.want, inverse)
		})
	}
}

func TestAvailableBalanceFor(t *testing.T) {
	t.Parallel()

	assetA := uuid.New().String()
	assetB := uuid.New().String()

	// Build a portfolio with both a global and an isolated account
	// for assetA, plus a global account for assetB. IsGlobal is
	// pointer-typed in DORA's model; we set it explicitly.
	yes := true
	no := false
	portfolio := &doraclient.AccountPortfolioV2{
		Accounts: map[string]map[string]doraclient.AccountV2{
			"global-A": {
				assetA: {AssetId: assetA, IsGlobal: &yes, Available: "1000"},
			},
			"isolated-A": {
				assetA: {AssetId: assetA, IsGlobal: &no, Available: "250"},
			},
			"global-B": {
				assetB: {AssetId: assetB, IsGlobal: &yes, Available: "777"},
			},
		},
	}

	s := New(Config{}) // strategy shell — we only call the method, no state needed

	tests := []struct {
		name       string
		assetID    string
		fromGlobal bool
		want       string
	}{
		{
			name:       "fromGlobal=true picks global account for asset",
			assetID:    assetA,
			fromGlobal: true,
			want:       "1000",
		},
		{
			name:       "fromGlobal=false picks isolated account for asset",
			assetID:    assetA,
			fromGlobal: false,
			want:       "250",
		},
		{
			name:       "fromGlobal=false with no isolated account falls back to global",
			assetID:    assetB,
			fromGlobal: false,
			want:       "777",
		},
		{
			name:       "unknown asset returns zero",
			assetID:    uuid.New().String(),
			fromGlobal: true,
			want:       "0",
		},
		{
			name:       "nil portfolio returns zero",
			assetID:    assetA,
			fromGlobal: true,
			want:       "0",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got decimal.Decimal
			if tt.name == "nil portfolio returns zero" {
				got = s.availableBalanceFor(nil, tt.assetID, tt.fromGlobal)
			} else {
				got = s.availableBalanceFor(portfolio, tt.assetID, tt.fromGlobal)
			}
			require.True(t, got.Equal(decimal.MustParse(tt.want)),
				"expected %s, got %s", tt.want, got)
		})
	}
}
