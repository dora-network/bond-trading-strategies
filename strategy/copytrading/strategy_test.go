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
		name             string
		side             doraclient.Side
		current          positionDirection
		leverage         decimal.Decimal
		positionOnGlobal bool
		want             bool
	}{
		// Closes: fromGlobal mirrors the IsGlobal of the account
		// holding the existing position. Leverage is irrelevant here.
		{
			name:             "SELL closing a long in global account → global",
			side:             doraclient.SIDE_SELL,
			current:          positionLong,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: true,
			want:             true,
		},
		{
			name:             "SELL closing a long in isolated account → isolated",
			side:             doraclient.SIDE_SELL,
			current:          positionLong,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "SELL closing a long, leverage irrelevant (close on global)",
			side:             doraclient.SIDE_SELL,
			current:          positionLong,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: true,
			want:             true,
		},
		{
			name:             "BUY closing a short in global account → global",
			side:             doraclient.SIDE_BUY,
			current:          positionShort,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: true,
			want:             true,
		},
		{
			name:             "BUY closing a short in isolated account → isolated",
			side:             doraclient.SIDE_BUY,
			current:          positionShort,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "BUY closing a short, leverage irrelevant (close on isolated)",
			side:             doraclient.SIDE_BUY,
			current:          positionShort,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: false,
			want:             false,
		},

		// Opens/extends: fromGlobal depends on leverage only.
		// (The leverage rule: leverage ≤ 1 longs → global, leverage ≤ 1
		// shorts → isolated, leverage > 1 all → isolated.)
		{
			name:             "BUY opening a long, no leverage → global",
			side:             doraclient.SIDE_BUY,
			current:          positionFlat,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: false, // ignored for opens
			want:             true,
		},
		{
			name:             "BUY opening a long, leveraged → isolated",
			side:             doraclient.SIDE_BUY,
			current:          positionFlat,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "BUY extending a long, no leverage → global",
			side:             doraclient.SIDE_BUY,
			current:          positionLong,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: true, // ignored for extends
			want:             true,
		},
		{
			name:             "BUY extending a long, leveraged → isolated",
			side:             doraclient.SIDE_BUY,
			current:          positionLong,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: true,
			want:             false,
		},
		{
			name:             "SELL opening a short, no leverage → isolated (shorting requires leverage)",
			side:             doraclient.SIDE_SELL,
			current:          positionFlat,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "SELL opening a short, leveraged → isolated",
			side:             doraclient.SIDE_SELL,
			current:          positionFlat,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "SELL extending a short, no leverage → isolated",
			side:             doraclient.SIDE_SELL,
			current:          positionShort,
			leverage:         decimal.MustParse("1.0"),
			positionOnGlobal: false,
			want:             false,
		},
		{
			name:             "SELL extending a short, leveraged → isolated",
			side:             doraclient.SIDE_SELL,
			current:          positionShort,
			leverage:         decimal.MustParse("2.0"),
			positionOnGlobal: false,
			want:             false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, fromGlobalPosition(tt.side, tt.current, tt.leverage, tt.positionOnGlobal))
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

func TestBalanceAssetFor(t *testing.T) {
	t.Parallel()

	bond := uuid.New().String()
	usd := uuid.New().String()

	tests := []struct {
		name    string
		side    doraclient.Side
		current positionDirection
		want    string
	}{
		// Opens/extends always look at USD.
		{name: "flat + buy → USD (open long)", side: doraclient.SIDE_BUY, current: positionFlat, want: usd},
		{name: "flat + sell → USD (open short, USD is collateral)", side: doraclient.SIDE_SELL, current: positionFlat, want: usd},
		{name: "long + buy → USD (extend long)", side: doraclient.SIDE_BUY, current: positionLong, want: usd},
		{name: "short + sell → USD (extend short, USD is collateral)", side: doraclient.SIDE_SELL, current: positionShort, want: usd},
		// Closes look at the bond.
		{name: "long + sell → bond (close long)", side: doraclient.SIDE_SELL, current: positionLong, want: bond},
		{name: "short + buy → bond (close short)", side: doraclient.SIDE_BUY, current: positionShort, want: bond},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := balanceAssetFor(tt.side, tt.current, bond, usd)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestPositionAccountIsGlobal(t *testing.T) {
	t.Parallel()

	assetA := uuid.New().String()
	assetB := uuid.New().String()

	yes := true
	no := false

	tests := []struct {
		name      string
		portfolio *doraclient.AccountPortfolioV2
		assetID   string
		wantIsG   bool
		wantFound bool
	}{
		{
			name: "long in global account → (true, true)",
			portfolio: &doraclient.AccountPortfolioV2{
				Accounts: map[string]map[string]doraclient.AccountV2{
					"global-A": {assetA: {AssetId: assetA, IsGlobal: &yes, Available: "1000"}},
				},
			},
			assetID:   assetA,
			wantIsG:   true,
			wantFound: true,
		},
		{
			name: "long in isolated account → (false, true)",
			portfolio: &doraclient.AccountPortfolioV2{
				Accounts: map[string]map[string]doraclient.AccountV2{
					"isolated-A": {assetA: {AssetId: assetA, IsGlobal: &no, Available: "250"}},
				},
			},
			assetID:   assetA,
			wantIsG:   false,
			wantFound: true,
		},
		{
			name: "short in isolated account (Borrowed > 0) → (false, true)",
			portfolio: &doraclient.AccountPortfolioV2{
				Accounts: map[string]map[string]doraclient.AccountV2{
					"isolated-A": {assetA: {AssetId: assetA, IsGlobal: &no, Available: "0", Borrowed: "300"}},
				},
			},
			assetID:   assetA,
			wantIsG:   false,
			wantFound: true,
		},
		{
			name: "global has it AND isolated has it → global wins (closes against global first)",
			portfolio: &doraclient.AccountPortfolioV2{
				Accounts: map[string]map[string]doraclient.AccountV2{
					"global-A":   {assetA: {AssetId: assetA, IsGlobal: &yes, Available: "100"}},
					"isolated-A": {assetA: {AssetId: assetA, IsGlobal: &no, Available: "200"}},
				},
			},
			assetID:   assetA,
			wantIsG:   true,
			wantFound: true,
		},
		{
			name: "no positions anywhere → (false, false)",
			portfolio: &doraclient.AccountPortfolioV2{
				Accounts: map[string]map[string]doraclient.AccountV2{
					"global-B": {assetB: {AssetId: assetB, IsGlobal: &yes, Available: "0"}},
				},
			},
			assetID:   assetA, // ask about an asset nothing has
			wantIsG:   false,
			wantFound: false,
		},
		{
			name:      "nil portfolio → (false, false)",
			portfolio: nil,
			assetID:   assetA,
			wantIsG:   false,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			isG, found := positionAccountIsGlobal(tt.portfolio, tt.assetID)
			require.Equal(t, tt.wantIsG, isG, "isGlobal")
			require.Equal(t, tt.wantFound, found, "found")
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
