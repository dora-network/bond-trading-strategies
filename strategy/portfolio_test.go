package strategy

import (
	"testing"

	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
)

// findBalancesInAccounts filter accepts it.
func accountWith(available, borrowed string) doraclient.AccountV2 {
	isGlobal := true
	return doraclient.AccountV2{
		IsGlobal:  &isGlobal,
		Available: available,
		Borrowed:  borrowed,
	}
}

// TestFindBalancesInAccounts_HappyPath verifies the contract: a
// well-formed asset quote/base pair returns the populated balance and
// err=nil.
func TestFindBalancesInAccounts_HappyPath(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("10", ""),
			quoteID: accountWith("1000", ""),
		},
	}
	bal, ok, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, bal.USD.Equal(decimal.MustNew(1000, 0)))
	assert.True(t, bal.Bond.Equal(decimal.MustNew(10, 0)))
}

// TestFindBalancesInAccounts_NonNumericAvailableReturnsError verifies
// the contract-violation path: an upstream balance string that
// doesn't parse surfaces an error rather than silently dropping the
// asset (which would leave the strategy undercapitalized).
func TestFindBalancesInAccounts_NonNumericAvailableReturnsError(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("not-a-number", ""),
		},
	}
	_, _, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.Error(t, err)
	require.ErrorContains(t, err, "parse base asset")
	require.ErrorContains(t, err, "not-a-number")
}

// TestFindBalancesInAccounts_NonNumericBorrowedReturnsError is the
// mirror test for the borrowed field.
func TestFindBalancesInAccounts_NonNumericBorrowedReturnsError(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("10", "still-not-a-number"),
		},
	}
	_, _, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.Error(t, err)
	require.ErrorContains(t, err, "borrowed")
}

// TestFindBalancesInAccounts_NoMatchingAssetReturnsOkFalse verifies
// the missing-account path: no error, just ok=false (so the caller
// can fall back to legacy AssetPosition).
func TestFindBalancesInAccounts_NoMatchingAssetReturnsOkFalse(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			"unrelated-asset": accountWith("10", ""),
		},
	}
	bal, ok, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.True(t, bal.isZero())
}

// TestFindAccountAndBalance_PropagatesError verifies the caller-side
// error path: a Parse failure bubbles up so initializeBalancesFromPortfolio
// can record it (mirrors Slice H's recordErr pattern).
func TestFindAccountAndBalance_PropagatesError(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("garbage", ""),
		},
	}
	_, _, err := findAccountAndBalance(accounts, true, assetID, quoteID)
	require.Error(t, err)
	require.ErrorContains(t, err, "parse base asset")
}

// TestFindAccountAndBalance_IsolatedFallsBackToGlobal pins the
// leverage>1x path: when fromGlobalPosition=false and no isolated
// account exists for the base asset, the function falls back to the
// global account. Without this branch the strategy would return
// ok=false and silently use zero capital for a leveraged run.
func TestFindAccountAndBalance_IsolatedFallsBackToGlobal(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("10", ""),
			quoteID: accountWith("2000", ""),
		},
		// No isolated account for asset-A exists yet (no leveraged
		// position opened) — fall back to global.
	}
	bal, ok, err := findAccountAndBalance(accounts, false, assetID, quoteID)
	require.NoError(t, err)
	require.True(t, ok, "isolated->global fallback must succeed when global has the assets")
	assert.True(t, bal.USD.Equal(decimal.MustNew(2000, 0)))
	assert.True(t, bal.Bond.Equal(decimal.MustNew(10, 0)))
}

// TestFindBalancesInAccounts_ShortPositionFromBorrowed pins the
// money-path reconstruction: when Borrowed > 0, the position is a
// short and bal.Bond must equal borrowed.Neg() (not borrowed). A
// sign-flip here would double-count leverage on every leveraged
// entry and silently corrupt every PnL downstream.
func TestFindBalancesInAccounts_ShortPositionFromBorrowed(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("0", "5"),
			quoteID: accountWith("1000", ""),
		},
	}
	bal, ok, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.True(t, bal.Bond.IsNeg(),
		"bond quantity must invert sign for short positions; got %s", bal.Bond)
	assert.True(t, bal.Bond.Equal(decimal.MustNew(-5, 0)),
		"short position must equal -borrowed; got %s", bal.Bond)
}

// TestFindBalancesInAccounts_QuoteAssetParseError pins the
// quote-asset parse-error branch: non-numeric USD balance surfaces
// an error mentioning "quote asset" rather than silently dropping
// the USD leg (which would leave the strategy with no spending
// budget and never open).
func TestFindBalancesInAccounts_QuoteAssetParseError(t *testing.T) {
	const assetID, quoteID = "asset-A", "USDC"
	accounts := map[string]map[string]doraclient.AccountV2{
		"global": {
			assetID: accountWith("10", ""),
			quoteID: accountWith("not-a-number", ""),
		},
	}
	_, _, err := findBalancesInAccounts(accounts, true, assetID, quoteID)
	require.Error(t, err)
	require.ErrorContains(t, err, "parse quote asset")
}

// TestSignalFromBondQty pins the open-position signal reconstruction.
// Pre-fix this was an unexported helper with no test; a sign-flip
// would flip every reconstructed openSignal downstream.
func TestSignalFromBondQty(t *testing.T) {
	cases := []struct {
		name string
		qty  decimal.Decimal
		want types.Signal
	}{
		{"zero -> Hold", decimal.Zero, types.SignalHold},
		{"positive -> Buy", decimal.MustNew(5, 0), types.SignalBuy},
		{"negative -> Sell", decimal.MustNew(-5, 0), types.SignalSell},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := signalFromBondQty(tc.qty)
			assert.Equal(t, tc.want, got)
		})
	}
}
