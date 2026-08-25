package strategy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/govalues/decimal"

	"github.com/dora-network/dora-client-go/doraclient"
)

// accountWith returns a V2 account with IsGlobal set true so the
// FindBalancesInAccounts filter accepts it.
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
	bal, ok, err := FindBalancesInAccounts(accounts, true, assetID, quoteID)
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
	_, _, err := FindBalancesInAccounts(accounts, true, assetID, quoteID)
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
	_, _, err := FindBalancesInAccounts(accounts, true, assetID, quoteID)
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
	bal, ok, err := FindBalancesInAccounts(accounts, true, assetID, quoteID)
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
	_, _, err := FindAccountAndBalance(accounts, true, assetID, quoteID)
	require.Error(t, err)
	require.ErrorContains(t, err, "parse base asset")
}
