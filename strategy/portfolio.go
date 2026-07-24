package strategy

import (
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

// AccountBalance is the USD available balance and bond quantity
// extracted from one DORA portfolio account.
type AccountBalance struct {
	USD  decimal.Decimal
	Bond decimal.Decimal
}

// isZero reports whether the balance carries no information.
func (b AccountBalance) isZero() bool {
	return b.USD.IsZero() && b.Bond.IsZero()
}

// FindAccountAndBalance locates the correct account and extracts the
// available USD balance and the bond (base asset) position from it.
//
// Account selection (the fromGlobalPosition rule):
//   - fromGlobalPosition is true (leverage == 1x):  use the global account.
//   - fromGlobalPosition is false (leverage > 1x): use the isolated account
//     whose asset ID matches the base asset. If no isolated account
//     exists yet (because no leveraged position has been opened), fall
//     back to the global account — the isolated account will be
//     created by the first order.
//
// Balance selection per side (the close/open rule):
//   - Buys always size off usdAvailable — the cash to spend.
//   - Sells cap at the current bond position, so the same account
//     also returns the bond's net position. This mirrors the
//     copytrading strategy's rule: closes look at the traded bond;
//     opens/extends look at USD.
//
// Returns the balance and ok=true when a matching account was found;
// ok=false means the caller should fall back to legacy AssetPosition.
func FindAccountAndBalance(
	accounts map[string]map[string]doraclient.AccountV2,
	fromGlobalPosition bool,
	baseAssetID string,
	quoteAssetID string,
) (AccountBalance, bool) {
	if bal, ok := FindBalancesInAccounts(accounts, fromGlobalPosition, baseAssetID, quoteAssetID); ok {
		return bal, true
	}
	if !fromGlobalPosition {
		if bal, ok := FindBalancesInAccounts(accounts, true, baseAssetID, quoteAssetID); ok {
			return bal, true
		}
	}
	return AccountBalance{}, false
}

// FindBalancesInAccounts walks the portfolio and extracts USD and
// bond balances from accounts matching the global/isolated filter.
func FindBalancesInAccounts(
	accounts map[string]map[string]doraclient.AccountV2,
	wantGlobal bool,
	baseAssetID string,
	quoteAssetID string,
) (AccountBalance, bool) {
	var bal AccountBalance
	for _, assetPositions := range accounts {
		for assetID, acct := range assetPositions {
			if acct.GetIsGlobal() != wantGlobal {
				continue
			}
			if assetID == quoteAssetID {
				if avail, err := decimal.Parse(acct.Available); err == nil {
					bal.USD = avail
				}
			}
			if assetID == baseAssetID {
				avail, err := decimal.Parse(acct.Available)
				borrowed, bErr := decimal.Parse(acct.Borrowed)
				if err == nil && bErr == nil {
					if !borrowed.IsZero() {
						bal.Bond = borrowed.Neg()
					} else {
						bal.Bond = avail
					}
				}
			}
		}
	}
	if bal.isZero() {
		return AccountBalance{}, false
	}
	return bal, true
}

// InitialBalancesFromPortfolio extracts the initial USD and bond
// balance from the V2 portfolio API for the requested account type.
// It returns the balance, the open-position signal reconstructed from
// the bond quantity, and ok=false if the portfolio has no matching
// account (in which case the caller should fall back to the legacy
// AssetPosition path).
func InitialBalancesFromPortfolio(
	portfolio *doraclient.AccountPortfolioV2,
	fromGlobalPosition bool,
	baseAssetID string,
	quoteAssetID string,
	logger *slog.Logger,
) (AccountBalance, types.Signal, bool) {
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		logger.Warn("initialise balances: no accounts in portfolio")
		return AccountBalance{}, types.SignalHold, false
	}
	bal, ok := FindAccountAndBalance(accounts, fromGlobalPosition, baseAssetID, quoteAssetID)
	if !ok {
		logger.Warn("initialise balances: no matching account found in portfolio, falling back to legacy path")
		return AccountBalance{}, types.SignalHold, false
	}
	signal := signalFromBondQty(bal.Bond)
	logger.Info("initialised balances from portfolio",
		"fromGlobalPosition", fromGlobalPosition,
		"usdBal", bal.USD, "bondQty", bal.Bond,
	)
	return bal, signal, true
}

// signalFromBondQty reconstructs the open-position signal direction
// from the fetched bond quantity. Zero quantity → flat (Hold).
func signalFromBondQty(bondQty decimal.Decimal) types.Signal {
	switch {
	case bondQty.IsPos():
		return types.SignalBuy
	case bondQty.IsNeg():
		return types.SignalSell
	default:
		return types.SignalHold
	}
}
