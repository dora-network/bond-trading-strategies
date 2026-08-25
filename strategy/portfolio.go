package strategy

import (
	"fmt"
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

// findAccountAndBalance locates the correct account and extracts the
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
// Unexported because every external consumer goes through
// InitialBalancesFromPortfolio.
func findAccountAndBalance(
	accounts map[string]map[string]doraclient.AccountV2,
	fromGlobalPosition bool,
	baseAssetID string,
	quoteAssetID string,
) (AccountBalance, bool, error) {
	bal, ok, err := findBalancesInAccounts(accounts, fromGlobalPosition, baseAssetID, quoteAssetID)
	if err != nil {
		return AccountBalance{}, false, err
	}
	if ok {
		return bal, true, nil
	}
	if !fromGlobalPosition {
		bal, ok, err = findBalancesInAccounts(accounts, true, baseAssetID, quoteAssetID)
		if err != nil {
			return AccountBalance{}, false, err
		}
		if ok {
			return bal, true, nil
		}
	}
	return AccountBalance{}, false, nil
}

// findBalancesInAccounts walks the portfolio and extracts USD and
// bond balances from accounts matching the global/isolated filter.
// Returns (balance, found, err): err is non-nil if a decimal.Parse
// fails on a quote or base asset we encountered. Silently dropping
// a balance would leave the strategy undercapitalized without any
// signal to the caller. Unexported; same-package tests reach it
// via the lowercase name.
func findBalancesInAccounts(
	accounts map[string]map[string]doraclient.AccountV2,
	wantGlobal bool,
	baseAssetID string,
	quoteAssetID string,
) (AccountBalance, bool, error) {
	var bal AccountBalance
	found := false
	for _, assetPositions := range accounts {
		for assetID, acct := range assetPositions {
			if acct.GetIsGlobal() != wantGlobal {
				continue
			}
			switch assetID {
			case quoteAssetID:
				avail, err := decimal.Parse(acct.Available)
				if err != nil {
					return AccountBalance{}, false, fmt.Errorf("parse quote asset %s available %q: %w",
						assetID, acct.Available, err)
				}
				bal.USD = avail
				found = true
			case baseAssetID:
				avail, err := decimal.Parse(acct.Available)
				if err != nil {
					return AccountBalance{}, false, fmt.Errorf("parse base asset %s available %q: %w",
						assetID, acct.Available, err)
				}
				// Empty borrowed string means no debt (DORA returns '' when
				// borrowed is zero); surface non-empty Parse failures only.
				var borrowed decimal.Decimal
				if acct.Borrowed != "" {
					var bErr error
					borrowed, bErr = decimal.Parse(acct.Borrowed)
					if bErr != nil {
						return AccountBalance{}, false, fmt.Errorf("parse base asset %s borrowed %q: %w",
							assetID, acct.Borrowed, bErr)
					}
				}
				if !borrowed.IsZero() {
					bal.Bond = borrowed.Neg()
				} else {
					bal.Bond = avail
				}
				found = true
			}
		}
	}
	if !found || bal.isZero() {
		return AccountBalance{}, false, nil
	}
	return bal, true, nil
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
) (AccountBalance, types.Signal, bool, error) {
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		logger.Warn("initialise balances: no accounts in portfolio")
		return AccountBalance{}, types.SignalHold, false, nil
	}
	bal, ok, err := findAccountAndBalance(accounts, fromGlobalPosition, baseAssetID, quoteAssetID)
	if err != nil {
		return AccountBalance{}, types.SignalHold, false, fmt.Errorf("init balances from portfolio: %w", err)
	}
	if !ok {
		logger.Warn("initialise balances: no matching account found in portfolio, falling back to legacy path")
		return AccountBalance{}, types.SignalHold, false, nil
	}
	signal := signalFromBondQty(bal.Bond)
	// Info log intentionally omitted: every caller (momentum/balances.go,
	// meanreversion/balances.go) logs the same event with runID context.
	// Logging here produced a duplicate Info line per init. Adapters
	// also pass the structured logger and surface the same fields.
	return bal, signal, true, nil
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
