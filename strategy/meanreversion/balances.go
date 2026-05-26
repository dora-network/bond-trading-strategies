package meanreversion

import (
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/govalues/decimal"
)

// findAccountAndBalance locates the correct account and extracts the available USD
// balance based on the fromGlobalPosition rule:
//
//	If fromGlobalPosition is true (leverage == 1x):  use the global account.
//	If fromGlobalPosition is false (leverage > 1x): use the isolated account
//	  whose asset ID matches the base asset. If no isolated account exists yet
//	  (because no leveraged position has been opened), fall back to the global
//	  account — the isolated account will be created by the first order.
//
// It also returns the bond (base asset) position from the same account.
func findAccountAndBalance(
	accounts map[string]map[string]doraclient.AccountV2,
	fromGlobalPosition bool,
	baseAssetID string,
	quoteAssetID string,
) (usdAvailable decimal.Decimal, bondQty decimal.Decimal, ok bool) {
	// First pass: try the desired account type (isolated or global).
	usdAvailable, bondQty, found := findBalancesInAccounts(accounts, fromGlobalPosition, baseAssetID, quoteAssetID)
	if found {
		return usdAvailable, bondQty, true
	}

	// If the desired account type wasn't found (e.g., no isolated account exists yet
	// because no leveraged order has been placed), fall back to the global account.
	// The isolated account will be created by the DORA platform when the first
	// leveraged order is placed.
	if !fromGlobalPosition {
		usdAvailable, bondQty, found = findBalancesInAccounts(accounts, true, baseAssetID, quoteAssetID)
		if found {
			return usdAvailable, bondQty, true
		}
	}

	return decimal.Zero, decimal.Zero, false
}

// findBalancesInAccounts walks the portfolio and extracts USD available balance
// and bond quantity from accounts matching the global/isolated filter.
func findBalancesInAccounts(
	accounts map[string]map[string]doraclient.AccountV2,
	wantGlobal bool,
	baseAssetID string,
	quoteAssetID string,
) (usdAvailable decimal.Decimal, bondQty decimal.Decimal, found bool) {
	for _, assetPositions := range accounts {
		for assetID, acct := range assetPositions {
			if acct.GetIsGlobal() != wantGlobal {
				continue
			}
			if assetID == quoteAssetID {
				avail, err := decimal.Parse(acct.Available)
				if err == nil {
					usdAvailable = avail
				}
			}
			if assetID == baseAssetID {
				avail, err := decimal.Parse(acct.Available)
				borrowed, bErr := decimal.Parse(acct.Borrowed)
				if err == nil && bErr == nil {
					if !borrowed.IsZero() {
						bondQty = borrowed.Neg()
					} else {
						bondQty = avail
					}
				}
			}
		}
	}

	if !usdAvailable.IsZero() || !bondQty.IsZero() {
		return usdAvailable, bondQty, true
	}
	return decimal.Zero, decimal.Zero, false
}

// initializeBalancesFromPortfolio uses the V2 portfolio API to set the
// initial balance and bond position from the correct account based on
// the fromGlobalPosition rule.
func initializeBalancesFromPortfolio(
	s *Strategy,
	portfolio *doraclient.AccountPortfolioV2,
	baseAssetID string,
	quoteAssetID string,
	fromGlobalPosition bool,
	logger *slog.Logger,
) {
	accounts := portfolio.GetAccounts()
	if len(accounts) == 0 {
		logger.Warn("initialise balances: no accounts in portfolio")
		return
	}

	usdAvailable, bondQty, ok := findAccountAndBalance(accounts, fromGlobalPosition, baseAssetID, quoteAssetID)
	if !ok {
		logger.Warn("initialise balances: no matching account found in portfolio, falling back to legacy path")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.usdBal = usdAvailable
	s.bondQty = bondQty
	if !usdAvailable.IsZero() {
		s.cfg.InitialBalance = usdAvailable
	}

	// Reconstruct the open-position signal from the fetched bond quantity.
	switch {
	case s.bondQty.IsPos():
		s.openSignal = types.SignalBuy
	case s.bondQty.IsNeg():
		s.openSignal = types.SignalSell
	default:
		s.openSignal = types.SignalHold
	}

	logger.Info("initialised balances from portfolio",
		"runID", s.runID,
		"fromGlobalPosition", fromGlobalPosition,
		"usdBal", s.usdBal,
		"bondQty", s.bondQty,
		"initialBalance", s.cfg.InitialBalance,
	)
}
