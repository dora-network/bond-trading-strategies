package momentum

import (
	"fmt"
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/dora-client-go/doraclient"
)

// initializeBalancesFromPortfolio is the momentum adapter for the
// shared strategy.InitialBalancesFromPortfolio helper.
func initializeBalancesFromPortfolio(
	s *Strategy,
	portfolio *doraclient.AccountPortfolioV2,
	baseAssetID string,
	quoteAssetID string,
	fromGlobalPosition bool,
	logger *slog.Logger,
) {
	bal, signal, ok, err := strategy.InitialBalancesFromPortfolio(portfolio, fromGlobalPosition, baseAssetID, quoteAssetID, logger)
	if err != nil {
		s.recordErr(fmt.Errorf("initialise balances: %w", err))
	}
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usdBal = bal.USD
	s.bondQty = bal.Bond
	if !bal.USD.IsZero() {
		s.cfg.InitialBalance = bal.USD
	}
	s.openSignal = signal
	logger.Info("initialised balances from portfolio",
		"runID", s.runID,
		"fromGlobalPosition", fromGlobalPosition,
		"usdBal", s.usdBal,
		"bondQty", s.bondQty,
		"initialBalance", s.cfg.InitialBalance,
	)
}
