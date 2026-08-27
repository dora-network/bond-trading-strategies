package momentum

import (
	"fmt"
	"log/slog"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/dora-client-go/doraclient"
)

// initializeBalancesFromPortfolio is the momentum adapter for the
// shared strategy.InitialBalancesFromPortfolio helper. Returns true
// when the helper converged and tracked state was applied; false means
// the caller should fall back to the legacy AssetPosition path.
func initializeBalancesFromPortfolio(
	s *Strategy,
	portfolio *doraclient.AccountPortfolioV2,
	baseAssetID string,
	quoteAssetID string,
	fromGlobalPosition bool,
	logger *slog.Logger,
) bool {
	bal, signal, ok, err := strategy.InitialBalancesFromPortfolio(portfolio, fromGlobalPosition, baseAssetID, quoteAssetID, logger)
	if err != nil {
		logger.Error("initialise balances from portfolio", "runID", s.runID, "err", err)
		s.recordErr(fmt.Errorf("initialise balances: %w", err))
	}
	if !ok {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usdBal = bal.USD
	s.bondQty = bal.Bond
	// Always sync, including zero: an unfunded account must not keep
	// sizing orders off the config default (synthetic capital).
	s.cfg.InitialBalance = bal.USD
	s.openSignal = signal
	logger.Info(
		"initialised balances from portfolio",
		"runID", s.runID,
		"fromGlobalPosition", fromGlobalPosition,
		"usdBal", s.usdBal,
		"bondQty", s.bondQty,
		"initialBalance", s.cfg.InitialBalance,
	)
	return true
}
