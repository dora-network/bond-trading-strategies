package twap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

// StrategyType is the constant strategy name used in decisions and persistence.
const StrategyType = "twap"

// Strategy is the TWAP trading strategy for a single order book.
type Strategy struct {
	cfg             Config
	log             *slog.Logger
	runID           uuid.UUID
	mu              sync.RWMutex
	paused          bool
	cancel          context.CancelFunc
	marketAPIClient strategy.MarketAPIClient
	decisionStore   strategy.DecisionRecorder
	stateStore      strategy.StateStore
	decisionSeq     int64
}

// New creates a new TWAP strategy.
func New(cfg Config, log *slog.Logger, opts ...func(*Strategy)) *Strategy {
	s := &Strategy{
		cfg: cfg,
		log: log,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithMarketAPIClient injects the DORA API client.
func WithMarketAPIClient(client strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.marketAPIClient = client }
}

// WithDecisionStore injects the decision recorder.
func WithDecisionStore(rec strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.decisionStore = rec }
}

// WithStateStore injects the per-run state checkpoint store. Used to
// persist and recover TWAP execution progress across server restarts.
func WithStateStore(store strategy.StateStore) func(*Strategy) {
	return func(s *Strategy) { s.stateStore = store }
}

// Backtest returns a no-op result since TWAP is execution-only.
func (s *Strategy) Backtest(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
	return types.ErrorResult{}, nil
}

// Run starts the TWAP strategy in the background.
func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	s.mu.Lock()
	s.runID = runID
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	return s.run(runCtx, msgCh)
}

func (s *Strategy) run(ctx context.Context, msgCh <-chan strategy.Message) error {
	// Calculate chunk size and interval.
	numChunks := s.cfg.NumChunks()
	if numChunks == 0 {
		return fmt.Errorf("twap: no chunks in execution window")
	}
	divisor := decimal.MustNew(int64(numChunks), 0)
	chunkSize, err := s.cfg.TotalAmount.Quo(divisor)
	if err != nil {
		return fmt.Errorf("twap: calculate chunk size: %w", err)
	}

	// Calculate interval.
	duration := s.cfg.EndTime.Sub(s.cfg.StartTime)
	interval := duration / time.Duration(numChunks)
	execTicker := time.NewTicker(interval)
	defer execTicker.Stop()

	// Determine signal.
	signal := types.SignalBuy
	if s.cfg.Side == "sell" {
		signal = types.SignalSell
	}

	// Determine asset ID from order book ID.
	assetID := s.cfg.OrderBookID

	s.log.Info(
		"starting TWAP run",
		"runID", s.runID,
		"orderBookID", s.cfg.OrderBookID,
		"totalAmount", s.cfg.TotalAmount,
		"numChunks", numChunks,
		"chunkSize", chunkSize,
		"interval", interval,
		"side", s.cfg.Side,
	)

	started := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgCh:
			s.mu.Lock()
			switch msg {
			case strategy.Pause:
				s.paused = true
			case strategy.Resume:
				s.paused = false
			case strategy.Stop:
				if s.cancel != nil {
					s.cancel()
				}
			}
			s.mu.Unlock()
		case <-execTicker.C:
			s.mu.RLock()
			if s.paused {
				s.mu.RUnlock()
				continue
			}
			s.mu.RUnlock()

			if !started {
				// Wait until we're at or past start time.
				now := time.Now().UTC()
				if now.Before(s.cfg.StartTime) {
					// Reset ticker to fire at start time.
					execTicker.Reset(s.cfg.StartTime.Sub(now))
					continue
				}
				started = true
			}

			// Stop if the execution window has elapsed — do not place
			// orders past EndTime.
			if time.Now().UTC().After(s.cfg.EndTime) {
				s.log.Info("twap: execution window ended", "runID", s.runID)
				return nil
			}

			// Place order for this chunk.
			decision := NewDecision(time.Now().UTC(), assetID, signal, chunkSize)
			clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
			if err := s.executeDecision(ctx, decision, clientOrderID); err != nil {
				s.log.Error(
					"twap: failed to execute decision",
					"runID", s.runID,
					"err", err,
				)
			}
		}
	}
}

func (s *Strategy) executeDecision(ctx context.Context, decision Decision, clientOrderID string) error {
	quantity := decision.position
	if quantity.IsZero() {
		return nil
	}

	side := doraclient.SIDE_BUY
	if decision.signal == types.SignalSell {
		side = doraclient.SIDE_SELL
	}

	// TWAP doesn't use leverage — pass 1.0 as inverse leverage.
	inverseLeverage := decimal.One

	_, err := s.marketAPIClient.CreateMarketOrder(
		ctx,
		decision.bondID,
		side,
		quantity,
		inverseLeverage,
		false, // fromGlobalPosition
		clientOrderID,
	)
	if err != nil {
		return fmt.Errorf("create market order: %w", err)
	}

	s.log.Info(
		"twap: market order placed",
		"runID", s.runID,
		"quantity", quantity,
		"side", side,
		"clientOrderID", clientOrderID,
	)

	// Record the decision.
	s.recordDecision(ctx, decision, clientOrderID)

	return nil
}

func (s *Strategy) recordDecision(ctx context.Context, decision Decision, clientOrderID string) {
	if s.decisionStore == nil {
		return
	}
	s.mu.Lock()
	s.decisionSeq++
	seq := s.decisionSeq
	runID := s.runID
	s.mu.Unlock()

	d := strategy.Decision{
		RunID:         runID,
		Seq:           seq,
		StrategyType:  StrategyType,
		OrderBookID:   mustParseUUID(decision.bondID),
		Asset:         mustParseUUID(decision.bondID),
		Side:          decision.signal.String(),
		Signal:        decision.signal.String(),
		Quantity:      decision.position,
		Price:         decimal.Zero,
		Kind:          strategy.DecisionKindOpen,
		Reason:        "twap_execution",
		CreatedAt:     decision.time,
		ClientOrderID: clientOrderID,
	}

	if err := s.decisionStore.SaveDecision(ctx, d); err != nil {
		s.log.Error(
			"save strategy decision",
			"err", err,
			"run_id", d.RunID,
			"seq", d.Seq,
			"side", d.Side,
			"signal", d.Signal,
			"quantity", d.Quantity,
		)
	}
}

func mustParseUUID(s string) uuid.UUID {
	u, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return u
}

// SetDecisionSeq sets the in-memory decision sequence counter to the
// value returned by the decision store's MaxSeq. This ensures a resumed
// run doesn't reuse sequence numbers that are already persisted.
func (s *Strategy) SetDecisionSeq(seq int64) {
	s.mu.Lock()
	s.decisionSeq = seq
	s.mu.Unlock()
}
