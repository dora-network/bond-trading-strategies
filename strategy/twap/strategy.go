package twap

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/exec"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
)

// StrategyType is the constant strategy name used in decisions and persistence.
const StrategyType = "twap"

// Strategy is the TWAP trading strategy for a single order book.
// Shared I/O and state-update logic lives in exec.Executor; this
// strategy contains only the TWAP-specific run loop and rebalance math.
type Strategy struct {
	cfg          Config
	log          *slog.Logger
	exec         *exec.Executor
	runID        uuid.UUID
	mu           sync.RWMutex
	paused       bool
	cancel       context.CancelFunc
	orderUpdates <-chan OrderFillEvent
	runState     RunState
	decSeq       int64
}

func New(cfg Config, log *slog.Logger, opts ...func(*Strategy)) *Strategy {
	s := &Strategy{
		cfg: cfg,
		log: log,
		exec: &exec.Executor{
			Name: StrategyType,
			Log:  log,
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithMarketAPIClient(client strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.exec.Market = client }
}

func WithDecisionStore(rec strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.exec.Records = rec }
}

// WithStateStore injects the per-run state checkpoint store.
func WithStateStore(store strategy.StateStore) func(*Strategy) {
	return func(s *Strategy) { s.exec.Store = store }
}

// WithOrderUpdates injects a channel of order fill events from the
// DORA order update stream.
func WithOrderUpdates(ch <-chan OrderFillEvent) func(*Strategy) {
	return func(s *Strategy) { s.orderUpdates = ch }
}

// SetOrderUpdatesChannel lets the handler inject the channel after
// construction.
func (s *Strategy) SetOrderUpdatesChannel(ch <-chan OrderFillEvent) {
	s.mu.Lock()
	s.orderUpdates = ch
	s.mu.Unlock()
}

// SetDecisionSeq seeds the in-memory decision sequence counter.
func (s *Strategy) SetDecisionSeq(seq int64) {
	s.mu.Lock()
	s.decSeq = seq
	s.mu.Unlock()
}

// Backtest returns a no-op result since TWAP is execution-only.
func (s *Strategy) Backtest(_ context.Context, _ time.Time, _ time.Time) (types.BacktestResult, error) {
	return types.ErrorResult{}, nil
}

// Run starts the TWAP strategy in the background.
func (s *Strategy) Run(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
	s.mu.Lock()
	s.runID = runID
	s.exec.RunID = runID
	var runCtx context.Context
	runCtx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()
	return s.run(runCtx, msgCh)
}

// loadState delegates to the shared Executor.
func (s *Strategy) loadState(ctx context.Context) {
	raw, err := s.exec.LoadState(ctx)
	if err != nil {
		s.log.Error("twap: load run state, starting fresh", "err", err, "runID", s.runID)
		return
	}
	if len(raw) == 0 {
		return
	}
	state, err := UnmarshalState(raw)
	if err != nil {
		s.log.Error("twap: unmarshal run state, starting fresh", "err", err, "runID", s.runID)
		return
	}
	s.mu.Lock()
	s.runState = state
	s.mu.Unlock()
}

// reconcilePendingOrders delegates to the shared Executor.
func (s *Strategy) reconcilePendingOrders(ctx context.Context) {
	s.mu.RLock()
	stateCopy := s.runState
	s.mu.RUnlock()
	s.exec.ReconcilePendingOrders(ctx, &stateCopy, func(ctx context.Context, orderID string) (string, decimal.Decimal, error) {
		return s.exec.Market.GetOrderFilledStatus(ctx, orderID)
	})
	s.mu.Lock()
	s.runState = stateCopy
	s.mu.Unlock()
}

// processOrderUpdate delegates to the shared Executor.
func (s *Strategy) processOrderUpdate(ctx context.Context, evt OrderFillEvent) {
	s.mu.RLock()
	stateCopy := s.runState
	s.mu.RUnlock()
	s.exec.ProcessOrderUpdate(ctx, &stateCopy, evt)
	s.mu.Lock()
	s.runState = stateCopy
	s.mu.Unlock()
}

// saveState delegates to the shared Executor.
func (s *Strategy) saveState(ctx context.Context) {
	s.mu.RLock()
	stateCopy := s.runState
	s.mu.RUnlock()
	s.exec.SaveState(ctx, &stateCopy)
	s.mu.Lock()
	s.runState = stateCopy
	s.mu.Unlock()
}

// run initialises the run, then drives the dispatch loop until
// completion or context cancellation.
func (s *Strategy) run(ctx context.Context, msgCh <-chan strategy.Message) error {
	s.loadState(ctx)
	s.reconcilePendingOrders(ctx)

	numChunks := s.cfg.NumChunks()
	if numChunks == 0 {
		return fmt.Errorf("twap: no chunks in execution window")
	}

	divisor := decimal.MustNew(int64(numChunks), 0)
	baseChunkSize, err := s.cfg.TotalAmount.Quo(divisor)
	if err != nil {
		return fmt.Errorf("twap: calculate chunk size: %w", err)
	}

	interval := time.Duration(s.cfg.IntervalSeconds) * time.Second

	signal := types.SignalBuy
	if s.cfg.Side == "sell" {
		signal = types.SignalSell
	}
	assetID := s.cfg.OrderBookID

	s.mu.RLock()
	totalSubmitted := s.runState.TotalSubmitted
	chunksProcessed := s.runState.ChunksProcessed
	s.mu.RUnlock()

	s.log.Info(
		"starting TWAP run",
		"runID", s.runID,
		"orderBookID", s.cfg.OrderBookID,
		"totalAmount", s.cfg.TotalAmount,
		"numChunks", numChunks,
		"interval", interval,
		"side", s.cfg.Side,
		"executedAmount", totalSubmitted,
		"chunksProcessed", chunksProcessed,
		"orders", len(s.runState.Orders),
	)

	scheduledTime := func(i int) time.Time {
		return s.cfg.StartTime.Add(time.Duration(i) * interval)
	}

	// Skip bucket slots whose scheduled time has already passed
	// (missed during downtime, restart, or pause).
	now := time.Now().UTC()
	for chunksProcessed < numChunks && !now.Before(scheduledTime(chunksProcessed)) {
		s.mu.Lock()
		s.runState.ChunksProcessed = chunksProcessed + 1
		s.mu.Unlock()
		s.saveState(ctx)
		s.log.Info("twap: skipping missed chunk", "runID", s.runID, "chunk", chunksProcessed)
		chunksProcessed++
	}
	if chunksProcessed >= numChunks {
		return nil
	}

	return s.dispatchLoop(ctx, msgCh, numChunks, chunksProcessed, scheduledTime, baseChunkSize, assetID, signal)
}

// dispatchLoop drives the timer + select until completion.
func (s *Strategy) dispatchLoop(
	ctx context.Context,
	msgCh <-chan strategy.Message,
	numChunks, chunksProcessed int,
	scheduledTime func(int) time.Time,
	baseChunkSize decimal.Decimal,
	assetID string,
	signal types.Signal,
) error {
	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		wait := time.Until(scheduledTime(chunksProcessed))
		if wait < 0 {
			wait = 0
		}
		timer = time.NewTimer(wait)
	}
	resetTimer()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

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
		case evt, ok := <-s.orderUpdates:
			if !ok {
				s.log.Warn("twap: order updates channel closed", "runID", s.runID)
				s.orderUpdates = nil
				continue
			}
			s.processOrderUpdate(ctx, evt)
		case <-timer.C:
			if time.Now().UTC().After(s.cfg.EndTime) {
				s.log.Info("twap: execution window ended", "runID", s.runID)
				return nil
			}
			s.mu.RLock()
			paused := s.paused
			s.mu.RUnlock()
			if paused {
				s.log.Info("twap: skipping chunk (paused)", "runID", s.runID, "chunk", chunksProcessed)
			} else {
				s.mu.RLock()
				ts := s.runState.TotalSubmitted
				cp := chunksProcessed
				s.mu.RUnlock()
				chunkSize, cErr := s.computeChunkSize(numChunks, ts, cp)
				if cErr != nil {
					s.log.Error("twap: compute chunk size", "err", cErr, "runID", s.runID)
					chunkSize = baseChunkSize
				}
				s.placeChunk(ctx, chunksProcessed, assetID, signal, chunkSize)
			}
			chunksProcessed++
			s.mu.Lock()
			s.runState.ChunksProcessed = chunksProcessed
			s.mu.Unlock()
			s.saveState(ctx)
			if chunksProcessed >= numChunks {
				return nil
			}
			resetTimer()
		}
	}
}

// computeChunkSize returns the rebalanced chunk size for the next
// chunk: (totalAmount - totalSubmitted) / (numChunks - chunksProcessed).
func (s *Strategy) computeChunkSize(numChunks int, totalSubmitted decimal.Decimal, chunksProcessed int) (decimal.Decimal, error) {
	remaining := numChunks - chunksProcessed
	if remaining <= 0 {
		return decimal.Zero, nil
	}
	delta, err := s.cfg.TotalAmount.Sub(totalSubmitted)
	if err != nil {
		return decimal.Zero, err
	}
	if !delta.IsPos() {
		delta = decimal.Zero
	}
	divisor := decimal.MustNew(int64(remaining), 0)
	return delta.Quo(divisor)
}

// placeChunk places a single chunk order via the shared Executor and
// records the decision with the TWAP-specific Decision struct.
func (s *Strategy) placeChunk(ctx context.Context, chunkIdx int, assetID string, signal types.Signal, chunkSize decimal.Decimal) {
	s.mu.RLock()
	stateCopy := s.runState
	s.mu.RUnlock()

	orderID, clientOrderID, err := s.exec.PlaceOrder(ctx, &stateCopy, assetID, signal, chunkSize)
	if err != nil {
		s.log.Error("twap: create market order failed", "runID", s.runID, "chunk", chunkIdx, "err", err)
		return
	}

	s.mu.Lock()
	s.runState = stateCopy
	s.decSeq++
	seq := s.decSeq
	s.mu.Unlock()

	s.exec.RecordDecision(ctx, signal, assetID, chunkSize, clientOrderID, time.Now().UTC(), seq)
	s.saveState(ctx)

	s.log.Info(
		"twap: chunk order placed",
		"runID", s.runID,
		"chunk", chunkIdx,
		"client_order_id", clientOrderID,
		"order_id", orderID,
		"chunk_size", chunkSize,
	)
}

// ForTesting exposes internal executor wiring for the cross-package
// handler resume test (strategy/http/handler_resume_test.go). Not
// intended for production callers — names are stable enough for
// tests but not part of the strategy's public API.

// MarketClientWired reports whether the strategy's executor has a
// non-nil Market client (truthy proof of the resume path's
// decrypt/API-key switch).
func (s *Strategy) MarketClientWired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exec.Market != nil
}

// StateStoreWired reports whether the strategy's executor has a
// non-nil state Store (truthy proof of attachStateStore's
// vwap/twap wiring).
func (s *Strategy) StateStoreWired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exec.Store != nil
}

// DecisionSeqForTest returns the in-memory decision sequence counter.
// resumePersistedRun's SetDecisionSeq switch must seed this from
// MaxSeq so the first post-restart decision doesn't collide on
// strategy_decisions.
func (s *Strategy) DecisionSeqForTest() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.decSeq
}

// RunStateForTest returns the in-memory state counters after
// loadState has run.
func (s *Strategy) RunStateForTest() (decimal.Decimal, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runState.TotalSubmitted, s.runState.ChunksProcessed
}

// LoadStateForTest invokes the strategy's loadState path. Exposed
// so the resume test can drive the load and then read back the
// in-memory state via RunStateForTest.
func (s *Strategy) LoadStateForTest(ctx context.Context) { s.loadState(ctx) }

// RunIDForTest sets the strategy's run id and executor RunID. The
// production Run method does the same — this lets a test drive
// loadState without going through Run.
func (s *Strategy) RunIDForTest(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runID = id
	s.exec.RunID = id
}
