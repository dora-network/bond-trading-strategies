package vwap

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
const StrategyType = "vwap"

// Strategy is the VWAP execution strategy for a single order book.
// Shared I/O and state-update logic lives in exec.Executor; this
// strategy contains only the VWAP-specific run loop, schedule, and
// bucket-size math.
type Strategy struct {
	cfg      Config
	log      *slog.Logger
	exec     *exec.Executor
	runID    uuid.UUID
	mu       sync.RWMutex
	paused   bool
	cancel   context.CancelFunc
	schedule Schedule
	store    TradeVolumeStore
	updates  <-chan OrderFillEvent
	state    RunState
	decSeq   int64
}

func New(cfg Config, log *slog.Logger, opts ...func(*Strategy)) *Strategy {
	s := &Strategy{
		cfg:  cfg,
		log:  log,
		exec: &exec.Executor{Name: StrategyType, Log: log},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func WithTradeHistoryStore(store TradeVolumeStore) func(*Strategy) {
	return func(s *Strategy) { s.store = store }
}

func WithMarketAPIClient(client strategy.MarketAPIClient) func(*Strategy) {
	return func(s *Strategy) { s.exec.Market = client }
}

func WithDecisionStore(rec strategy.DecisionRecorder) func(*Strategy) {
	return func(s *Strategy) { s.exec.Records = rec }
}

func WithStateStore(store strategy.StateStore) func(*Strategy) {
	return func(s *Strategy) { s.exec.Store = store }
}

func WithOrderUpdates(ch <-chan OrderFillEvent) func(*Strategy) {
	return func(s *Strategy) { s.updates = ch }
}

// SetOrderUpdatesChannel lets the handler inject the channel after
// construction but before Run is invoked.
func (s *Strategy) SetOrderUpdatesChannel(ch <-chan OrderFillEvent) {
	s.mu.Lock()
	s.updates = ch
	s.mu.Unlock()
}

// SetDecisionSeq seeds the in-memory decision sequence counter.
func (s *Strategy) SetDecisionSeq(seq int64) {
	s.mu.Lock()
	s.decSeq = seq
	s.mu.Unlock()
}

// Backtest returns a no-op result since VWAP is execution-only.
func (s *Strategy) Backtest(_ context.Context, _ time.Time, _ time.Time) (types.BacktestResult, error) {
	return types.ErrorResult{}, nil
}

// Run starts the strategy in the background.
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
		s.log.Error("vwap: load run state, starting fresh", "err", err, "runID", s.runID)
		return
	}
	if len(raw) == 0 {
		return
	}
	state, err := UnmarshalState(raw)
	if err != nil {
		s.log.Error("vwap: unmarshal run state, starting fresh", "err", err, "runID", s.runID)
		return
	}
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
}

// reconcilePendingOrders delegates to the shared Executor.
func (s *Strategy) reconcilePendingOrders(ctx context.Context) {
	s.mu.RLock()
	stateCopy := s.state
	s.mu.RUnlock()
	s.exec.ReconcilePendingOrders(ctx, &stateCopy, func(ctx context.Context, orderID string) (string, decimal.Decimal, error) {
		return s.exec.Market.GetOrderFilledStatus(ctx, orderID)
	})
	s.mu.Lock()
	s.state = stateCopy
	s.mu.Unlock()
}

// processOrderUpdate delegates to the shared Executor.
func (s *Strategy) processOrderUpdate(ctx context.Context, evt OrderFillEvent) {
	s.mu.RLock()
	stateCopy := s.state
	s.mu.RUnlock()
	s.exec.ProcessOrderUpdate(ctx, &stateCopy, evt)
	s.mu.Lock()
	s.state = stateCopy
	s.mu.Unlock()
}

// saveState delegates to the shared Executor.
func (s *Strategy) saveState(ctx context.Context) {
	s.mu.RLock()
	stateCopy := s.state
	s.mu.RUnlock()
	s.exec.SaveState(ctx, &stateCopy)
	s.mu.Lock()
	s.state = stateCopy
	s.mu.Unlock()
}

func (s *Strategy) run(ctx context.Context, msgCh <-chan strategy.Message) error {
	s.loadState(ctx)
	s.reconcilePendingOrders(ctx)

	if err := s.loadSchedule(ctx); err != nil {
		return fmt.Errorf("vwap: load schedule: %w", err)
	}

	numBuckets := len(s.schedule.Buckets)
	if numBuckets == 0 {
		return fmt.Errorf("vwap: empty schedule")
	}
	bucketSize := time.Duration(s.cfg.BucketMinutes) * time.Minute

	signal := types.SignalBuy
	if s.cfg.Side == "sell" {
		signal = types.SignalSell
	}
	assetID := s.cfg.OrderBookID

	s.mu.RLock()
	totalSubmitted := s.state.TotalSubmitted
	chunksProcessed := s.state.ChunksProcessed
	s.mu.RUnlock()

	if _, err := s.computeBucketSize(numBuckets, chunksProcessed, totalSubmitted, chunksProcessed); err != nil {
		return fmt.Errorf("vwap: validate bucket size formula: %w", err)
	}

	s.log.Info(
		"starting VWAP run",
		"runID", s.runID,
		"orderBookID", s.cfg.OrderBookID,
		"totalAmount", s.cfg.TotalAmount,
		"numBuckets", numBuckets,
		"bucketMinutes", s.cfg.BucketMinutes,
		"side", s.cfg.Side,
		"executedAmount", totalSubmitted,
		"chunksProcessed", chunksProcessed,
		"orders", len(s.state.Orders),
	)

	scheduledTime := func(i int) time.Time {
		return s.cfg.StartTime.Add(time.Duration(i) * bucketSize)
	}

	nextBucket := s.skipMissedBuckets(ctx, numBuckets, scheduledTime, chunksProcessed)
	if nextBucket >= numBuckets {
		return nil
	}

	return s.dispatchLoop(ctx, msgCh, numBuckets, nextBucket, scheduledTime, assetID, signal)
}

// dispatchLoop drives the bucket timer and select until completion.
func (s *Strategy) dispatchLoop(
	ctx context.Context,
	msgCh <-chan strategy.Message,
	numBuckets, nextBucket int,
	scheduledTime func(int) time.Time,
	assetID string,
	signal types.Signal,
) error {
	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		wait := time.Until(scheduledTime(nextBucket))
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

	for nextBucket < numBuckets {
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
		case evt, ok := <-s.updates:
			if !ok {
				s.log.Warn("vwap: order updates channel closed", "runID", s.runID)
				s.updates = nil
				continue
			}
			s.processOrderUpdate(ctx, evt)
		case <-timer.C:
			if !s.handleBucketTick(ctx, nextBucket, numBuckets, assetID, signal) {
				return nil
			}
			nextBucket++
			resetTimer()
		}
	}
	return nil
}

// computeBucketSize returns the rebalanced size for the next bucket.
// VWAP-specific: each bucket's size is its scheduled share of the
// remaining work, so high-ADV buckets stay larger than quiet ones:
//
//	size = schedule.Buckets[bucketIdx] * remainingQty / remainingSched
//
// This naturally absorbs in-flight and failed quantities into the
// remaining buckets while preserving the historical volume profile.
func (s *Strategy) computeBucketSize(
	numBuckets, bucketIdx int,
	totalSubmitted decimal.Decimal,
	chunksProcessed int,
) (decimal.Decimal, error) {
	if bucketIdx >= numBuckets || bucketIdx < chunksProcessed {
		return decimal.Zero, nil
	}
	remaining := numBuckets - chunksProcessed
	if remaining <= 0 {
		return decimal.Zero, nil
	}
	remainingSched := decimal.Zero
	for i := chunksProcessed; i < numBuckets; i++ {
		remainingSched, _ = remainingSched.Add(s.schedule.Buckets[i])
	}
	if remainingSched.IsZero() {
		return decimal.Zero, nil
	}
	remainingQty, err := s.cfg.TotalAmount.Sub(totalSubmitted)
	if err != nil {
		return decimal.Zero, err
	}
	if !remainingQty.IsPos() {
		return decimal.Zero, nil
	}
	bucketSched := s.schedule.Buckets[bucketIdx]
	numerator, err := bucketSched.Mul(remainingQty)
	if err != nil {
		return decimal.Zero, err
	}
	return numerator.Quo(remainingSched)
}

// handleBucketTick places the next bucket order (or skips it if
// paused), advances the bucket index, and persists state.
func (s *Strategy) handleBucketTick(
	ctx context.Context,
	bucketIdx, numBuckets int,
	assetID string,
	signal types.Signal,
) bool {
	s.mu.RLock()
	paused := s.paused
	ts := s.state.TotalSubmitted
	cp := s.state.ChunksProcessed
	s.mu.RUnlock()

	if paused {
		s.log.Info("vwap: skipping bucket (paused)", "runID", s.runID, "bucket", bucketIdx)
	} else {
		bucketSize, err := s.computeBucketSize(numBuckets, bucketIdx, ts, cp)
		if err != nil {
			s.log.Error("vwap: recalculate bucket size", "err", err)
		}
		s.placeBucket(ctx, bucketIdx, assetID, signal, bucketSize)
	}

	nextBucket := bucketIdx + 1
	s.mu.Lock()
	s.state.ChunksProcessed = nextBucket
	s.mu.Unlock()
	s.saveState(ctx)

	if nextBucket >= numBuckets {
		s.log.Info("vwap: all buckets placed", "runID", s.runID)
		return false
	}
	return true
}

// skipMissedBuckets consumes bucket slots whose scheduled time has
// already passed.
func (s *Strategy) skipMissedBuckets(
	ctx context.Context,
	numBuckets int,
	scheduledTime func(int) time.Time,
	chunksProcessed int,
) int {
	nextBucket := chunksProcessed
	now := time.Now().UTC()
	for nextBucket < numBuckets && !now.Before(scheduledTime(nextBucket)) {
		s.mu.Lock()
		s.state.ChunksProcessed = nextBucket + 1
		s.mu.Unlock()
		s.saveState(ctx)
		s.log.Info("vwap: skipping missed bucket", "runID", s.runID, "bucket", nextBucket)
		nextBucket++
	}
	if nextBucket >= numBuckets {
		s.log.Info("vwap: execution window fully elapsed", "runID", s.runID)
	}
	return nextBucket
}

// loadSchedule builds the VWAP schedule from historical trades.
// Logs a warning for any bucket that ends up with zero allocation
// (no historical ADV in that time slice) so operators can see why
// certain buckets were skipped.
func (s *Strategy) loadSchedule(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("vwap: trade history store not configured")
	}
	orderBookID, err := uuid.Parse(s.cfg.OrderBookID)
	if err != nil {
		return fmt.Errorf("vwap: parse order_book_id: %w", err)
	}
	sched, err := BuildSchedule(ctx, s.store, orderBookID, s.cfg)
	if err != nil {
		return err
	}
	for i, b := range sched.Buckets {
		if b.IsZero() {
			bucketStart := s.cfg.StartTime.Add(time.Duration(i) * time.Duration(s.cfg.BucketMinutes) * time.Minute)
			s.log.Warn(
				"vwap: bucket has zero allocation (no historical ADV)",
				"runID", s.runID,
				"bucket", i,
				"bucketStart", bucketStart.Format(time.RFC3339),
			)
		}
	}
	s.schedule = sched
	return nil
}

// placeBucket places a single bucket order via the shared Executor
// and records the decision.
func (s *Strategy) placeBucket(ctx context.Context, bucketIdx int, assetID string, signal types.Signal, bucketSize decimal.Decimal) {
	s.mu.RLock()
	stateCopy := s.state
	s.mu.RUnlock()

	orderID, clientOrderID, err := s.exec.PlaceOrder(ctx, &stateCopy, assetID, signal, bucketSize)
	if err != nil {
		s.log.Error("vwap: create market order failed", "runID", s.runID, "bucket", bucketIdx, "err", err)
		return
	}

	s.mu.Lock()
	s.state = stateCopy
	s.decSeq++
	seq := s.decSeq
	s.mu.Unlock()

	s.exec.RecordDecision(ctx, signal, assetID, bucketSize, clientOrderID, time.Now().UTC(), seq)
	s.saveState(ctx)

	s.log.Info(
		"vwap: bucket order placed",
		"runID", s.runID,
		"bucket", bucketIdx,
		"client_order_id", clientOrderID,
		"order_id", orderID,
		"bucket_size", bucketSize,
	)
}

// ForTesting accessors for the cross-package resume test
// (strategy/http/handler_resume_test.go). See strategy/twap/strategy.go
// for the contract — identical surface.

func (s *Strategy) MarketClientWired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exec.Market != nil
}

func (s *Strategy) StateStoreWired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exec.Store != nil
}

func (s *Strategy) DecisionSeqForTest() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.decSeq
}

func (s *Strategy) RunStateForTest() (decimal.Decimal, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.TotalSubmitted, s.state.ChunksProcessed
}

func (s *Strategy) LoadStateForTest(ctx context.Context) { s.loadState(ctx) }
func (s *Strategy) RunIDForTest(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runID = id
	s.exec.RunID = id
}
