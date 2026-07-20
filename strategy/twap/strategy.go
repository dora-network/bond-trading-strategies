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
	orderUpdates    <-chan OrderFillEvent
	runState        RunState
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

// WithOrderUpdates injects a channel of order fill events from the
// DORA order update stream. The run loop reads from this channel to
// track actual fill quantities and rebalance remaining chunks.
func WithOrderUpdates(ch <-chan OrderFillEvent) func(*Strategy) {
	return func(s *Strategy) { s.orderUpdates = ch }
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
	numChunks := s.cfg.NumChunks()
	if numChunks == 0 {
		return fmt.Errorf("twap: no chunks in execution window")
	}
	duration := s.cfg.EndTime.Sub(s.cfg.StartTime)
	interval := duration / time.Duration(numChunks)

	signal := types.SignalBuy
	if s.cfg.Side == "sell" {
		signal = types.SignalSell
	}
	assetID := s.cfg.OrderBookID

	s.loadState(ctx)
	s.reconcilePendingOrders(ctx)

	s.mu.RLock()
	state := s.runState
	chunksProcessed := state.ChunksProcessed
	totalSubmitted := state.TotalSubmitted
	s.mu.RUnlock()

	baseChunkSize, err := s.computeChunkSize(numChunks, totalSubmitted, chunksProcessed)
	if err != nil {
		return fmt.Errorf("twap: calculate chunk size: %w", err)
	}

	scheduledTime := func(i int) time.Time {
		return s.cfg.StartTime.Add(time.Duration(i) * interval)
	}

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
		"orders", len(state.Orders),
	)

	nextChunk := s.skipMissedChunks(ctx, numChunks, scheduledTime, chunksProcessed)
	if nextChunk >= numChunks {
		return nil
	}

	var timer *time.Timer
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		wait := time.Until(scheduledTime(nextChunk))
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

	for nextChunk < numChunks {
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
			if !s.handleChunkTick(ctx, nextChunk, numChunks, baseChunkSize, assetID, signal) {
				return nil
			}
			nextChunk++
			resetTimer()
		}
	}
	return nil
}

// skipMissedChunks consumes chunk slots whose scheduled time has
// already passed (missed during downtime or pause). Returns the
// index of the next chunk to schedule.
func (s *Strategy) skipMissedChunks(ctx context.Context, numChunks int, scheduledTime func(int) time.Time, chunksProcessed int) int {
	nextChunk := chunksProcessed
	now := time.Now().UTC()
	for nextChunk < numChunks && !now.Before(scheduledTime(nextChunk)) {
		s.mu.Lock()
		s.runState.ChunksProcessed = nextChunk + 1
		s.mu.Unlock()
		s.saveState(ctx)
		s.log.Info("twap: skipping missed chunk", "runID", s.runID, "chunk", nextChunk)
		nextChunk++
	}
	if nextChunk >= numChunks {
		s.log.Info("twap: execution window fully elapsed", "runID", s.runID)
	}
	return nextChunk
}

// computeChunkSize returns the rebalanced chunk size for the next
// chunk: (totalAmount - totalSubmitted) / (numChunks - chunksProcessed).
// Using TotalSubmitted (not TotalFilled) prevents re-placing in-flight
// or cancelled quantities.
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

// handleChunkTick places the next chunk order (or skips it if paused),
// advances the chunk index, and persists state. Returns false if the
// run has completed and the caller should return.
func (s *Strategy) handleChunkTick(
	ctx context.Context,
	chunkIdx, numChunks int,
	baseChunkSize decimal.Decimal,
	assetID string,
	signal types.Signal,
) bool {
	s.mu.RLock()
	paused := s.paused
	ts := s.runState.TotalSubmitted
	cp := s.runState.ChunksProcessed
	s.mu.RUnlock()

	if paused {
		s.log.Info("twap: skipping chunk (paused)", "runID", s.runID, "chunk", chunkIdx)
	} else {
		chunkSize, err := s.computeChunkSize(numChunks, ts, cp)
		if err != nil {
			s.log.Error("twap: recalculate chunk size", "err", err)
			chunkSize = baseChunkSize
		}
		s.placeChunk(ctx, chunkIdx, assetID, signal, chunkSize)
	}

	nextChunk := chunkIdx + 1
	s.mu.Lock()
	s.runState.ChunksProcessed = nextChunk
	s.mu.Unlock()
	s.saveState(ctx)

	if nextChunk >= numChunks {
		s.log.Info("twap: all chunks placed", "runID", s.runID)
		return false
	}
	return true
}

// placeChunk places a single chunk order and records it in pending state.
func (s *Strategy) placeChunk(ctx context.Context, chunkIdx int, assetID string, signal types.Signal, chunkSize decimal.Decimal) {
	clientOrderID := strategy.BuildClientOrderID(StrategyType, s.runID)
	decision := NewDecision(time.Now().UTC(), assetID, signal, chunkSize)
	orderID, err := s.marketAPIClient.CreateMarketOrder(
		ctx,
		decision.bondID,
		doraclientSide(signal),
		chunkSize,
		decimal.One,
		false,
		clientOrderID,
	)
	if err != nil {
		s.log.Error("twap: create market order failed", "runID", s.runID, "chunk", chunkIdx, "err", err)
		return
	}

	// Track this order so we can reconcile it on restart.
	s.mu.Lock()
	s.runState.Orders = append(s.runState.Orders, OrderEntry{
		OrderID:           orderID,
		ClientOrderID:     clientOrderID,
		RequestedQuantity: chunkSize,
		FilledQuantity:    decimal.Zero,
		Status:            statusOpen,
	})
	s.runState.TotalSubmitted, _ = s.runState.TotalSubmitted.Add(chunkSize)
	s.mu.Unlock()

	s.recordDecision(ctx, decision, clientOrderID)
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

// processOrderUpdate handles an order fill event from the DORA order
// update stream. It matches the event to an order by client_order_id
// and replaces its filled_quantity and status with the values from
// the event. The updated state is persisted after each event.
func (s *Strategy) processOrderUpdate(ctx context.Context, evt OrderFillEvent) {
	s.mu.Lock()
	idx := -1
	for i, o := range s.runState.Orders {
		if o.ClientOrderID == evt.ClientOrderID {
			idx = i
			break
		}
	}
	if idx == -1 {
		s.mu.Unlock()
		s.log.Debug(
			"twap: order update for unknown client_order_id, ignoring",
			"runID", s.runID,
			"client_order_id", evt.ClientOrderID,
		)
		return
	}

	entry := &s.runState.Orders[idx]
	wasTerminal := isTerminal(entry.Status)
	entry.FilledQuantity = evt.FilledQuantity
	entry.Status = evt.Status
	nowTerminal := isTerminal(evt.Status)

	// Add to running total only on the terminal transition — once,
	// not on every partial fill.
	if nowTerminal && !wasTerminal {
		s.runState.TotalFilled, _ = s.runState.TotalFilled.Add(evt.FilledQuantity)
	}
	s.mu.Unlock()

	if nowTerminal {
		s.log.Info(
			"twap: order reached terminal status",
			"runID", s.runID,
			"client_order_id", evt.ClientOrderID,
			"status", evt.Status,
			"filled", evt.FilledQuantity,
			"totalFilled", s.runState.TotalFilled,
		)
	}

	s.saveState(ctx)
}

// reconcilePendingOrders queries DORA for each order that was open when
// the strategy was last running, updating state with their actual
// status and filled quantity. Called once at run startup, after
// loadState. Orders that reached terminal status during downtime
// contribute to TotalFilled.
func (s *Strategy) reconcilePendingOrders(ctx context.Context) {
	if s.marketAPIClient == nil {
		return
	}
	s.mu.Lock()
	orders := append([]OrderEntry(nil), s.runState.Orders...)
	s.mu.Unlock()

	for i, o := range orders {
		// Skip orders already at terminal status.
		if isTerminal(o.Status) {
			continue
		}
		status, filledQty, err := s.marketAPIClient.GetOrderFilledStatus(ctx, o.OrderID)
		if err != nil {
			s.log.Error("twap: reconcile order failed", "runID", s.runID, "orderID", o.OrderID, "err", err)
			continue
		}
		s.mu.Lock()
		entry := &s.runState.Orders[i]
		if !isTerminal(entry.Status) && isTerminal(status) {
			s.runState.TotalFilled, _ = s.runState.TotalFilled.Add(filledQty)
		}
		entry.FilledQuantity = filledQty
		entry.Status = status
		s.mu.Unlock()
	}
	s.saveState(ctx)
}

// saveState persists the current run state to the state store. A
// failure is logged but non-fatal — the strategy continues, and the
// next checkpoint overwrites the stale row.
func (s *Strategy) saveState(ctx context.Context) {
	if s.stateStore == nil {
		return
	}
	s.mu.RLock()
	state := s.runState
	s.mu.RUnlock()

	raw, err := state.Marshal()
	if err != nil {
		s.log.Error("twap: marshal run state", "err", err)
		return
	}
	if err := s.stateStore.SaveState(ctx, s.runID, raw); err != nil {
		s.log.Error("twap: save run state", "err", err, "runID", s.runID)
	}
}

// loadState reads the persisted run state from the state store. Called
// once at the start of run(). A failure or empty state is non-fatal —
// the strategy starts fresh from zero.
func (s *Strategy) loadState(ctx context.Context) {
	if s.stateStore == nil {
		return
	}
	raw, err := s.stateStore.LoadState(ctx, s.runID)
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

// doraclientSide converts a strategy signal to a DORA order side.
func doraclientSide(signal types.Signal) doraclient.Side {
	if signal == types.SignalSell {
		return doraclient.SIDE_SELL
	}
	return doraclient.SIDE_BUY
}
