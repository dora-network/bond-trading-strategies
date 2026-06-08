package strategy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

// Service is the interface that must be implemented by a strategy service.
// A strategy service is responsible for managing and backtesting trading strategies offered by the DORA network.
//
//counterfeiter:generate . Service
type Service interface {
	// RunBacktest runs a backtest of the given strategy against the given historical data.
	// The caller supplies the backtest id so it can be embedded in any
	// per-trade rows the strategy writes while running.
	RunBacktest(ctx context.Context, id uuid.UUID, strategy Strategy, start, end time.Time) (result <-chan types.BacktestResult, err error)
	// StopBacktest stops a backtest with the given ID, before it completes.
	StopBacktest(id uuid.UUID) error
	// RunStrategy starts a trading strategy in the background.
	RunStrategy(ctx context.Context, strategy Strategy) (id uuid.UUID, err error)
	// StopStrategy stops a running strategy.
	StopStrategy(ctx context.Context, id uuid.UUID) error
	// PauseStrategy pauses a running strategy.
	PauseStrategy(ctx context.Context, id uuid.UUID) error
	// ResumeStrategy resumes a paused strategy.
	ResumeStrategy(ctx context.Context, id uuid.UUID) error
}

var (
	ErrBacktestNotFound = errors.New("backtest not found")
	ErrRunIDNotFound    = errors.New("run ID not found")
)

type service struct {
	mu         sync.RWMutex
	baseCtx    context.Context
	backtests  map[uuid.UUID]context.CancelFunc
	strategies map[uuid.UUID]*runState
}

type runState struct {
	messages chan Message
	cancel   context.CancelFunc
}

func NewService(opts ...func(*service)) Service {
	s := &service{
		baseCtx:    context.Background(),
		backtests:  make(map[uuid.UUID]context.CancelFunc),
		strategies: make(map[uuid.UUID]*runState),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.baseCtx == nil {
		s.baseCtx = context.Background()
	}
	return s
}

func WithBaseContext(ctx context.Context) func(*service) {
	return func(s *service) {
		s.baseCtx = ctx
	}
}

func (s *service) RunBacktest(
	_ context.Context,
	backtestID uuid.UUID,
	strategy Strategy,
	start, end time.Time,
) (<-chan types.BacktestResult, error) {
	ch := make(chan types.BacktestResult)
	// we have to use the service base context here as the endpoint is async and the request context will end
	// before the backtest is complete
	btCtx, btCancel := context.WithCancel(s.baseCtx)
	s.mu.Lock()
	s.backtests[backtestID] = btCancel
	s.mu.Unlock()
	go func() {
		defer btCancel()
		res, err := strategy.Backtest(btCtx, start, end)
		if err != nil {
			ch <- types.ErrorResult{Err: err}
			s.mu.Lock()
			delete(s.backtests, backtestID)
			s.mu.Unlock()
			return
		}
		ch <- res
		s.mu.Lock()
		delete(s.backtests, backtestID)
		s.mu.Unlock()
	}()
	return ch, nil
}

func (s *service) StopBacktest(id uuid.UUID) error {
	s.mu.RLock()
	cancel, ok := s.backtests[id]
	s.mu.RUnlock()
	if !ok {
		return ErrBacktestNotFound
	}
	cancel()
	return nil
}

func (s *service) RunStrategy(ctx context.Context, strategy Strategy) (id uuid.UUID, err error) {
	runID := uuid.Must(uuid.NewV7())

	if err := s.RunStrategyWithID(ctx, runID, strategy); err != nil {
		return uuid.Nil, err
	}
	return runID, nil
}

func (s *service) RunStrategyWithID(ctx context.Context, id uuid.UUID, strategy Strategy) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	ch := make(chan Message)
	runCtx, cancel := context.WithCancel(s.baseCtx)
	state := &runState{messages: ch, cancel: cancel}

	s.mu.Lock()
	if _, exists := s.strategies[id]; exists {
		s.mu.Unlock()
		cancel()
		return fmt.Errorf("run ID %s already exists", id)
	}
	s.strategies[id] = state
	s.mu.Unlock()

	go func() {
		defer cancel()
		if err := strategy.Run(runCtx, ch, id); err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(runCtx, "strategy exited with error", "err", err, "run_id", id)
		}
		s.mu.Lock()
		if current, ok := s.strategies[id]; ok && current == state {
			delete(s.strategies, id)
		}
		s.mu.Unlock()
	}()

	return nil
}

func (s *service) StopStrategy(ctx context.Context, id uuid.UUID) error {
	s.mu.RLock()
	strategy, ok := s.strategies[id]
	s.mu.RUnlock()
	if !ok {
		return ErrRunIDNotFound
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case strategy.messages <- Stop:
		strategy.cancel()
		return nil
	}
}

func (s *service) PauseStrategy(ctx context.Context, id uuid.UUID) error {
	s.mu.RLock()
	strategy, ok := s.strategies[id]
	s.mu.RUnlock()
	if !ok {
		return ErrRunIDNotFound
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case strategy.messages <- Pause:
		return nil
	}
}

func (s *service) ResumeStrategy(ctx context.Context, id uuid.UUID) error {
	s.mu.RLock()
	strategy, ok := s.strategies[id]
	s.mu.RUnlock()
	if !ok {
		return ErrRunIDNotFound
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case strategy.messages <- Resume:
		return nil
	}
}
