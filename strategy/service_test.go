package strategy_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/meanreversion"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	timeout = time.Second
	svc     = strategy.NewService()
	start   = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end     = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
)

func TestService_RunBacktest(t *testing.T) {
	t.Run("should return a backtest result if the strategy backtest succeeds", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		myStrategy := &strategyfakes.FakeStrategy{
			BacktestStub: func(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
				return meanreversion.BacktestResult{
					ClosedTrades: nil,
					TotalPnL:     decimal.Decimal{},
					WinCount:     0,
					LossCount:    0,
					MaxDrawdown:  decimal.Decimal{},
					SharpeRatio:  decimal.Decimal{},
				}, nil
			},
		}

		ch, err := svc.RunBacktest(ctx, uuid.Nil, myStrategy, start, end)
		require.NoError(t, err)
		assert.NotNil(t, ch)
		for {
			select {
			case res := <-ch:
				assert.Equal(t, meanreversion.BacktestResult{}, res)
				return
			case <-ctx.Done():
				assert.Fail(t, "timeout")
				return
			}
		}
	})

	t.Run("should return an error if the strategy backtest fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		myStrategy := &strategyfakes.FakeStrategy{
			BacktestStub: func(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
				return nil, fmt.Errorf("backtest failed")
			},
		}

		ch, err := svc.RunBacktest(ctx, uuid.Nil, myStrategy, start, end)
		require.NoError(t, err)
		assert.NotNil(t, ch)
		for {
			select {
			case res := <-ch:
				assert.Equal(t, types.ErrorResult{Err: fmt.Errorf("backtest failed")}, res)
				return
			case <-ctx.Done():
				assert.Fail(t, "timeout")
				return
			}
		}
	})
}

func TestService_StopBacktest(t *testing.T) {
	var isRunning atomic.Bool
	myStrategy := &strategyfakes.FakeStrategy{
		BacktestStub: func(ctx context.Context, start, end time.Time) (types.BacktestResult, error) {
			isRunning.Store(true)

			for {
				select {
				case <-ctx.Done():
					return nil, errors.New("backtest cancelled by user")
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		},
	}

	t.Run("should return an error if the backtest ID is not found", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ch, err := svc.RunBacktest(ctx, uuid.Nil, myStrategy, start, end)

		require.NoError(t, err)
		assert.NotNil(t, ch)

		err = svc.StopBacktest(uuid.Must(uuid.NewV7()))
		assert.ErrorIs(t, err, strategy.ErrBacktestNotFound)
	})

	t.Run("should stop the backtest and return the result", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		backtestID := uuid.Must(uuid.NewV7())
		ch, err := svc.RunBacktest(ctx, backtestID, myStrategy, start, end)

		require.NoError(t, err)
		assert.NotNil(t, ch)

		require.NoError(t, svc.StopBacktest(backtestID))
		for {
			select {
			case res := <-ch:
				assert.True(t, isRunning.Load())
				assert.Equal(t, types.ErrorResult{Err: fmt.Errorf("backtest cancelled by user")}, res)
				return
			case <-ctx.Done():
				assert.True(t, isRunning.Load())
				assert.Fail(t, "timeout")
				return
			}
		}
	})
}

func TestService_RunStrategy(t *testing.T) {
	var isRunning atomic.Bool
	wg := sync.WaitGroup{}
	wg.Add(1)
	myStrategy := &strategyfakes.FakeStrategy{
		RunStub: func(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
			isRunning.Store(true)
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgCh:
					t.Logf("received message: %v", msg)
				}
			}
		},
	}

	t.Run("should start the strategy and return the ID", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		id, err := svc.RunStrategy(ctx, myStrategy)

		require.NoError(t, err)
		assert.True(t, strategy.RunExists(svc, id))
		require.Eventually(t, func() bool {
			return isRunning.Load()
		}, time.Second, 10*time.Millisecond)
		assert.True(t, isRunning.Load())
		stopCtx, stopCancel := context.WithTimeout(context.Background(), timeout)
		defer stopCancel()
		require.NoError(t, svc.StopStrategy(stopCtx, id))
		wg.Wait()
	})
}

func TestService_RunStrategyIgnoresRequestContextCancellation(t *testing.T) {
	t.Parallel()

	svc := strategy.NewService()
	started := make(chan struct{})
	stopped := make(chan struct{})
	myStrategy := &strategyfakes.FakeStrategy{
		RunStub: func(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
			close(started)
			defer close(stopped)
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-msgCh:
				}
			}
		},
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	id, err := svc.RunStrategy(requestCtx, myStrategy)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, id)
	<-started

	cancel()

	select {
	case <-stopped:
		t.Fatal("strategy stopped when request context was cancelled")
	case <-time.After(50 * time.Millisecond):
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), timeout)
	defer stopCancel()
	require.NoError(t, svc.StopStrategy(stopCtx, id))
	<-stopped
}

func TestService_StopStrategy(t *testing.T) {
	mu := sync.Mutex{}
	isRunning := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	myStrategy := &strategyfakes.FakeStrategy{
		RunStub: func(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
			mu.Lock()
			isRunning = true
			mu.Unlock()
			wg.Done()

			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgCh:
					switch msg {
					case strategy.Stop:
						mu.Lock()
						isRunning = false
						wg.Done()
						mu.Unlock()
						return nil
					default:
						t.Fatalf("unexpected message: %v", msg)
					}
				}
			}
		},
	}

	t.Run("should return an error if the strategy ID is not found", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		someID := uuid.Must(uuid.NewV7())
		err := svc.StopStrategy(ctx, someID)
		require.Error(t, err)
		assert.ErrorIs(t, err, strategy.ErrRunIDNotFound)
	})

	t.Run("should stop the strategy and return nil", func(t *testing.T) {
		startCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		id, err := svc.RunStrategy(startCtx, myStrategy)
		require.NoError(t, err)
		assert.True(t, strategy.RunExists(svc, id))
		wg.Wait()
		assert.True(t, isRunning)
		stopCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		wg.Add(1)
		err = svc.StopStrategy(stopCtx, id)
		require.NoError(t, err)
		wg.Wait()
		require.Eventually(t, func() bool {
			return !strategy.RunExists(svc, id)
		}, time.Second, 10*time.Millisecond)
		assert.False(t, isRunning)
	})
}

func TestService_PauseStrategy(t *testing.T) {
	mu := sync.Mutex{}
	isRunning := false
	isPaused := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	myStrategy := &strategyfakes.FakeStrategy{
		RunStub: func(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
			mu.Lock()
			isRunning = true
			mu.Unlock()
			wg.Done()

			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgCh:
					switch msg {
					case strategy.Pause:
						mu.Lock()
						isPaused = true
						wg.Done()
						mu.Unlock()
					default:
						t.Fatalf("unexpected message: %v", msg)
					}
				}
			}
		},
	}

	t.Run("should return an error if the strategy ID is not found", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		someID := uuid.Must(uuid.NewV7())
		err := svc.PauseStrategy(ctx, someID)
		require.Error(t, err)
		assert.ErrorIs(t, err, strategy.ErrRunIDNotFound)
	})

	t.Run("should pause the strategy and return nil", func(t *testing.T) {
		startCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		id, err := svc.RunStrategy(startCtx, myStrategy)
		require.NoError(t, err)
		assert.True(t, strategy.RunExists(svc, id))
		wg.Wait()
		assert.True(t, isRunning)
		pauseCtx, pauseCtxCancel := context.WithTimeout(context.Background(), timeout)
		defer pauseCtxCancel()
		wg.Add(1)
		err = svc.PauseStrategy(pauseCtx, id)
		require.NoError(t, err)
		wg.Wait()
		assert.True(t, isPaused)
	})
}

func TestService_ResumeStrategy(t *testing.T) {
	mu := sync.Mutex{}
	isRunning := false
	isPaused := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	myStrategy := &strategyfakes.FakeStrategy{
		RunStub: func(ctx context.Context, msgCh <-chan strategy.Message, runID uuid.UUID) error {
			mu.Lock()
			isRunning = true
			mu.Unlock()
			wg.Done()

			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-msgCh:
					switch msg {
					case strategy.Stop:
						return errors.New("received unexpected stop message")
					case strategy.Pause:
						mu.Lock()
						isPaused = true
						wg.Done()
						mu.Unlock()
					case strategy.Resume:
						mu.Lock()
						isPaused = false
						wg.Done()
						mu.Unlock()
					default:
						return errors.New("unexpected message")
					}
				}
			}
		},
	}

	t.Run("should return an error if the strategy ID is not found", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		someID := uuid.Must(uuid.NewV7())
		err := svc.ResumeStrategy(ctx, someID)
		require.Error(t, err)
		assert.ErrorIs(t, err, strategy.ErrRunIDNotFound)
	})

	t.Run("should resume the strategy and return nil", func(t *testing.T) {
		startCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		id, err := svc.RunStrategy(startCtx, myStrategy)
		require.NoError(t, err)
		assert.True(t, strategy.RunExists(svc, id))
		wg.Wait()
		assert.True(t, isRunning)
		pauseCtx, pauseCtxCancel := context.WithTimeout(context.Background(), timeout)
		defer pauseCtxCancel()
		wg.Add(1)
		err = svc.PauseStrategy(pauseCtx, id)
		require.NoError(t, err)
		wg.Wait()
		assert.True(t, isPaused)
		resumeCtx, resumeCtxCancel := context.WithTimeout(context.Background(), timeout)
		defer resumeCtxCancel()
		wg.Add(1)
		err = svc.ResumeStrategy(resumeCtx, id)
		require.NoError(t, err)
		wg.Wait()
		assert.False(t, isPaused)
	})
}
