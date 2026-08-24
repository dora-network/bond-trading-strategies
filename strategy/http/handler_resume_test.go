package http

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/strategyfakes"
	"github.com/dora-network/bond-trading-strategies/strategy/twap"
	"github.com/dora-network/bond-trading-strategies/strategy/vwap"
	"github.com/google/uuid"
	"github.com/govalues/decimal"
	"github.com/stretchr/testify/require"
)

// memStateStore is an in-memory strategycore.StateStore for resume
// tests. Thread-safe.
type memStateStore struct {
	mu   sync.Mutex
	data map[uuid.UUID][]byte
}

func (s *memStateStore) SaveState(_ context.Context, runID uuid.UUID, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = map[uuid.UUID][]byte{}
	}
	s.data[runID] = raw
	return nil
}

func (s *memStateStore) LoadState(_ context.Context, runID uuid.UUID) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[runID], nil
}

// memDecisionStore is an in-memory strategycore.DecisionRecorder
// returning a configurable maxSeq for the resume path's seed step.
type memDecisionStore struct {
	maxSeq int64
}

func (s *memDecisionStore) SaveDecision(_ context.Context, _ strategycore.Decision) error {
	return nil
}

func (s *memDecisionStore) MaxSeq(_ context.Context, _ uuid.UUID) (int64, error) {
	return s.maxSeq, nil
}

// silentLogger returns an *slog.Logger that drops every record.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runResume builds the minimum harness for resumePersistedRun and
// captures the strategy instance the handler handed to
// service.RunStrategy. The captured strategy is the same object
// the handler wired up via attachStateStore, the decrypt switch,
// and SetDecisionSeq — probing it is the regression target.
//
// Seeds a vwap-style RunState into the store. The alias collapses
// twap.RunState and vwap.RunState to exec.RunState, so the bytes
// are interchangeable — TWAP and VWAP share the same on-the-wire
// JSON shape.
func runResume(
	t *testing.T,
	strategyType string,
	persistedTotal decimal.Decimal,
	persistedChunks int,
	maxSeq int64,
) (strategycore.Strategy, *memStateStore, uuid.UUID) {
	t.Helper()

	runID := uuid.New()
	var captured strategycore.Strategy

	state := vwap.RunState{
		TotalSubmitted:  persistedTotal,
		ChunksProcessed: persistedChunks,
	}
	persistedBytes, err := json.Marshal(&state)
	require.NoError(t, err)

	store := &memStateStore{}
	require.NoError(t, store.SaveState(context.Background(), runID, persistedBytes))

	encKey := make([]byte, 32)
	_, err = rand.Read(encKey)
	require.NoError(t, err)

	apiKey := []byte("test-api-key")
	encryptedAPIKey := encryptForTest(t, encKey, apiKey)

	startTime := time.Now().UTC().Add(-time.Minute)
	endTime := time.Now().UTC().Add(time.Hour)
	var cfg json.RawMessage
	switch strategyType {
	case "vwap":
		cfg, err = json.Marshal(vwap.Config{
			OrderBookID:   runID.String(),
			TotalAmount:   decimal.MustNew(1000, 0),
			Side:          "buy",
			StartTime:     startTime,
			EndTime:       endTime,
			WindowDays:    30,
			BucketMinutes: 5,
		})
	case "twap":
		cfg, err = json.Marshal(twap.Config{
			OrderBookID:     runID.String(),
			TotalAmount:     decimal.MustNew(1000, 0),
			Side:            "buy",
			StartTime:       startTime,
			EndTime:         endTime,
			IntervalSeconds: 60,
		})
	default:
		t.Fatalf("unsupported strategy type %q", strategyType)
	}
	require.NoError(t, err)

	svc := &strategyfakes.FakeService{}
	svc.BaseContextReturns(context.Background())
	svc.RunStrategyCalls(func(_ context.Context, s strategycore.Strategy) (uuid.UUID, error) {
		captured = s
		return runID, nil
	})
	svc.IsRunActiveCalls(func(_ uuid.UUID) bool { return true })

	h := &Handler{
		service:               svc,
		stateStore:            store,
		encryptionKey:         encKey,
		decisionStore:         &memDecisionStore{maxSeq: maxSeq},
		now:                   time.Now,
		runs:                  make(map[uuid.UUID]*RunDetail),
		runningStrategies:     make(map[uuid.UUID]strategycore.Strategy),
		stopLossObservers:     make(map[uuid.UUID]context.CancelFunc),
		runCompletionWatchers: make(map[uuid.UUID]context.CancelFunc),
		strategies:            defaultStrategies(nil, nil, nil, nil, nil, silentLogger()),
		log:                   silentLogger(),
	}

	detail := &RunDetail{
		RunSummary: RunSummary{
			ID:           runID,
			DORAUserID:   "user-1",
			StrategyType: strategyType,
			Status:       "running",
		},
		Config:          cfg,
		EncryptedAPIKey: encryptedAPIKey,
	}

	require.NoError(t, h.resumePersistedRun(context.Background(), detail))
	require.NotNil(t, captured, "strategy must have been captured from RunStrategyCalls")
	return captured, store, runID
}

// TestResumePersistedRun_WiresAllThreeFindings is the regression
// test for the VWAP/TWAP wiring findings the code review flagged:
//
//  1. attachStateStore missing the vwap case (state never loaded).
//  2. The decrypt/API-key switch in resumePersistedRun missing the
//     vwap case (Market client stays nil → PlaceOrder panics on
//     first order).
//  3. The SetDecisionSeq switch missing the vwap case (decision seq
//     restarts at 1 → PK collision on strategy_decisions).
//
// All three converge on the same root cause: the resume path
// excluded *vwap.Strategy from its per-strategy switch arms. This
// test exercises the full resume path against both execution
// strategies and asserts the three wiring invariants directly
// using the per-strategy ForTesting accessors — covering all three
// findings in one test rather than three separate switches.
func TestResumePersistedRun_WiresAllThreeFindings(t *testing.T) {
	t.Parallel()

	const (
		persistedTotal  int64 = 250
		persistedChunks int   = 4
		maxSeq          int64 = 17
	)

	for _, strategyType := range []string{"twap", "vwap"} {
		strategyType := strategyType
		t.Run(strategyType, func(t *testing.T) {
			t.Parallel()

			resumed, _, runID := runResume(t, strategyType,
				decimal.MustNew(persistedTotal, 0), persistedChunks, maxSeq)
			require.NotNil(t, resumed)
			require.NotEqual(t, uuid.Nil, runID)

			var (
				marketOK, storeOK bool
				decSeq            int64
				total             decimal.Decimal
				chunks            int
			)
			switch s := resumed.(type) {
			case *twap.Strategy:
				marketOK = s.MarketClientWired()
				storeOK = s.StateStoreWired()
				decSeq = s.DecisionSeqForTest()
				s.RunIDForTest(runID)
				s.LoadStateForTest(context.Background())
				total, chunks = s.RunStateForTest()
			case *vwap.Strategy:
				marketOK = s.MarketClientWired()
				storeOK = s.StateStoreWired()
				decSeq = s.DecisionSeqForTest()
				s.RunIDForTest(runID)
				s.LoadStateForTest(context.Background())
				total, chunks = s.RunStateForTest()
			default:
				t.Fatalf("%s: unexpected resumed strategy type %T", strategyType, resumed)
			}

			require.True(t, marketOK,
				"%s: Market client must be wired by resumePersistedRun's decrypt switch "+
					"(finding #2 — nil panic on PlaceOrder)", strategyType)
			require.True(t, storeOK,
				"%s: state Store must be wired by resumePersistedRun's attachStateStore "+
					"(finding #1 — restart over-executes)", strategyType)
			require.Equal(t, maxSeq, decSeq,
				"%s: decision seq must be seeded past MaxSeq by resumePersistedRun's SetDecisionSeq switch "+
					"(finding #3 — PK collision)", strategyType)
			whole, _, _ := total.Int64(0)
			require.Equal(t, persistedTotal, whole,
				"%s: TotalSubmitted must be loaded from persisted state, not zero", strategyType)
			require.Equal(t, persistedChunks, chunks,
				"%s: ChunksProcessed must be loaded from persisted state, not zero", strategyType)
		})
	}
}

// TestAttachStateStore_WiresBothStrategies guards the resolution
// finding reviewers flagged: the original handler attached the
// StateStore only to *twap.Strategy. Resumed VWAP runs therefore
// saw a nil store and re-submitted every chunk on restart. This
// test asserts attachStateStore attaches the store to BOTH
// strategy types.
func TestAttachStateStore_WiresBothStrategies(t *testing.T) {
	t.Parallel()

	store := &memStateStore{}
	runID := uuid.New()

	twapCfg := twap.DefaultConfig()
	twapCfg.OrderBookID = runID.String()
	twapStrat := twap.New(twapCfg, silentLogger())

	vwapCfg := vwap.DefaultConfig()
	vwapCfg.OrderBookID = runID.String()
	vwapStrat := vwap.New(vwapCfg, silentLogger())

	h := &Handler{stateStore: store}
	h.attachStateStore(twapStrat)
	h.attachStateStore(vwapStrat)

	require.True(t, twapStrat.StateStoreWired(),
		"twap strategy must receive the state store")
	require.True(t, vwapStrat.StateStoreWired(),
		"vwap strategy must receive the state store")
}

// encryptForTest wraps the handler's internal AES-GCM encrypt so
// the in-package test can stage encrypted keys for
// resumePersistedRun. Mirrors the handler-side encryption (nonce
// prepended to ciphertext).
func encryptForTest(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	require.Len(t, key, 32, "key must be 32 bytes for AES-256")

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	require.NoError(t, err)

	return gcm.Seal(nonce, nonce, plaintext, nil)
}
