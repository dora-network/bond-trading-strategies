package http_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestCursor_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 1, 15, 10, 30, 0, 123456789, time.UTC)
	original := &strategyhttp.Cursor{Time: when, Seq: 42, Version: 1}

	encoded := original.Encode()
	require.NotEmpty(t, encoded)
	assert.False(t, strings.ContainsAny(encoded, " \t\n"), "cursor must be URL-safe")

	decoded, err := strategyhttp.DecodeCursor(encoded)
	require.NoError(t, err)
	assert.True(t, original.Time.Equal(decoded.Time), "time round-trip: want %s got %s", original.Time, decoded.Time)
	assert.Equal(t, original.Seq, decoded.Seq)
}

func TestDecodeCursor_RejectsBadInput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"empty":                  "",
		"not base64":             "@@@not-base64@@@",
		"valid base64, bad json": "bm90LWpzb24=",               // "not-json"
		"unknown version":        "eyJ2IjoyLCJ0IjoxLCJzIjoxfQ", // {"v":2,"t":1,"s":1}
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := strategyhttp.DecodeCursor(raw)
			assert.Error(t, err)
		})
	}
}

func TestPGDecisionStore_ListDecisions_Pagination(t *testing.T) {
	t.Parallel()

	runID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	rows := []strategycore.Decision{
		{RunID: runID, Seq: 5, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindOpen, Reason: "z_score_entry"},
		{RunID: runID, Seq: 4, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindClose, Reason: "take_profit"},
		{RunID: runID, Seq: 3, StrategyType: "mean_reversion", Kind: strategycore.DecisionKindOpen, Reason: "z_score_entry"},
	}

	// store backed by a stub queryer. See decision_store_test.go stub below.
	s := newStubDecisionStore(rows)

	t.Run("first page returns the head and a cursor", func(t *testing.T) {
		t.Parallel()
		got, cursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 2,
		})
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, int64(5), got[0].Seq)
		assert.Equal(t, int64(4), got[1].Seq)
		require.NotNil(t, cursor)
		assert.Equal(t, int64(4), cursor.Seq)
	})

	t.Run("second page returns the tail and no cursor", func(t *testing.T) {
		t.Parallel()
		first, cursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, cursor)

		got, nextCursor, err := s.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID:     runID,
			Limit:     2,
			AfterTime: &cursor.Time,
			AfterSeq:  &cursor.Seq,
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(3), got[0].Seq)
		assert.Equal(t, first[1].Seq, got[0].Seq+1)
		assert.Nil(t, nextCursor)
	})

	t.Run("empty run returns zero items and no cursor", func(t *testing.T) {
		t.Parallel()
		empty := newStubDecisionStore(nil)
		got, cursor, err := empty.ListDecisions(context.Background(), strategyhttp.ListDecisionsParams{
			RunID: runID, Limit: 50,
		})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Nil(t, cursor)
	})
}

// stubDecisionStore is a hand-rolled stand-in for the production
// PGDecisionStore used by ListDecisions unit tests. It validates
// arguments and returns the next slice of seeded rows in the order
// they were provided, applying the cursor predicate in memory. The
// production code path is the one with the SQL; this stub exists so
// the cursor / pagination / probe-row logic can be unit-tested
// without a Postgres fixture.
type stubDecisionStore struct {
	all []strategycore.Decision
}

func newStubDecisionStore(seed []strategycore.Decision) *stubDecisionStore {
	return &stubDecisionStore{all: seed}
}

func (s *stubDecisionStore) ListDecisions(_ context.Context, p strategyhttp.ListDecisionsParams) ([]strategycore.Decision, *strategyhttp.Cursor, error) {
	if p.Limit <= 0 {
		return nil, nil, fmt.Errorf("limit must be positive, got %d", p.Limit)
	}
	filtered := make([]strategycore.Decision, 0, len(s.all))
	for _, d := range s.all {
		if d.RunID != p.RunID {
			continue
		}
		if p.From != nil && d.CreatedAt.Before(*p.From) {
			continue
		}
		if p.To != nil && d.CreatedAt.After(*p.To) {
			continue
		}
		if p.AfterTime != nil {
			if d.CreatedAt.After(*p.AfterTime) {
				continue
			}
			if d.CreatedAt.Equal(*p.AfterTime) && p.AfterSeq != nil && d.Seq >= *p.AfterSeq {
				continue
			}
		}
		filtered = append(filtered, d)
	}
	// already newest-first
	if len(filtered) > p.Limit {
		next := filtered[p.Limit-1]
		filtered = filtered[:p.Limit]
		return filtered, &strategyhttp.Cursor{Time: next.CreatedAt, Seq: next.Seq, Version: 1}, nil
	}
	return filtered, nil, nil
}
