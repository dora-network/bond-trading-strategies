package vwap

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"

	"github.com/dora-network/bond-trading-strategies/strategy/breakout"
)

// TradeVolumeStore supplies historical trades for VWAP bucket
// computation. In production, *breakout.PGTradeHistoryStore satisfies
// this interface; tests can pass a fake.
type TradeVolumeStore interface {
	StreamTrades(
		ctx context.Context,
		orderBookID uuid.UUID,
		start, end time.Time,
	) (<-chan breakout.Trade, <-chan error)
}

// VolumeBucket is the aggregated historical volume for a single
// time-of-day bucket.
type VolumeBucket struct {
	Start  time.Time
	Volume decimal.Decimal
}
