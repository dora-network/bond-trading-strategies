package twap

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
)

// Decision is the per-chunk evaluation output produced by the TWAP strategy.
// It satisfies strategy.Decision via accessor methods.
//
// Go disallows a field and a method with the same name on the same type, so
// any field that an interface method needs to expose must be unexported on
// this struct; the method becomes the public accessor.
type Decision struct {
	time     time.Time
	bondID   string
	price    decimal.Decimal
	signal   types.Signal
	position decimal.Decimal
}

// Time returns the timestamp of this decision.
func (d Decision) Time() time.Time { return d.time }

// BondID returns the order book identifier.
func (d Decision) BondID() string { return d.bondID }

// Price returns 0 for TWAP since market orders fill at unknown price.
func (d Decision) Price() decimal.Decimal { return d.price }

// Signal returns BUY or SELL.
func (d Decision) Signal() types.Signal { return d.signal }

// PositionSize returns the quantity for this chunk.
func (d Decision) PositionSize() decimal.Decimal { return d.position }

// StrategyType returns the constant strategy name.
func (d Decision) StrategyType() string { return "twap" }

// Reason returns a machine-readable code.
func (d Decision) Reason() string { return "twap_execution" }

// NewDecision creates a new TWAP decision.
func NewDecision(t time.Time, bondID string, signal types.Signal, position decimal.Decimal) Decision {
	return Decision{
		time:     t,
		bondID:   bondID,
		signal:   signal,
		position: position,
	}
}
