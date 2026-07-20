package vwap

import (
	"time"

	"github.com/dora-network/bond-trading-strategies/strategy/types"
	"github.com/govalues/decimal"
)

// Decision is the per-bucket evaluation output produced by the VWAP strategy.
// It satisfies strategy.Decision via accessor methods.
type Decision struct {
	time     time.Time
	bondID   string
	price    decimal.Decimal
	signal   types.Signal
	position decimal.Decimal
}

func (d Decision) Time() time.Time               { return d.time }
func (d Decision) BondID() string                { return d.bondID }
func (d Decision) Price() decimal.Decimal        { return d.price }
func (d Decision) Signal() types.Signal          { return d.signal }
func (d Decision) PositionSize() decimal.Decimal { return d.position }

// StrategyType returns the constant strategy name.
func (d Decision) StrategyType() string { return StrategyType }

// Reason returns a machine-readable code.
func (d Decision) Reason() string { return "vwap_execution" }

// NewDecision creates a new VWAP decision.
func NewDecision(t time.Time, bondID string, signal types.Signal, position decimal.Decimal) Decision {
	return Decision{
		time:     t,
		bondID:   bondID,
		signal:   signal,
		position: position,
	}
}
