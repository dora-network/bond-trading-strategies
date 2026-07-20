package twap

import (
	"fmt"
	"time"

	"github.com/govalues/decimal"
)

// Config holds all tunable parameters for the TWAP strategy.
type Config struct {
	// OrderBookID is the DORA order book UUID where orders will be placed.
	OrderBookID string `json:"order_book_id"`
	// TotalAmount is the total quantity to trade across the execution window.
	TotalAmount decimal.Decimal `json:"total_amount"`
	// Side is "buy" or "sell".
	Side string `json:"side"`
	// StartTime is the ISO 8601 start time for execution.
	StartTime time.Time `json:"start_time"`
	// EndTime is the ISO 8601 end time for execution.
	EndTime time.Time `json:"end_time"`
	// IntervalSeconds is the time between each chunk order (default 300 = 5 min).
	IntervalSeconds int `json:"interval_seconds"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		IntervalSeconds: 300,
	}
}

// Validate checks that the config is valid for a run.
func (c Config) Validate() error {
	if c.OrderBookID == "" {
		return fmt.Errorf("order_book_id is required")
	}
	if c.TotalAmount.IsZero() || c.TotalAmount.IsNeg() {
		return fmt.Errorf("total_amount must be positive")
	}
	if c.Side != "buy" && c.Side != "sell" {
		return fmt.Errorf("side must be 'buy' or 'sell'")
	}
	if c.StartTime.IsZero() || c.EndTime.IsZero() {
		return fmt.Errorf("start_time and end_time are required")
	}
	if !c.EndTime.After(c.StartTime) {
		return fmt.Errorf("end_time must be strictly after start_time")
	}
	if c.IntervalSeconds <= 0 {
		return fmt.Errorf("interval_seconds must be positive")
	}
	return nil
}

// NumChunks returns the number of execution chunks.
func (c Config) NumChunks() int {
	duration := c.EndTime.Sub(c.StartTime)
	interval := time.Duration(c.IntervalSeconds) * time.Second
	if duration <= 0 {
		return 0
	}
	n := int(duration / interval)
	if n == 0 {
		n = 1
	}
	return n
}
