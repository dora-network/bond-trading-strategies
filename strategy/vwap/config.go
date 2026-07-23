package vwap

import (
	"fmt"
	"time"

	"github.com/govalues/decimal"
)

// Config holds all tunable parameters for the VWAP strategy.
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
	// WindowDays is how many days of trade history to use for computing
	// the ADV (Average Daily Volume) bucket distribution. Default 30.
	WindowDays int `json:"window_days"`
	// BucketMinutes is the granularity of each VWAP bucket. Default 5.
	BucketMinutes int `json:"bucket_minutes"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		WindowDays:    30,
		BucketMinutes: 5,
	}
}

// Validate checks that the config is valid for a run. The now parameter
// lets tests inject a fixed clock; pass time.Now().UTC() in production.
func (c Config) Validate(now time.Time) error {
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
	if !c.EndTime.After(now) {
		return fmt.Errorf("end_time must be in the future (got %s, now %s)", c.EndTime.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	if c.WindowDays <= 0 {
		return fmt.Errorf("window_days must be positive")
	}
	if c.BucketMinutes <= 0 {
		return fmt.Errorf("bucket_minutes must be positive")
	}
	return nil
}

// NumBuckets returns the number of execution buckets across the
// [StartTime, EndTime] window for the configured BucketMinutes.
func (c Config) NumBuckets() int {
	duration := c.EndTime.Sub(c.StartTime)
	bucket := time.Duration(c.BucketMinutes) * time.Minute
	if duration <= 0 || bucket <= 0 {
		return 0
	}
	n := int(duration / bucket)
	if n == 0 {
		n = 1
	}
	return n
}
