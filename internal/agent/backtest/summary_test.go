package backtest

import (
	"math"
	"testing"
	"time"
)

// approxEq is the floating-point comparison helper for summary fields.
// 1e-9 is well below any v1 stats precision we care about.
func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestCompute_BasicRoundTrip(t *testing.T) {
	t.Parallel()
	fills := []Fill{
		{Timestamp: time.Unix(0, 0), Side: "buy", Quantity: 1, Price: 100},
		{Timestamp: time.Unix(1, 0), Side: "sell", Quantity: 1, Price: 110},
	}
	s := Compute(fills, 1000)
	if !approxEq(s.TotalReturn, 0.01) {
		t.Errorf("total_return: got %v, want 0.01", s.TotalReturn)
	}
	if s.TradeCount != 2 {
		t.Errorf("trade_count: got %d, want 2", s.TradeCount)
	}
	if !approxEq(s.WinRate, 1.0) {
		t.Errorf("win_rate: got %v, want 1.0", s.WinRate)
	}
	if !approxEq(s.StartEquity, 1000) {
		t.Errorf("start_equity: got %v, want 1000", s.StartEquity)
	}
	if !approxEq(s.EndEquity, 1010) {
		t.Errorf("end_equity: got %v, want 1010", s.EndEquity)
	}
}

func TestCompute_NoFills(t *testing.T) {
	t.Parallel()
	s := Compute(nil, 1000)
	if !approxEq(s.TotalReturn, 0) {
		t.Errorf("total_return: got %v, want 0", s.TotalReturn)
	}
	if s.TradeCount != 0 {
		t.Errorf("trade_count: got %d, want 0", s.TradeCount)
	}
	if !approxEq(s.WinRate, 0) {
		t.Errorf("win_rate: got %v, want 0", s.WinRate)
	}
	if !approxEq(s.StartEquity, 1000) {
		t.Errorf("start_equity: got %v, want 1000", s.StartEquity)
	}
	if !approxEq(s.EndEquity, 1000) {
		t.Errorf("end_equity: got %v, want 1000", s.EndEquity)
	}
}

func TestCompute_DefaultEquity(t *testing.T) {
	t.Parallel()
	// initialEquity <= 0 collapses to the 1000 default.
	fills := []Fill{
		{Side: "buy", Quantity: 1, Price: 100},
		{Side: "sell", Quantity: 1, Price: 100},
	}
	s := Compute(fills, 0)
	if !approxEq(s.StartEquity, 1000) {
		t.Errorf("default start_equity: got %v, want 1000", s.StartEquity)
	}
	// Round-trip on 100 -> 100 is 0 net PnL; equity stays 1000.
	if !approxEq(s.TotalReturn, 0) {
		t.Errorf("total_return: got %v, want 0", s.TotalReturn)
	}
}

func TestCompute_MultipleTrades(t *testing.T) {
	t.Parallel()
	// Two round-trips: +10 then -20 -> net -10 from 1000 => -0.01.
	fills := []Fill{
		{Side: "buy", Quantity: 1, Price: 100},
		{Side: "sell", Quantity: 1, Price: 110}, // win
		{Side: "buy", Quantity: 1, Price: 200},
		{Side: "sell", Quantity: 1, Price: 180}, // loss
	}
	s := Compute(fills, 1000)
	if !approxEq(s.TotalReturn, -0.01) {
		t.Errorf("total_return: got %v, want -0.01", s.TotalReturn)
	}
	if s.TradeCount != 4 {
		t.Errorf("trade_count: got %d, want 4", s.TradeCount)
	}
	// Two buys, one of which closed at a profit => 0.5.
	if !approxEq(s.WinRate, 0.5) {
		t.Errorf("win_rate: got %v, want 0.5", s.WinRate)
	}
	if !approxEq(s.EndEquity, 990) {
		t.Errorf("end_equity: got %v, want 990", s.EndEquity)
	}
	// Drawdown walk: 1000 -> 900 -> 1010 (peak) -> 810 (trough) -> 990.
	// Worst peak-to-trough is (1010 - 810) / 1010.
	if !approxEq(s.MaxDrawdown, 200.0/1010.0) {
		t.Errorf("max_drawdown: got %v, want %v", s.MaxDrawdown, 200.0/1010.0)
	}
}
