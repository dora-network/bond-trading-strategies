package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type pingFunc func(context.Context) error

type healthChecker struct {
	mu               sync.RWMutex
	startedAt        time.Time
	startupGrace     time.Duration
	staleAfter       time.Duration
	candlesEnabled   bool
	dbPingEnabled    bool
	dbPingTimeout    time.Duration
	dbPing           pingFunc
	lastPriceStream  time.Time
	lastPriceWrite   time.Time
	lastCandleStream time.Time
	lastCandleWrite  time.Time
}

type healthStatus struct {
	OK      bool     `json:"ok"`
	Status  string   `json:"status"`
	Reasons []string `json:"reasons,omitempty"`
	//nolint:tagliatelle
	Checked time.Time `json:"checked_at"`
	//nolint:tagliatelle
	Grace bool `json:"startup_grace"`
	//nolint:tagliatelle
	GraceEnd time.Time `json:"startup_grace_until"`
}

func newHealthChecker(now time.Time, startupGrace, staleAfter time.Duration, candlesEnabled, dbPingEnabled bool, dbPingTimeout time.Duration, dbPing pingFunc) *healthChecker { //nolint:lll
	return &healthChecker{
		startedAt:      now,
		startupGrace:   startupGrace,
		staleAfter:     staleAfter,
		candlesEnabled: candlesEnabled,
		dbPingEnabled:  dbPingEnabled,
		dbPingTimeout:  dbPingTimeout,
		dbPing:         dbPing,
	}
}

func (h *healthChecker) markPriceStream(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastPriceStream = now
}

func (h *healthChecker) markPriceWrite(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastPriceWrite = now
}

func (h *healthChecker) markCandleStream(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastCandleStream = now
}

func (h *healthChecker) markCandleWrite(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastCandleWrite = now
}

func (h *healthChecker) status(ctx context.Context, now time.Time) healthStatus {
	h.mu.RLock()
	startedAt := h.startedAt
	startupGrace := h.startupGrace
	staleAfter := h.staleAfter
	candlesEnabled := h.candlesEnabled
	dbPingEnabled := h.dbPingEnabled
	dbPingTimeout := h.dbPingTimeout
	dbPing := h.dbPing
	lastPriceStream := h.lastPriceStream
	lastPriceWrite := h.lastPriceWrite
	lastCandleStream := h.lastCandleStream
	lastCandleWrite := h.lastCandleWrite
	h.mu.RUnlock()

	status := healthStatus{
		OK:       true,
		Status:   "ok",
		Checked:  now,
		Grace:    now.Before(startedAt.Add(startupGrace)),
		GraceEnd: startedAt.Add(startupGrace),
	}

	if status.Grace {
		if dbPingEnabled {
			if err := h.runDBPing(ctx, dbPingTimeout, dbPing); err != nil {
				status.OK = false
				status.Status = "unhealthy"
				status.Reasons = append(status.Reasons, "database ping failed")
			}
		}
		return status
	}

	if isStale(now, lastPriceStream, staleAfter) {
		status.OK = false
		status.Reasons = append(status.Reasons, "price stream is stale")
	}
	if isStale(now, lastPriceWrite, staleAfter) {
		status.OK = false
		status.Reasons = append(status.Reasons, "price writes are stale")
	}
	if candlesEnabled {
		if isStale(now, lastCandleStream, staleAfter) {
			status.OK = false
			status.Reasons = append(status.Reasons, "candle stream is stale")
		}
		if isStale(now, lastCandleWrite, staleAfter) {
			status.OK = false
			status.Reasons = append(status.Reasons, "candle writes are stale")
		}
	}
	if dbPingEnabled {
		if err := h.runDBPing(ctx, dbPingTimeout, dbPing); err != nil {
			status.OK = false
			status.Reasons = append(status.Reasons, "database ping failed")
		}
	}

	if !status.OK {
		status.Status = "unhealthy"
	}

	return status
}

func (h *healthChecker) runDBPing(ctx context.Context, timeout time.Duration, ping pingFunc) error {
	if ping == nil {
		return nil
	}

	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return ping(pingCtx)
}

func isStale(now, last time.Time, staleAfter time.Duration) bool {
	if last.IsZero() {
		return true
	}
	return now.Sub(last) > staleAfter
}

func newHealthHandler(checker *healthChecker, now func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := checker.status(r.Context(), now())
		w.Header().Set("Content-Type", "application/json")
		if !status.OK {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
}
