package vwap

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/govalues/decimal"
)

const hoursPerDay = 24

// Schedule is the VWAP bucket schedule: the planned quantity for
// each bucket index from 0..N-1.
type Schedule struct {
	Buckets []decimal.Decimal
}

// Total returns the sum of all bucket quantities (should match
// TotalAmount for a freshly-computed schedule before any fills).
func (s Schedule) Total() decimal.Decimal {
	total := decimal.Zero
	for _, b := range s.Buckets {
		total, _ = total.Add(b)
	}
	return total
}

// BuildSchedule computes the VWAP schedule from historical trade volume.
//
// Algorithm:
//  1. Stream historical trades for the same order book over the
//     last WindowDays.
//  2. Bucket each trade by its time-of-day (minutes since midnight
//     / BucketMinutes).
//  3. Mean daily volume per bucket = sum / WindowDays.
//  4. For each bucket in [StartTime, EndTime], allocate TotalAmount
//     proportional to its share of total ADV.
//
// If no historical trades are available, the schedule falls back to
// an even distribution across buckets.
func BuildSchedule(
	ctx context.Context,
	store TradeVolumeStore,
	orderBookID uuid.UUID,
	cfg Config,
) (Schedule, error) {
	buckets := cfg.NumBuckets()
	if buckets == 0 {
		return Schedule{}, fmt.Errorf("vwap: no buckets in execution window")
	}

	bucketSize := time.Duration(cfg.BucketMinutes) * time.Minute
	now := time.Now().UTC()
	historyStart := now.AddDate(0, 0, -cfg.WindowDays)

	advVolumes := aggregateADVByBucket(ctx, store, orderBookID, historyStart, now, cfg.BucketMinutes)

	totalADV := decimal.Zero
	for i := range buckets {
		bucketStart := cfg.StartTime.Add(time.Duration(i) * bucketSize)
		idx := minutesSinceMidnight(bucketStart) / cfg.BucketMinutes
		vol := advVolumes[idx]
		totalADV, _ = totalADV.Add(vol)
	}

	out := make([]decimal.Decimal, buckets)
	if totalADV.IsZero() {
		// Even distribution. Floor each bucket so the last one
		// absorbs the rounding remainder and the total equals
		// TotalAmount exactly.
		perBucket, err := cfg.TotalAmount.Quo(decimal.MustNew(int64(buckets), 0))
		if err != nil {
			return Schedule{}, fmt.Errorf("vwap: even distribution: %w", err)
		}
		floored := perBucket.Floor(0)
		for i := 0; i < buckets-1; i++ {
			out[i] = floored
		}
		assigned := decimal.Zero
		for i := 0; i < buckets-1; i++ {
			assigned, _ = assigned.Add(out[i])
		}
		out[buckets-1], _ = cfg.TotalAmount.Sub(assigned)
		return Schedule{Buckets: out}, nil
	}

	scale, err := cfg.TotalAmount.Quo(totalADV)
	if err != nil {
		return Schedule{}, fmt.Errorf("vwap: scale factor: %w", err)
	}
	for i := range buckets {
		bucketStart := cfg.StartTime.Add(time.Duration(i) * bucketSize)
		idx := minutesSinceMidnight(bucketStart) / cfg.BucketMinutes
		vol := advVolumes[idx]
		amt, err := vol.Mul(scale)
		if err != nil {
			return Schedule{}, fmt.Errorf("vwap: bucket %d: %w", i, err)
		}
		// Floor each bucket so the last one absorbs the rounding
		// remainder and the total matches TotalAmount exactly.
		out[i] = amt.Floor(0)
	}
	assigned := decimal.Zero
	for i := 0; i < buckets-1; i++ {
		assigned, _ = assigned.Add(out[i])
	}
	out[buckets-1], _ = cfg.TotalAmount.Sub(assigned)
	return Schedule{Buckets: out}, nil
}

// aggregateADVByBucket streams historical trades and returns the
// mean daily volume per time-of-day bucket.
func aggregateADVByBucket(
	ctx context.Context,
	store TradeVolumeStore,
	orderBookID uuid.UUID,
	start, end time.Time,
	bucketMinutes int,
) map[int]decimal.Decimal {
	volumes := make(map[int]decimal.Decimal, 24*60/bucketMinutes)
	trades, errs := store.StreamTrades(ctx, orderBookID, start, end)

	for trades != nil || errs != nil {
		select {
		case <-ctx.Done():
			return volumes
		case t, ok := <-trades:
			if !ok {
				trades = nil
				continue
			}
			idx := minutesSinceMidnight(t.Time.UTC()) / bucketMinutes
			cur, _ := volumes[idx].Add(t.Quantity)
			volumes[idx] = cur
		case e, ok := <-errs:
			if ok && e != nil {
				_ = e
			}
			errs = nil
		}
	}

	windowDays := int(end.Sub(start).Hours() / hoursPerDay)
	if windowDays <= 0 {
		windowDays = 1
	}
	divisor := decimal.MustNew(int64(windowDays), 0)
	for k, v := range volumes {
		volumes[k], _ = v.Quo(divisor)
	}
	return volumes
}

// minutesSinceMidnight returns the number of whole minutes from
// midnight UTC to t.
func minutesSinceMidnight(t time.Time) int {
	return t.Hour()*60 + t.Minute()
}
