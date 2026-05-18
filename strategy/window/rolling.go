package window

import "github.com/govalues/decimal"

// Rolling maintains a fixed-size circular buffer of decimal.Decimal values and
// provides efficient rolling mean and standard deviation.
//
// All arithmetic uses Welford's online algorithm so mean and variance are
// updated in O(1) per observation rather than recomputing from scratch.
type Rolling struct {
	size  int
	buf   []decimal.Decimal
	head  int // index of the oldest element (next write position)
	count int // number of elements currently in the buffer
	mean  decimal.Decimal
	m2    decimal.Decimal // sum of squared deviations from the mean (Welford)
}

// NewRollingWindow creates an empty window with the given lookback size.
// size must be ≥ 2 (required for standard deviation to be defined).
func NewRollingWindow(size int) *Rolling {
	if size < 2 { //nolint:mnd
		size = 2
	}
	return &Rolling{
		size: size,
		buf:  make([]decimal.Decimal, size),
	}
}

// Add inserts a new value, evicting the oldest value once the buffer is full.
// It updates the rolling mean and Welford M2 incrementally.
func (w *Rolling) Add(value decimal.Decimal) error {
	if w.count < w.size {
		// Buffer not yet full — straightforward online update.
		w.count++
		delta, err := value.Sub(w.mean)
		if err != nil {
			return err
		}
		countDec := decimal.MustNew(int64(w.count), 0)
		inc, err := delta.Quo(countDec)
		if err != nil {
			return err
		}
		w.mean, err = w.mean.Add(inc)
		if err != nil {
			return err
		}
		diff, err := value.Sub(w.mean)
		if err != nil {
			return err
		}
		prod, err := delta.Mul(diff)
		if err != nil {
			return err
		}
		w.m2, err = w.m2.Add(prod)
		if err != nil {
			return err
		}
		w.buf[w.head] = value
		w.head = (w.head + 1) % w.size
		return nil
	}

	// Buffer is full: remove the oldest value and add the new one.
	oldest := w.buf[w.head]
	oldMean := w.mean

	// Welford downdate for the value being removed.
	diff, err := value.Sub(oldest)
	if err != nil {
		return err
	}
	sizeDec := decimal.MustNew(int64(w.size), 0)
	inc, err := diff.Quo(sizeDec)
	if err != nil {
		return err
	}
	w.mean, err = w.mean.Add(inc)
	if err != nil {
		return err
	}
	valueMean, err := value.Sub(w.mean)
	if err != nil {
		return err
	}
	oldestOldMean, err := oldest.Sub(oldMean)
	if err != nil {
		return err
	}
	term2, err := valueMean.Add(oldestOldMean)
	if err != nil {
		return err
	}
	prod, err := diff.Mul(term2)
	if err != nil {
		return err
	}
	w.m2, err = w.m2.Add(prod)
	if err != nil {
		return err
	}

	// Guard against numerical drift below zero.
	if w.m2.IsNeg() {
		w.m2 = decimal.Zero
	}

	w.buf[w.head] = value
	w.head = (w.head + 1) % w.size
	return nil
}

// Ready reports whether the window contains enough data (i.e. is full) to
// produce statistically meaningful signals.
func (w *Rolling) Ready() bool {
	return w.count == w.size
}

// Mean returns the rolling mean of the values in the window.
// Returns 0 if the window is empty.
func (w *Rolling) Mean() decimal.Decimal {
	return w.mean
}

// Variance returns the sample variance (divides by n−1).
// Returns 0 if fewer than 2 values are present.
func (w *Rolling) Variance() (decimal.Decimal, error) {
	if w.count < 2 { //nolint:mnd
		return decimal.Zero, nil
	}
	countMinus1 := decimal.MustNew(int64(w.count-1), 0)
	return w.m2.Quo(countMinus1)
}

// StdDev returns the sample standard deviation.
func (w *Rolling) StdDev() (decimal.Decimal, error) {
	v, err := w.Variance()
	if err != nil {
		return decimal.Zero, err
	}
	return v.Sqrt()
}

// ZScore returns the z-score of the given value relative to the window's
// current rolling mean and standard deviation.
//
// If the standard deviation is below the given minStdDev floor (or zero),
// ZScore returns 0 to avoid division by near-zero noise.
func (w *Rolling) ZScore(value, minStdDev decimal.Decimal) (decimal.Decimal, error) {
	sd, err := w.StdDev()
	if err != nil {
		return decimal.Zero, err
	}
	if sd.Less(minStdDev) {
		return decimal.Zero, nil
	}
	num, err := value.Sub(w.mean)
	if err != nil {
		return decimal.Zero, err
	}
	return num.Quo(sd)
}
