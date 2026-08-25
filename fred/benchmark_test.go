package fred

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseBenchmarkTenor covers the parser against known good inputs,
// known aliases, and known bad inputs.
func TestParseBenchmarkTenor(t *testing.T) {
	t.Run("canonical codes", func(t *testing.T) {
		for code, want := range map[string]Tenor{
			"1M":  Tenor1Month,
			"3M":  Tenor3Month,
			"6M":  Tenor6Month,
			"1Y":  Tenor1Year,
			"2Y":  Tenor2Year,
			"3Y":  Tenor3Year,
			"5Y":  Tenor5Year,
			"7Y":  Tenor7Year,
			"10Y": Tenor10Year,
			"20Y": Tenor20Year,
			"30Y": Tenor30Year,
		} {
			got, err := ParseBenchmarkTenor(code)
			require.NoError(t, err, "code %q should parse", code)
			assert.Equal(t, want, got, "code %q mapped to %v, want %v", code, got, want)
		}
	})

	t.Run("aliases", func(t *testing.T) {
		for _, input := range []string{"10YR", "2YEAR", "1MO", "3MON", "6MONTH", "5YR", "30YR"} {
			_, err := ParseBenchmarkTenor(input)
			assert.NoError(t, err, "alias %q should parse", input)
		}
	})

	t.Run("normalized inputs", func(t *testing.T) {
		// Normalization: ToUpper + strip whitespace/hyphens/underscores
		// + drop trailing S.
		for _, input := range []string{" 2 year ", "1-months", "10YR", "10_yr", " 10 YEARS "} {
			got, err := ParseBenchmarkTenor(input)
			// "10 YEARS" normalizes to "10YEAR" -> 10YR alias -> Tenor10Year.
			// " 2 year " normalizes to "2YEAR" -> 2YR alias -> Tenor2Year.
			// "1-months" normalizes to "1MONTH" -> 1MONTH alias -> Tenor1Month.
			// "10_yr" -> "10YR" -> Tenor10Year.
			// "10YR" -> direct.
			assert.NoError(t, err, "normalized input %q should parse", input)
			_ = got
		}
	})

	t.Run("unknown input returns error", func(t *testing.T) {
		for _, input := range []string{
			"abc", "99Y", "", "ten", "1.5Y", "1XYZ",
		} {
			_, err := ParseBenchmarkTenor(input)
			assert.Error(t, err, "input %q should not parse", input)
		}
	})
}

// TestNormalizeTenor exhaustively covers the normalization rules:
// uppercasing, stripping whitespace/hyphens/underscores, and
// dropping a trailing "S".
func TestNormalizeTenor(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Uppercasing.
		{"2y", "2Y"},
		{"2year", "2YEAR"},
		// Whitespace stripped.
		{" 2Y", "2Y"},
		{"2 Y ", "2Y"},
		{"  2 YEAR  ", "2YEAR"},
		// Hyphens/underscores stripped.
		{"2-Y", "2Y"},
		{"2_Y", "2Y"},
		{"2-YEAR", "2YEAR"},
		{"10_YR", "10YR"},
		// Trailing "S" dropped - matches "10YEARS" / "1MONTHS" etc.
		{"2YEARS", "2YEAR"},
		{"1MONTHS", "1MONTH"},
		{"6MONTHS", "6MONTH"},
		// Empty stays empty (after trim).
		{"", ""},
		{"   ", ""},
	}
	for _, tc := range cases {
		got := NormalizeTenor(tc.in)
		assert.Equal(t, tc.want, got, "NormalizeTenor(%q) = %q, want %q", tc.in, got, tc.want)
	}
}

// TestNormalizeDate pins the contract: UTC midnight of the input's
// calendar date, regardless of the input's time-of-day or timezone.
// This is what gets used to key cached daily FRED yields against tick
// timestamps, so a wrong answer silently miscomputes spreads.
func TestNormalizeDate(t *testing.T) {
	// In UTC: normalize to the same UTC midnight.
	in := time.Date(2024, 8, 24, 15, 30, 45, 123, time.UTC)
	got := NormalizeDate(in)
	assert.Equal(t, time.Date(2024, 8, 24, 0, 0, 0, 0, time.UTC), got)

	// In another timezone: still UTC midnight of the calendar date in UTC.
	// 2024-08-25 01:00 UTC = 2024-08-24 21:00 EDT
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	in = time.Date(2024, 8, 25, 9, 0, 0, 0, tokyo) // 2024-08-25 00:00 UTC
	got = NormalizeDate(in)
	assert.Equal(t, time.Date(2024, 8, 25, 0, 0, 0, 0, time.UTC), got)

	// Edge: midnight input still gives midnight (no off-by-one).
	in = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	got = NormalizeDate(in)
	assert.Equal(t, in, got)

	// Edge: end of day still gives that calendar date.
	in = time.Date(2024, 12, 31, 23, 59, 59, 999, time.UTC)
	got = NormalizeDate(in)
	assert.Equal(t, time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), got)
}

// TestSupportedBenchmarkTenors verifies the list shape: 11 entries
// spanning 1M..30Y, sorted ascending by tenor, each with a non-empty
// Code and at least one alias. Catches accidentally edited table.
func TestSupportedBenchmarkTenors(t *testing.T) {
	list := SupportedBenchmarkTenors()
	require.Len(t, list, 11, "expected 11 benchmark tenors (1M..30Y)")

	prevValue := Tenor(-1)
	for i, bt := range list {
		assert.NotEmpty(t, bt.Code, "index %d: Code must be non-empty", i)
		assert.NotEmpty(t, bt.Description, "index %d: Description must be non-empty", i)
		assert.NotEmpty(t, bt.Aliases, "index %d: Aliases must be non-empty", i)
		assert.Greater(t, float64(bt.Value), float64(prevValue),
			"index %d (%s): list must be sorted ascending by tenor value", i, bt.Code)
		prevValue = bt.Value
	}

	// Spot-check the first and last entries.
	assert.Equal(t, "1M", list[0].Code)
	assert.Equal(t, Tenor1Month, list[0].Value)
	assert.Equal(t, "30Y", list[len(list)-1].Code)
	assert.Equal(t, Tenor30Year, list[len(list)-1].Value)
}
