package fred

import (
	"fmt"
	"strings"
	"time"
)

// BenchmarkTenor is a row in the supported benchmark yield tenor list:
// a human-readable code, a description, the underlying fred.Tenor, and
// the set of aliases the parser accepts (uppercased, with separators
// stripped).
type BenchmarkTenor struct {
	Code        string
	Description string
	Value       Tenor
	Aliases     []string
}

//nolint:gochecknoglobals // Package-level constant list of known benchmark tenors
var benchmarkTenors = []BenchmarkTenor{
	{Code: "1M", Description: "1 Month Treasury", Value: Tenor1Month, Aliases: []string{"1MO", "1MON", "1MONTH"}},
	{Code: "3M", Description: "3 Month Treasury", Value: Tenor3Month, Aliases: []string{"3MO", "3MON", "3MONTH"}},
	{Code: "6M", Description: "6 Month Treasury", Value: Tenor6Month, Aliases: []string{"6MO", "6MON", "6MONTH"}},
	{Code: "1Y", Description: "1 Year Treasury", Value: Tenor1Year, Aliases: []string{"1YR", "1YEAR"}},
	{Code: "2Y", Description: "2 Year Treasury", Value: Tenor2Year, Aliases: []string{"2YR", "2YEAR"}},
	{Code: "3Y", Description: "3 Year Treasury", Value: Tenor3Year, Aliases: []string{"3YR", "3YEAR"}},
	{Code: "5Y", Description: "5 Year Treasury", Value: Tenor5Year, Aliases: []string{"5YR", "5YEAR"}},
	{Code: "7Y", Description: "7 Year Treasury", Value: Tenor7Year, Aliases: []string{"7YR", "7YEAR"}},
	{Code: "10Y", Description: "10 Year Treasury", Value: Tenor10Year, Aliases: []string{"10YR", "10YEAR"}},
	{Code: "20Y", Description: "20 Year Treasury", Value: Tenor20Year, Aliases: []string{"20YR", "20YEAR"}},
	{Code: "30Y", Description: "30 Year Treasury", Value: Tenor30Year, Aliases: []string{"30YR", "30YEAR"}},
}

// SupportedBenchmarkTenors returns a copy of the known benchmark tenor
// list so callers can render configuration UIs.
func SupportedBenchmarkTenors() []BenchmarkTenor {
	return append([]BenchmarkTenor(nil), benchmarkTenors...)
}

// ParseBenchmarkTenor resolves a user-supplied tenor string (e.g. "2Y",
// "10YR", "1 MONTH") to a fred.Tenor. Returns an error for unknown
// inputs.
func ParseBenchmarkTenor(value string) (Tenor, error) {
	normalised := NormalizeTenor(value)
	for _, tenor := range benchmarkTenors {
		if normalised == tenor.Code {
			return tenor.Value, nil
		}
		for _, alias := range tenor.Aliases {
			if normalised == alias {
				return tenor.Value, nil
			}
		}
	}
	return 0, fmt.Errorf("unsupported tenor %q", value)
}

// NormalizeTenor uppercases, strips whitespace/hyphens/underscores, and
// drops a trailing "S" so user input like " 2 year " or "1-months"
// resolves consistently.
func NormalizeTenor(value string) string {
	normalised := strings.ToUpper(strings.TrimSpace(value))
	normalised = strings.ReplaceAll(normalised, "-", "")
	normalised = strings.ReplaceAll(normalised, "_", "")
	normalised = strings.ReplaceAll(normalised, " ", "")
	normalised = strings.TrimSuffix(normalised, "S")
	return normalised
}

// NormalizeDate returns the UTC midnight of the given timestamp's date.
// Used to key cached daily FRED yields against tick timestamps.
func NormalizeDate(ts time.Time) time.Time {
	year, month, day := ts.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
