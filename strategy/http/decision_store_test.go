package http_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
)

func TestCursor_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	when := time.Date(2026, 1, 15, 10, 30, 0, 123456789, time.UTC)
	original := &strategyhttp.Cursor{Time: when, Seq: 42, Version: 1}

	encoded := original.Encode()
	require.NotEmpty(t, encoded)
	assert.False(t, strings.ContainsAny(encoded, " \t\n"), "cursor must be URL-safe")

	decoded, err := strategyhttp.DecodeCursor(encoded)
	require.NoError(t, err)
	assert.True(t, original.Time.Equal(decoded.Time), "time round-trip: want %s got %s", original.Time, decoded.Time)
	assert.Equal(t, original.Seq, decoded.Seq)
}

func TestDecodeCursor_RejectsBadInput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"empty":                  "",
		"not base64":             "@@@not-base64@@@",
		"valid base64, bad json": "bm90LWpzb24=",               // "not-json"
		"unknown version":        "eyJ2IjoyLCJ0IjoxLCJzIjoxfQ", // {"v":2,"t":1,"s":1}
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := strategyhttp.DecodeCursor(raw)
			assert.Error(t, err)
		})
	}
}
