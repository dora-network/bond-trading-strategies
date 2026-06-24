package http_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggingResponseWriter_CapturesStatusAndBytes(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)

	lw.WriteHeader(http.StatusTeapot)
	n, err := lw.Write([]byte("hello"))

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, http.StatusTeapot, lw.Status())
	assert.Equal(t, 5, lw.Bytes())
	assert.Equal(t, "", lw.Err())
}

func TestLoggingResponseWriter_DefaultStatusIs200(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	_, _ = lw.Write([]byte("body"))

	assert.Equal(t, http.StatusOK, lw.Status())
	assert.Equal(t, 4, lw.Bytes())
	assert.Equal(t, "", lw.Err())
}

func TestLoggingResponseWriter_WithError(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	lw.WithError("missing Authorization header")
	lw.WithError("overwritten")

	assert.Equal(t, "overwritten", lw.Err())
}

// fakeHijacker records whether Hijack was called and returns a
// canned (nil, nil, nil) response.
type fakeHijacker struct {
	httptest.ResponseRecorder
	hijacked bool
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	f.hijacked = true
	return nil, nil, nil
}

func TestLoggingResponseWriter_DelegatesHijack(t *testing.T) {
	t.Parallel()

	fake := &fakeHijacker{}
	lw := strategyhttp.NewLoggingResponseWriter(fake)
	_, _, err := lw.Hijack()

	require.NoError(t, err)
	assert.True(t, fake.hijacked, "Hijack must delegate to the underlying writer")
}

func TestLoggingResponseWriter_HijackFailsWithoutHijacker(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	_, _, err := lw.Hijack()

	require.Error(t, err)
}

// TestLoggingResponseWriter_AccumulatesMultipleWrites asserts that
// successive Write calls sum their byte counts. Guards against an
// off-by-one in the counter (e.g. assignment instead of addition).
func TestLoggingResponseWriter_AccumulatesMultipleWrites(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	lw := strategyhttp.NewLoggingResponseWriter(rec)
	_, _ = lw.Write([]byte("hello"))
	_, _ = lw.Write([]byte("world"))

	assert.Equal(t, 10, lw.Bytes())
}

// errorRecorder is a minimal http.ResponseWriter that returns the
// configured (n, err) from Write. Header is provided by embedding
// httptest.ResponseRecorder to avoid hand-rolling the map.
type errorRecorder struct {
	httptest.ResponseRecorder
	n   int
	err error
}

func (e *errorRecorder) Write(b []byte) (int, error) {
	return e.n, e.err
}

// TestLoggingResponseWriter_CountsPartialWrite asserts that the
// wrapper updates its byte count even when the underlying writer
// reports a partial write with an error. This matches the net/http
// contract: partial writes must be counted, and the error must be
// propagated to the caller.
func TestLoggingResponseWriter_CountsPartialWrite(t *testing.T) {
	t.Parallel()

	rec := &errorRecorder{n: 3, err: io.ErrShortWrite}
	lw := strategyhttp.NewLoggingResponseWriter(rec)

	n, err := lw.Write([]byte("hello"))

	assert.Equal(t, 3, n, "the partial-write byte count must be returned to the caller")
	assert.ErrorIs(t, err, io.ErrShortWrite)
	assert.Equal(t, 3, lw.Bytes(), "Bytes() must reflect the partial write, not the input length")
}
