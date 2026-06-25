package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseLog decodes a single JSON line emitted by slog's JSONHandler
// into a map. The middleware emits one record per request via
// log.LogAttrs, which the JSONHandler serializes as a single line.
func parseLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

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

type writeHeaderRecorder struct {
	header http.Header
	codes  []int
}

func (w *writeHeaderRecorder) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *writeHeaderRecorder) Write(b []byte) (int, error) { return len(b), nil }

func (w *writeHeaderRecorder) WriteHeader(status int) {
	w.codes = append(w.codes, status)
}

func TestLoggingResponseWriter_IgnoresSubsequentWriteHeader(t *testing.T) {
	t.Parallel()

	rec := &writeHeaderRecorder{}
	lw := strategyhttp.NewLoggingResponseWriter(rec)

	lw.WriteHeader(http.StatusCreated)
	lw.WriteHeader(http.StatusInternalServerError)

	assert.Equal(t, http.StatusCreated, lw.Status())
	assert.Equal(t, []int{http.StatusCreated}, rec.codes)
}

func TestLoggingResponseWriter_WriteCommitsDefaultStatus(t *testing.T) {
	t.Parallel()

	rec := &writeHeaderRecorder{}
	lw := strategyhttp.NewLoggingResponseWriter(rec)

	n, err := lw.Write([]byte("ok"))
	lw.WriteHeader(http.StatusInternalServerError)

	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, http.StatusOK, lw.Status())
	assert.Equal(t, 2, lw.Bytes())
	assert.Empty(t, rec.codes, "WriteHeader after Write must not be forwarded")
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

func TestRequestLog_InfoOnSuccess(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/strategies", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "request", log["msg"])
	assert.Equal(t, "INFO", log["level"])
	assert.Equal(t, "GET", log["method"])
	assert.Equal(t, "/v1/strategies", log["path"])
	assert.Equal(t, float64(200), log["status"])
	assert.Equal(t, "/v1/strategies", log["route"], "no mux pattern → route is the path")
	assert.GreaterOrEqual(t, log["duration_ms"], float64(0))
	assert.Equal(t, float64(11), log["bytes"])
	_, hasErr := log["err"]
	assert.False(t, hasErr, "err must be omitted when not set")
}

func TestRequestLog_WarnOn4xx(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// RequestLog already wrapped w in a *LoggingResponseWriter.
		// Type-assert it to call WithError; do NOT wrap again, the
		// inner wrapper's state would be invisible to the middleware.
		lw, _ := w.(*strategyhttp.LoggingResponseWriter)
		lw.WithError("bad input")
		lw.WriteHeader(http.StatusBadRequest)
		_, _ = lw.Write([]byte(`{"error":"bad input"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, float64(400), log["status"])
	assert.Equal(t, "bad input", log["err"])
}

func TestRequestLog_ErrorOn5xx(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lw, _ := w.(*strategyhttp.LoggingResponseWriter)
		lw.WithError("boom")
		lw.WriteHeader(http.StatusInternalServerError)
		_, _ = lw.Write([]byte(`{"error":"boom"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "ERROR", log["level"])
	assert.Equal(t, float64(500), log["status"])
	assert.Equal(t, "boom", log["err"])
}

func TestRequestLog_UnauthenticatedWhenUserIDMissing(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorised"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, float64(401), log["status"])
	assert.Equal(t, "unauthenticated", log["user_id"])
}

func TestRequestLog_UsesPatternWhenSet(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/backtests/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := strategyhttp.RequestLog(logger, nil)(mux)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests/abc", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "/v1/backtests/{id}", log["route"])
	assert.Equal(t, "/v1/backtests/abc", log["path"])
}

func TestRequestLog_FallsBackToPathWithoutPattern(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 404 from a bare http.ServeMux with no matching route.
	mux := http.NewServeMux()
	wrapped := strategyhttp.RequestLog(logger, nil)(mux)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/unknown", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	assert.Equal(t, "/unknown", log["route"])
	assert.Equal(t, "/unknown", log["path"])
	assert.Equal(t, float64(404), log["status"])
}

func TestRequestLog_SkipsExemptPaths(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	exempt := map[string]struct{}{
		"/healthz":    {},
		"/v1/openapi": {},
	}
	handler := strategyhttp.RequestLog(logger, exempt)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/healthz", "/v1/openapi"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
	}

	assert.Empty(t, buf.String(), "exempt paths must not produce a log record")
}

func TestRequestLog_NeverLogsCredentials(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const secret = "secret-abc-123"
	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorised"}`))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/backtests", nil)
	req.Header.Set("Authorization", "ApiKey "+secret)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	out := buf.String()
	// The secret must not appear in any form. The check is
	// intentionally strict: any substring of the secret would
	// indicate the key was logged, even partially.
	assert.NotContains(t, out, secret)
	assert.NotContains(t, out, "abc-123")
	assert.NotContains(t, out, "abc")
	assert.NotContains(t, out, "123")
	assert.NotContains(t, out, "ApiKey")
	assert.NotContains(t, out, "Bearer")
}

func TestRequestLog_NoErrFieldWhenAbsent(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := strategyhttp.RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLog(t, &buf)
	_, hasErr := log["err"]
	assert.False(t, hasErr)
}
