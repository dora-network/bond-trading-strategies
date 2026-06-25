package http

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseLogJSON decodes a single JSON line emitted by slog into a
// map. Mirrors the helper in logging_test.go (kept separate so the
// external and internal test files can evolve independently).
func parseLogJSON(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

// TestWriteError_AttachesErrorToWrapper ensures writeError stashes
// the message on the logging wrapper so the middleware can include
// it in the request log record. This covers the wiring between
// writeError's WithError call and the middleware's Err() read.
func TestWriteError_AttachesErrorToWrapper(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := RequestLog(logger, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusBadRequest, "config: invalid field")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/backtests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	log := parseLogJSON(t, &buf)
	assert.Equal(t, "WARN", log["level"])
	assert.Equal(t, "config: invalid field", log["err"])
}
