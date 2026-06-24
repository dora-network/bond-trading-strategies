package http

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// LoggingResponseWriter wraps an http.ResponseWriter to capture the
// status code, the number of bytes written, and an optional error
// string attached via WithError. The middleware reads these fields
// after the handler returns to emit the request log record.
//
// If the wrapped writer implements http.Hijacker (required for
// WebSocket upgrades), Hijack delegates to it; otherwise Hijack
// returns an error. See cmd/strategy-server/main.go for context.
type LoggingResponseWriter struct {
	w         http.ResponseWriter
	status    int
	statusSet bool
	bytes     int
	errMsg    string
}

// NewLoggingResponseWriter wraps w. The default captured status is
// 200; the first call to WriteHeader sets it to the actual code.
func NewLoggingResponseWriter(w http.ResponseWriter) *LoggingResponseWriter {
	return &LoggingResponseWriter{w: w, status: http.StatusOK}
}

// Header exposes the wrapped writer's Header.
func (l *LoggingResponseWriter) Header() http.Header { return l.w.Header() }

// WriteHeader records the status code and forwards to the wrapped
// writer. Subsequent calls are passed through unchanged (matching
// net/http's behaviour).
func (l *LoggingResponseWriter) WriteHeader(status int) {
	if !l.statusSet {
		l.status = status
		l.statusSet = true
	}
	l.w.WriteHeader(status)
}

// Write forwards to the wrapped writer and counts the bytes written.
func (l *LoggingResponseWriter) Write(b []byte) (int, error) {
	n, err := l.w.Write(b)
	l.bytes += n
	return n, err
}

// WithError stores an error string that the log middleware will
// include as the "err" field on the request log record. It is
// typically called by writeError right after writing the response.
// Safe to call multiple times; the last value wins.
func (l *LoggingResponseWriter) WithError(msg string) { l.errMsg = msg }

// Status returns the status code that was written. If WriteHeader
// was never called, this is 200 (the Go default for a successful
// response).
func (l *LoggingResponseWriter) Status() int { return l.status }

// Bytes returns the total number of bytes written to the body.
func (l *LoggingResponseWriter) Bytes() int { return l.bytes }

// Err returns the error string attached via WithError, or "" if
// none was attached.
func (l *LoggingResponseWriter) Err() string { return l.errMsg }

// Hijack implements http.Hijacker. The notifications package and
// websocket.Accept require it; without delegation, the WebSocket
// upgrade path would fail. If the wrapped writer does not implement
// http.Hijacker, this returns an error.
func (l *LoggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := l.w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
}
