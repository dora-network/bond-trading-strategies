package http

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
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

// WriteHeader records the first status code and forwards it to the
// wrapped writer. Subsequent calls are ignored, matching net/http's
// first-call-wins behavior.
func (l *LoggingResponseWriter) WriteHeader(status int) {
	if l.statusSet {
		return
	}
	l.status = status
	l.statusSet = true
	l.w.WriteHeader(status)
}

// Write forwards to the wrapped writer and counts the bytes written.
func (l *LoggingResponseWriter) Write(b []byte) (int, error) {
	if !l.statusSet {
		l.status = http.StatusOK
		l.statusSet = true
	}
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

// RequestLog returns a middleware that records one slog record per
// request. The record carries method, route (from r.Pattern, falling
// back to r.URL.Path when the request was not matched by a mux
// pattern), path, status, duration_ms, bytes, user_id, and an
// optional err string attached via LoggingResponseWriter.WithError.
//
// Paths in exempt produce no log record. The default behavior
// (exempt == nil) logs every request.
//
// Log levels:
//   - status < 400: slog.LevelInfo
//   - 400 ≤ status < 500: slog.LevelWarn
//   - status ≥ 500: slog.LevelError
//
// 401 responses from requireAuth are logged at slog.LevelWarn with
// user_id="unauthenticated" and err set to the body the handler
// wrote.
//
// The middleware reads only r.Method, r.URL.Path, r.Pattern, and the
// resolved DORA user id from the request context. It never reads
// the Authorization header or any other request body. The user_id
// helper (doraUserIDFromContext) is defined in auth.go and returns
// the empty string when authentication has not run.
func RequestLog(log *slog.Logger, exempt map[string]struct{}) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := exempt[r.URL.Path]; ok {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			lw := NewLoggingResponseWriter(w)
			next.ServeHTTP(lw, r)

			status := lw.Status()
			level := slog.LevelInfo
			switch {
			case status >= http.StatusInternalServerError:
				level = slog.LevelError
			case status >= http.StatusBadRequest:
				level = slog.LevelWarn
			}

			userID, _ := doraUserIDFromContext(r.Context())
			if userID == "" {
				userID = "unauthenticated"
			}

			route := r.Pattern
			if route == "" {
				route = r.URL.Path
			}

			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("route", route),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
				slog.Int("bytes", lw.Bytes()),
				slog.String("user_id", userID),
			}
			if errMsg := lw.Err(); errMsg != "" {
				attrs = append(attrs, slog.String("err", errMsg))
			}

			log.LogAttrs(r.Context(), level, "request", attrs...)
		})
	}
}
