package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// SSEWriter frames Server-Sent Events onto an HTTP response.
type SSEWriter struct {
	w  http.ResponseWriter
	fl http.Flusher
}

// NewSSEWriter sets SSE headers and asserts the response supports flushing.
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	fl, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	fl.Flush()
	return &SSEWriter{w: w, fl: fl}, nil
}

// Write emits one SSE event: an `event:` line, a JSON `data:` line, a blank.
func (s *SSEWriter) Write(event string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
		return err
	}
	s.fl.Flush()
	return nil
}
