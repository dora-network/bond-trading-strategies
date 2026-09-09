package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSSEWriter_WriteEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	sse, err := NewSSEWriter(rec)
	if err != nil {
		t.Fatalf("NewSSEWriter: %v", err)
	}
	if err := sse.Write("delta", map[string]string{"text": "hi"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sse.Write("done", map[string]string{"reason": ""}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "event: delta\n") {
		t.Errorf("body missing delta event:\n%s", body)
	}
	if !strings.Contains(body, `data: {"text":"hi"}`) {
		t.Errorf("body missing delta data:\n%s", body)
	}
	if !strings.Contains(body, "event: done\n") {
		t.Errorf("body missing done event:\n%s", body)
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("Content-Type: want text/event-stream, got %q", rec.Header().Get("Content-Type"))
	}
}
