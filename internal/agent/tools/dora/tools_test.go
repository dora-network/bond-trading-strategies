package dora

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newServerWithKeyCheck returns an httptest.Server that asserts the
// Authorization header carries the per-request API key, then dispatches h.
func newServerWithKeyCheck(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "ApiKey test-key" {
			t.Errorf("Authorization header = %q, want %q", got, "ApiKey test-key")
		}
		h(w, r)
	}))
}

// envelopeOK writes a minimal valid Dora envelope JSON body. Envelopes require
// a top-level "data" field and a "metadata" object carrying a status_code
// (the SDK unmarshals strictly and marks status_code required).
func envelopeOK(data string) string {
	return `{"data":` + data + `,"metadata":{"status_code":200,"trace_id":"","request_id":""}}`
}

// reqJSON marshals v to a json.RawMessage, failing the test on error.
func reqJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// assertEnvelope checks the marshalled output carries a data envelope.
func assertEnvelope(t *testing.T, out json.RawMessage) {
	t.Helper()
	if !strings.Contains(string(out), "data") {
		t.Errorf("output = %s, want a data envelope", string(out))
	}
}

func TestTool_ListAssets(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`[]`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_list_assets", reqJSON(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if gotPath != "/v1/assets" {
		t.Errorf("path = %q, want /v1/assets", gotPath)
	}
	assertEnvelope(t, out)
}

func TestTool_ListOrderBooks(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`[]`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_list_order_books", reqJSON(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if gotPath != "/v1/orderbooks" {
		t.Errorf("path = %q, want /v1/orderbooks", gotPath)
	}
	assertEnvelope(t, out)
}

func TestTool_GetOrderBook(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`null`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_order_book", reqJSON(t, map[string]any{"order_book_id": "ob-1"}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "/v1/orderbooks/ob-1/L2"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	assertEnvelope(t, out)
}

func TestTool_GetOrderBookStats(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`null`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_order_book_stats", reqJSON(t, map[string]any{"order_book_id": "ob-2"}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "/v1/orderbooks/ob-2/stats"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	assertEnvelope(t, out)
}

func TestTool_GetTrades(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`[]`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_trades", reqJSON(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if gotPath != "/v1/trades" {
		t.Errorf("path = %q, want /v1/trades", gotPath)
	}
	assertEnvelope(t, out)
}

func TestTool_GetPositions(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		// V2 envelope: data is AccountPortfolioResponseV2 with a real
		// Portfolio. The SDK is strict about data being populated
		// (it is a typed *struct, not interface{}), so we send a
		// minimal valid portfolio rather than null.
		_, _ = io.WriteString(w, envelopeOK(`{
			"portfolio":{
				"user_id":"u-1",
				"accounts":{},
				"net_stablecoin_equivalence":{}
			}
		}`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_positions", reqJSON(t, map[string]any{}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "/v2/ledger/accounts/self"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	assertEnvelope(t, out)
}

func TestTool_GetCandleData(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`[]`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_candle_data", reqJSON(t, map[string]any{
		"order_book_id": "ob-3",
		"start":         "2026-07-01T00:00:00Z",
		"end":           "2026-07-29T00:00:00Z",
		"resolution":    "1h",
	}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "/v1/charts/ob-3/candle"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	assertEnvelope(t, out)
}

func TestTool_GetCandleData_RequiresEnd(t *testing.T) {
	// The Dora API requires `end` on the candle endpoint; omitting it must be
	// rejected before any network call.
	h := NewHandlers("http://unused", "test-key")
	_, err := h.Invoke(t.Context(), "dora_get_candle_data", reqJSON(t, map[string]any{
		"order_book_id": "ob-3",
		"start":         "2026-07-01T00:00:00Z",
	}))
	if err == nil || !strings.Contains(err.Error(), "end is required") {
		t.Fatalf("want 'end is required' error, got %v", err)
	}
}

func TestTool_GetAssetYTM(t *testing.T) {
	var gotPath string
	srv := newServerWithKeyCheck(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, envelopeOK(`{"asset_id":"a-1","current_time":"2026-07-29T00:00:00Z","current_price":"100","yield_to_maturity":"0.05"}`))
	})
	defer srv.Close()

	h := NewHandlers(srv.URL, "test-key")
	out, err := h.Invoke(t.Context(), "dora_get_asset_ytm", reqJSON(t, map[string]any{"asset_id": "a-1"}))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "/v1/assets/a-1/ytm"; gotPath != want {
		t.Errorf("path = %q, want %q", gotPath, want)
	}
	if !strings.Contains(string(out), "yield_to_maturity") {
		t.Errorf("output = %s, want yield_to_maturity field", string(out))
	}
}

func TestTool_UnknownName(t *testing.T) {
	h := NewHandlers("http://unused", "test-key")
	_, err := h.Invoke(t.Context(), "dora_does_not_exist", reqJSON(t, map[string]any{}))
	if err == nil {
		t.Fatal("expected error for unknown tool name, got nil")
	}
}

func TestTools_Registry(t *testing.T) {
	h := NewHandlers("http://unused", "test-key")
	specs := h.Tools()
	want := []string{
		"dora_list_assets",
		"dora_list_order_books",
		"dora_get_order_book",
		"dora_get_order_book_stats",
		"dora_get_trades",
		"dora_get_positions",
		"dora_get_candle_data",
		"dora_get_asset_ytm",
	}
	if len(specs) != len(want) {
		t.Fatalf("Tools() returned %d specs, want %d", len(specs), len(want))
	}
	for i, w := range want {
		if specs[i].Name != w {
			t.Errorf("Tools()[%d].Name = %q, want %q", i, specs[i].Name, w)
		}
	}
}

func TestMapErr_SanitizesAuthorization(t *testing.T) {
	got := mapErr(fmt.Errorf("Authorization header missing"))
	if got == nil {
		t.Fatal("mapErr returned nil")
	}
	if strings.Contains(got.Error(), "Authorization") {
		t.Errorf("mapErr should sanitize Authorization, got %q", got.Error())
	}
	if !strings.HasPrefix(got.Error(), "dora:") {
		t.Errorf("mapErr should prefix with dora:, got %q", got.Error())
	}
}

func TestMapErr_PassesThroughNonAuth(t *testing.T) {
	got := mapErr(fmt.Errorf("connection refused"))
	if got == nil {
		t.Fatal("mapErr returned nil")
	}
	if !strings.Contains(got.Error(), "connection refused") {
		t.Errorf("mapErr should preserve non-auth message, got %q", got.Error())
	}
}
