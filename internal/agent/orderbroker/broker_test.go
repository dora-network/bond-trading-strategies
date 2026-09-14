package orderbroker_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"

	"github.com/dora-network/dora-client-go/doraclient"
)

// fakeDora is a tiny HTTP server that mimics the two Dora REST
// endpoints the broker calls: POST /v1/orders and DELETE
// /v1/orders/{id}. It records each request so tests can assert
// the wire-format body and the auth header.
type fakeDora struct {
	server         *httptest.Server
	createCalls    atomic.Int32
	cancelCalls    atomic.Int32
	lastAuth       atomic.Value // string
	lastCreateBody atomic.Value // map[string]any
}

func newFakeDora(t *testing.T) *fakeDora {
	t.Helper()
	f := &fakeDora{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		f.lastAuth.Store(r.Header.Get("Authorization"))
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(body, &parsed)
		f.lastCreateBody.Store(parsed)
		f.createCalls.Add(1)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doraclient.CreateOrderResponseEnvelope{
			Data:     &doraclient.OrderId{OrderId: new("ord-test-1")},
			Metadata: doraclient.Metadata{StatusCode: 201, TraceId: "trace-1", RequestId: "req-1"},
		})
	})
	mux.HandleFunc("/v1/orders/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		f.lastAuth.Store(r.Header.Get("Authorization"))
		f.cancelCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doraclient.CancelOrderResponseEnvelope{
			Metadata: doraclient.Metadata{StatusCode: 200, TraceId: "trace-2", RequestId: "req-2"},
		})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// clientFor returns a *doraclient.APIClient pointed at the fake
// server. The SDK builds the per-request URL from cfg.Servers[0].
func clientFor(t *testing.T, f *fakeDora) *doraclient.APIClient {
	t.Helper()
	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: f.server.URL}}
	return doraclient.NewAPIClient(cfg)
}

func TestBroker_SubmitOrder_BuildsCorrectRequest(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)

	res, err := b.SubmitOrder(t.Context(), "user-1", "strat-1", "user-api-key", orderbroker.Intent{
		OrderBookID:        "OB-1234",
		Side:               "buy",
		Type:               "market",
		Quantity:           "100",
		InverseLeverage:    "2",
		FromGlobalPosition: false,
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	if res.OrderID != "ord-test-1" {
		t.Errorf("OrderID: got %q want ord-test-1", res.OrderID)
	}
	if got := f.createCalls.Load(); got != 1 {
		t.Errorf("create calls: got %d want 1", got)
	}
	if got, _ := f.lastAuth.Load().(string); got != "ApiKey user-api-key" {
		t.Errorf("auth header: got %q want %q", got, "ApiKey user-api-key")
	}

	body, _ := f.lastCreateBody.Load().(map[string]any)
	if body == nil {
		t.Fatal("no request body captured")
	}
	if body["side"] != "BUY" {
		t.Errorf("side: got %v want BUY", body["side"])
	}
	if body["kind"] != "MARKET" {
		t.Errorf("kind: got %v want MARKET", body["kind"])
	}
	if body["quantity"] != "100" {
		t.Errorf("quantity: got %v want 100", body["quantity"])
	}
	if body["inverse_leverage"] != "2" {
		t.Errorf("inverse_leverage: got %v want 2", body["inverse_leverage"])
	}
	if v, ok := body["from_global_position"].(bool); !ok || v {
		t.Errorf("from_global_position: got %v (%T) want false", body["from_global_position"], body["from_global_position"])
	}
	if body["order_book_id"] != "OB-1234" {
		t.Errorf("order_book_id: got %v want OB-1234", body["order_book_id"])
	}
	// ClientOrderID must always be set with the dora-agent prefix.
	coid, ok := body["client_order_id"].(string)
	if !ok || !strings.HasPrefix(coid, "dora-agent_user-1_strat-1_") {
		t.Errorf("client_order_id: got %v, want prefix dora-agent_user-1_strat-1_", body["client_order_id"])
	}
	if res.ClientOrderID != coid {
		t.Errorf("Result.ClientOrderID: got %q want %q", res.ClientOrderID, coid)
	}
}

func TestBroker_SubmitOrder_DefaultsInverseLeverageToOne(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)

	// Leave InverseLeverage empty; broker must default to "1".
	_, err := b.SubmitOrder(t.Context(), "user-1", "strat-1", "k", orderbroker.Intent{
		OrderBookID: "OB-1", Side: "buy", Type: "market", Quantity: "1",
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	body, _ := f.lastCreateBody.Load().(map[string]any)
	if body["inverse_leverage"] != "1" {
		t.Errorf("inverse_leverage default: got %v want \"1\"", body["inverse_leverage"])
	}
}

func TestBroker_SubmitOrder_DefaultsFromGlobalPositionToFalse(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)

	// FromGlobalPosition zero value is false (isolated margin).
	_, err := b.SubmitOrder(t.Context(), "user-1", "strat-1", "k", orderbroker.Intent{
		OrderBookID: "OB-1", Side: "buy", Type: "market", Quantity: "1",
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	body, _ := f.lastCreateBody.Load().(map[string]any)
	if v, ok := body["from_global_position"].(bool); !ok || v {
		t.Errorf("from_global_position default: got %v want false", body["from_global_position"])
	}
}

func TestBroker_SubmitOrder_LimitPassesPrice(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)

	_, err := b.SubmitOrder(t.Context(), "user-1", "strat-1", "k", orderbroker.Intent{
		OrderBookID: "OB-1", Side: "sell", Type: "limit",
		Quantity: "0.5", Price: "99.5", InverseLeverage: "1",
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	body, _ := f.lastCreateBody.Load().(map[string]any)
	if body["kind"] != "LIMIT" {
		t.Errorf("kind: got %v want LIMIT", body["kind"])
	}
	if body["side"] != "SELL" {
		t.Errorf("side: got %v want SELL", body["side"])
	}
	if body["price"] != "99.5" {
		t.Errorf("price: got %v want 99.5", body["price"])
	}
}

// TestBroker_SubmitOrder_MarketIgnoresPrice asserts that a market
// order does not carry a price field in the wire body, even when
// the strategy sets intent.Price. Per the dora-api
// ValidateSubmitOrderRequest contract: market orders must have
// price = 0 or omitted. The broker omits it (the SDK's *string
// pointer is nil), so the field is absent from the JSON.
func TestBroker_SubmitOrder_MarketIgnoresPrice(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)

	_, err := b.SubmitOrder(t.Context(), "user-1", "strat-1", "k", orderbroker.Intent{
		OrderBookID:     "OB-1",
		Side:            "buy",
		Type:            "market",
		Quantity:        "10",
		Price:           "99.5", // must be ignored for market orders
		InverseLeverage: "1",
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	body, _ := f.lastCreateBody.Load().(map[string]any)
	if body["kind"] != "MARKET" {
		t.Errorf("kind: got %v want MARKET", body["kind"])
	}
	if v, ok := body["price"]; ok {
		t.Errorf("market order must not include price; got %v", v)
	}
}

func TestBroker_SubmitOrder_RejectsEmptyAPIKey(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)
	_, err := b.SubmitOrder(t.Context(), "u", "strat-1", "", orderbroker.Intent{
		OrderBookID: "OB-1", Side: "buy", Type: "market", Quantity: "1",
	})
	if err == nil {
		t.Fatal("expected error on empty API key")
	}
}

func TestBroker_CancelOrder_ForwardsAuthAndID(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)
	if err := b.CancelOrder(t.Context(), "u", "k", "ord-test-1"); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if got := f.cancelCalls.Load(); got != 1 {
		t.Errorf("cancel calls: got %d want 1", got)
	}
	if got, _ := f.lastAuth.Load().(string); got != "ApiKey k" {
		t.Errorf("auth header: got %q want %q", got, "ApiKey k")
	}
}

func TestBroker_CancelOrder_RejectsEmptyID(t *testing.T) {
	f := newFakeDora(t)
	api := clientFor(t, f)
	b := orderbroker.New(api)
	if err := b.CancelOrder(t.Context(), "u", "k", ""); err == nil {
		t.Fatal("expected error on empty order ID")
	}
}
