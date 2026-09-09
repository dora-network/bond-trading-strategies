// Package dora implements the read-only Dora tool handlers.
// Each handler is a thin wrapper around the regenerated doraclient SDK using
// a per-request API key. See spec §5 (Dora read tools).
package dora

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	doraclient "github.com/dora-network/dora-client-go/doraclient"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
)

// Handlers owns the per-request API key and the lazily-built SDK client. One
// instance is constructed per agent turn.
type Handlers struct {
	baseURL string
	apiKey  string
	client  *doraclient.APIClient
}

// NewHandlers constructs a per-turn Handlers bound to the given Dora base URL
// and per-request API key.
func NewHandlers(baseURL, apiKey string) *Handlers {
	return &Handlers{baseURL: baseURL, apiKey: apiKey}
}

// api returns the SDK client, building it on first use. The client's transport
// injects the per-request "Authorization: ApiKey <key>" header on every
// request: the generated SDK only attaches the context-value API key on a
// minority of operations (auth-marked in the OpenAPI spec), so a transport
// is the only way to guarantee every read tool is authenticated.
func (h *Handlers) api() *doraclient.APIClient {
	if h.client == nil {
		h.client = NewClient(h.baseURL)
		// Wrap the SDK's HTTP client with a header-injecting transport. The key
		// is held on the Handlers (per-turn, in-memory) and never logged.
		cfg := h.client.GetConfig()
		base := cfg.HTTPClient
		if base == nil {
			base = http.DefaultClient
		}
		cfg.HTTPClient = &http.Client{
			Transport: &authTransport{base: base.Transport, apiKey: h.apiKey},
			Timeout:   base.Timeout,
		}
	}
	return h.client
}

// authTransport sets "Authorization: ApiKey <key>" on every outbound request
// unless the caller already supplied one.
type authTransport struct {
	base   http.RoundTripper
	apiKey string
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") == "" && t.apiKey != "" {
		req.Header.Set("Authorization", apiKeyPrefix+" "+t.apiKey)
	}
	if t.base == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.base.RoundTrip(req)
}

// authed returns a context carrying the per-request API key.
func (h *Handlers) authed(ctx context.Context) context.Context {
	return WithAPIKey(ctx, h.apiKey)
}

// Tool names, in the order Tools() returns them and Invoke dispatches them.
const (
	toolListAssets        = "dora_list_assets"
	toolListOrderBooks    = "dora_list_order_books"
	toolGetOrderBook      = "dora_get_order_book"
	toolGetOrderBookStats = "dora_get_order_book_stats"
	toolGetTrades         = "dora_get_trades"
	toolGetPositions      = "dora_get_positions"
	toolGetCandleData     = "dora_get_candle_data"
	toolGetAssetYTM       = "dora_get_asset_ytm"
)

// JSON schemas for each tool. Defined as named constants so the
// 140-char lll rule in golangci-lint does not force us to either lop
// off the "description" fields or pepper the file with //nolint:lll.
//
//nolint:lll // JSON schemas are inherently long; lll on this const block is acceptable.
const (
	// order_book_id and asset_id are UUIDv7s from dora_list_orderbooks /
	// dora_list_assets, NOT the human-readable display_name like
	// "GOOG-USD" or "AMZN". Pass the order_book_id field from the
	// list response, not the display_name.
	schemaListAssets        = `{"type":"object","properties":{"asset_kind":{"type":"string","description":"asset_kind, e.g. BOND, CURRENCY, POOL_SHARE"},"asset_id":{"type":"string","description":"asset_id (UUID, not display symbol)"},"page":{"type":"integer"},"limit":{"type":"integer"}},"required":[]}`
	schemaListOrderBooks    = `{"type":"object","properties":{"base_asset_id":{"type":"string","description":"base_asset_id (UUID)"},"page":{"type":"integer"},"limit":{"type":"integer"}},"required":[]}`
	schemaGetOrderBook      = `{"type":"object","properties":{"order_book_id":{"type":"string","description":"order_book_id (UUIDv7 from dora_list_orderbooks), NOT the display_name like GOOG-USD"}},"required":["order_book_id"]}`
	schemaGetOrderBookStats = `{"type":"object","properties":{"order_book_id":{"type":"string","description":"order_book_id (UUIDv7, NOT display_name)"}},"required":["order_book_id"]}`
	schemaGetTrades         = `{"type":"object","properties":{"order_book_id":{"type":"string","description":"order_book_id (UUIDv7, NOT display_name)"},"limit":{"type":"integer"}},"required":[]}`
	schemaGetPositions      = `{"type":"object","properties":{}}`
	schemaGetCandleData     = `{"type":"object","properties":{"order_book_id":{"type":"string","description":"order_book_id (UUIDv7, NOT display_name)"},"start":{"type":"string","description":"RFC3339 start time, REQUIRED (e.g. 2024-01-01T00:00:00Z)"},"end":{"type":"string","description":"RFC3339 end time, REQUIRED (e.g. 2024-01-01T00:00:00Z)"},"resolution":{"type":"string","description":"resolution, e.g. 1h"},"limit":{"type":"integer"}},"required":["order_book_id","start","end"]}`
	schemaGetAssetYTM       = `{"type":"object","properties":{"asset_id":{"type":"string","description":"asset_id (UUIDv7, NOT display_symbol like GOOG_4.8_2036)"}},"required":["asset_id"]}`
)

// Tools returns the eight read-only Dora tool specs, in spec order.
func (h *Handlers) Tools() []llm.ToolSpec {
	return []llm.ToolSpec{
		spec(toolListAssets, "List tradeable Dora assets with optional filtering.", schemaListAssets),
		spec(toolListOrderBooks, "List Dora order books with optional asset filtering.", schemaListOrderBooks),
		spec(toolGetOrderBook, "Get the L2 (full depth) snapshot for a Dora order book.", schemaGetOrderBook),
		spec(toolGetOrderBookStats, "Get price/volume stats for a Dora order book.", schemaGetOrderBookStats),
		spec(toolGetTrades, "List recent Dora trade prints with optional filtering.", schemaGetTrades),
		spec(toolGetPositions, "List the caller’s ledger accounts (Dora V2 endpoint).", schemaGetPositions),
		spec(toolGetCandleData, "Get historical OHLCV candles for a Dora order book.", schemaGetCandleData),
		spec(toolGetAssetYTM, "Get the annualized yield-to-maturity for a bond asset.", schemaGetAssetYTM),
	}
}

// spec builds a ToolSpec with an explicit JSON schema. Each tool only
// uses a small set of optional or required filter fields; the schema
// tells the LLM which fields to send. Without it, Anthropic may emit
// empty input and the handler rejects as "order_book_id is required"
// or similar.
func spec(name, desc, schema string) llm.ToolSpec {
	return llm.ToolSpec{
		Name:        name,
		Description: desc,
		JSONSchema:  json.RawMessage(schema),
	}
}

// Invoke dispatches a Dora read tool by name. Unknown names return an error.
// Every dispatch attaches the per-request API key via WithAPIKey. Each dispatch
// is timed so a stalled upstream surfaces in the agent log (the agent's
// per-iteration LLM timeout only watches provider.Stream, not handler calls).
func (h *Handlers) Invoke(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	// Defensive: empty or whitespace input means {}. The shim also
	// normalizes "" and "null" to {} but if the LLM emits a stray
	// token (e.g. " " or a partial JSON fragment), the per-handler
	// json.Unmarshal would fail with "unexpected end of JSON input".
	if len(bytes.TrimSpace(input)) == 0 {
		input = json.RawMessage(`{}`)
	}
	slog.Info("dora tool invoke start", "name", name, "input_len", len(input))
	start := time.Now()
	out, err := h.dispatch(ctx, name, input)
	slog.Info("dora tool invoke done", "name", name, "duration_ms", time.Since(start).Milliseconds(), "err", errString(err))
	return out, err
}

// dispatch routes an Invoke to the per-tool handler.
func (h *Handlers) dispatch(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	switch name {
	case toolListAssets:
		return h.callListAssets(ctx, input)
	case toolListOrderBooks:
		return h.callListOrderBooks(ctx, input)
	case toolGetOrderBook:
		return h.callGetOrderBook(ctx, input)
	case toolGetOrderBookStats:
		return h.callGetOrderBookStats(ctx, input)
	case toolGetTrades:
		return h.callGetTrades(ctx, input)
	case toolGetPositions:
		return h.callGetPositions(ctx, input)
	case toolGetCandleData:
		return h.callGetCandleData(ctx, input)
	case toolGetAssetYTM:
		return h.callGetAssetYTM(ctx, input)
	default:
		return nil, fmt.Errorf("dora: unknown tool %q", name)
	}
}

// errString returns err.Error() or "" for a nil error.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// listAssetsInput are the optional filters for dora_list_assets.
type listAssetsInput struct {
	Page      *int32 `json:"page"`
	Limit     *int32 `json:"limit"`
	AssetKind string `json:"asset_kind"`
}

func (h *Handlers) callListAssets(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in listAssetsInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, mapErr(err)
	}
	req := h.api().DefaultAPI.ListAssets(h.authed(ctx))
	if in.Page != nil {
		req = req.Page(*in.Page)
	}
	if in.Limit != nil {
		req = req.Limit(*in.Limit)
	}
	if in.AssetKind != "" {
		req = req.AssetKind(doraclient.AssetKind(in.AssetKind))
	}
	resp, httpResp, err := req.Execute()
	return marshalResp(resp, httpResp, err)
}

// listOrderBooksInput are the optional filters for dora_list_order_books.
type listOrderBooksInput struct {
	Page        *int32 `json:"page"`
	Limit       *int32 `json:"limit"`
	BaseAssetID string `json:"base_asset_id"`
}

func (h *Handlers) callListOrderBooks(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in listOrderBooksInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, mapErr(err)
	}
	req := h.api().DefaultAPI.ListOrderBooks(h.authed(ctx))
	if in.Page != nil {
		req = req.Page(*in.Page)
	}
	if in.Limit != nil {
		req = req.Limit(*in.Limit)
	}
	if in.BaseAssetID != "" {
		req = req.BaseAssetId(in.BaseAssetID)
	}
	resp, httpResp, err := req.Execute()
	return marshalResp(resp, httpResp, err)
}

// orderBookIDInput is the single required input for tools keyed by order book id.
type orderBookIDInput struct {
	OrderBookID string `json:"order_book_id"`
}

func (h *Handlers) callGetOrderBook(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	in, err := parseOrderBookID(input)
	if err != nil {
		return nil, err
	}
	resp, httpResp, cErr := h.api().DefaultAPI.GetL2Depth(h.authed(ctx), in.OrderBookID).Execute()
	return marshalResp(resp, httpResp, cErr)
}

func (h *Handlers) callGetOrderBookStats(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	in, err := parseOrderBookID(input)
	if err != nil {
		return nil, err
	}
	resp, httpResp, cErr := h.api().DefaultAPI.GetOrderbookStats(h.authed(ctx), in.OrderBookID).Execute()
	return marshalResp(resp, httpResp, cErr)
}

// parseOrderBookID unmarshals an order_book_id input and rejects empties.
func parseOrderBookID(input json.RawMessage) (orderBookIDInput, error) {
	var in orderBookIDInput
	if err := json.Unmarshal(input, &in); err != nil {
		return in, mapErr(err)
	}
	if in.OrderBookID == "" {
		return in, fmt.Errorf("dora: order_book_id is required")
	}
	return in, nil
}

// getTradesInput are the optional filters for dora_get_trades.
type getTradesInput struct {
	UserIds      []string `json:"user_ids"`
	OrderBookIds []string `json:"order_book_ids"`
	Page         *int32   `json:"page"`
	Limit        *int32   `json:"limit"`
}

func (h *Handlers) callGetTrades(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in getTradesInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, mapErr(err)
	}
	req := h.api().DefaultAPI.GetTrades(h.authed(ctx))
	if len(in.UserIds) > 0 {
		req = req.UserIds(in.UserIds)
	}
	if len(in.OrderBookIds) > 0 {
		req = req.OrderBookIds(in.OrderBookIds)
	}
	if in.Page != nil {
		req = req.Page(*in.Page)
	}
	if in.Limit != nil {
		req = req.Limit(*in.Limit)
	}
	resp, httpResp, err := req.Execute()
	return marshalResp(resp, httpResp, err)
}

// callGetPositions lists the caller's ledger accounts via the V2
// endpoint (AGENTS.md "Prefer v2 routes over v1 whenever a v2
// equivalent exists; v1 routes are deprecated"). The accounts
// payload is small enough that no pagination input is wired today.
func (h *Handlers) callGetPositions(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	resp, httpResp, err := h.api().DefaultAPI.GetLedgerAccountsSelfV2(h.authed(ctx)).Execute()
	return marshalResp(resp, httpResp, err)
}

// candleDataInput are the filters for dora_get_candle_data. The Dora API
// requires both start and end on the candle endpoint, so both are mandatory.
type candleDataInput struct {
	OrderBookID string `json:"order_book_id"`
	Start       string `json:"start"`      // RFC3339 timestamp, required
	End         string `json:"end"`        // RFC3339 timestamp, required
	Resolution  string `json:"resolution"` // 1m|5m|15m|1h|4h|1d, optional
}

func (h *Handlers) callGetCandleData(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in candleDataInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, mapErr(err)
	}
	if in.OrderBookID == "" {
		return nil, fmt.Errorf("dora: order_book_id is required")
	}
	if in.Start == "" {
		return nil, fmt.Errorf("dora: start is required")
	}
	if in.End == "" {
		return nil, fmt.Errorf("dora: end is required")
	}
	start, err := time.Parse(time.RFC3339, in.Start)
	if err != nil {
		return nil, mapErr(fmt.Errorf("dora: invalid start: %w", err))
	}
	end, err := time.Parse(time.RFC3339, in.End)
	if err != nil {
		return nil, mapErr(fmt.Errorf("dora: invalid end: %w", err))
	}
	req := h.api().DefaultAPI.GetCandleData(h.authed(ctx), in.OrderBookID).Start(start).End(end)
	if in.Resolution != "" {
		req = req.Resolution(doraclient.CandleResolution(in.Resolution))
	}
	resp, httpResp, cErr := req.Execute()
	return marshalResp(resp, httpResp, cErr)
}

// assetIDInput is the single required input for dora_get_asset_ytm.
type assetIDInput struct {
	AssetID string `json:"asset_id"`
}

func (h *Handlers) callGetAssetYTM(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
	var in assetIDInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, mapErr(err)
	}
	if in.AssetID == "" {
		return nil, fmt.Errorf("dora: asset_id is required")
	}
	resp, httpResp, err := h.api().DefaultAPI.GetAssetYTMById(h.authed(ctx), in.AssetID).Execute()
	return marshalResp(resp, httpResp, err)
}

// marshalResp marshals an SDK envelope to JSON, mapping any call error through
// mapErr. The SDK's *http.Response body is closed before returning. A nil
// envelope with no error marshals to null.
func marshalResp(resp any, httpResp *http.Response, err error) (json.RawMessage, error) {
	if httpResp != nil && httpResp.Body != nil {
		_ = httpResp.Body.Close()
	}
	if err != nil {
		return nil, mapErr(err)
	}
	if resp == nil {
		return json.RawMessage("null"), nil
	}
	b, mErr := json.Marshal(resp)
	if mErr != nil {
		return nil, mapErr(mErr)
	}
	return b, nil
}

// mapErr wraps err as "dora: <err>" and rewrites any error mentioning the
// Authorization header to a generic upstream error, so credential details
// never surface to the model. Defence-in-depth.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "Authorization") {
		return fmt.Errorf("dora: upstream error")
	}
	return fmt.Errorf("dora: %w", err)
}
