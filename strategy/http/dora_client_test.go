package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/authctx"
)

var _ doraClient = (*liveDORAClient)(nil)

func TestLiveDORAClientGetUserIDIgnoresUnknownUserFields(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/user/self", r.URL.Path)
		assert.Equal(t, "ApiKey test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "tenant-A", r.Header.Get("tenant-id"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"id":                                     "user-123",
				"email":                                  "user-123@example.com",
				"first_name":                             "Test",
				"last_name":                              "User",
				"country_of_domicile":                    "US",
				"native_asset_id":                        "USD",
				"roles":                                  []string{"TRADER"},
				"show_tutorial_cards":                    false,
				"notifications_enabled":                  true,
				"tenant_id":                              "tenant-A",
				"allow_email_notifications":              true,
				"allow_liquidations_notifications":       true,
				"allow_deposit_withdrawal_notifications": true,
				"allow_orders_notifications":             true,
				"allow_copy_trading":                     true,
			},
			"metadata": map[string]any{
				"status_code": 200,
				"trace_id":    "trace",
				"request_id":  "req",
			},
		})
	}))
	defer srv.Close()

	cfg := doraclient.NewConfiguration()
	cfg.Servers = doraclient.ServerConfigurations{
		{URL: srv.URL, Description: "test"},
	}
	client := &liveDORAClient{
		client:     doraclient.NewAPIClient(cfg),
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}

	got, err := client.GetUserID(authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{
		APIKey:   "test-key",
		TenantID: "tenant-A",
	}))
	require.NoError(t, err)
	assert.Equal(t, "user-123", got)
}

func TestLiveDORAClient_SDKCallForwardsTenantID(t *testing.T) {
	t.Parallel()

	var seenTenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenTenant = r.Header.Get("tenant-id")
		if r.URL.Path != "/v1/user/copy_traders" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := doraclient.GetCopyTradersResponse{
			Data: []doraclient.CopyTrader{},
			Metadata: doraclient.Metadata{
				StatusCode: 200,
				TraceId:    "t",
				RequestId:  "r",
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cfg := doraclient.NewConfiguration()
	cfg.Servers = doraclient.ServerConfigurations{
		{URL: srv.URL, Description: "test"},
	}
	// Wire the SDK with the same tenantTransport wrapper as production.
	cfg.HTTPClient = &http.Client{Transport: &tenantTransport{base: http.DefaultTransport}}
	client := &liveDORAClient{
		client:     doraclient.NewAPIClient(cfg),
		baseURL:    srv.URL,
		httpClient: cfg.HTTPClient,
	}

	ctx := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{
		APIKey:   "test-key",
		TenantID: "tenant-SDK",
	})
	_, err := client.ListCopyTraders(ctx)
	require.NoError(t, err)
	assert.Equal(t, "tenant-SDK", seenTenant)
}

func TestTenantTransport_OmitsHeaderWhenTenantEmpty(t *testing.T) {
	t.Parallel()

	var seenHasHeader bool
	rt := &tenantTransport{base: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		_, seenHasHeader = r.Header[http.CanonicalHeaderKey(TenantIDHeader)]
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	// AuthInfo present, TenantID empty — header must NOT be set.
	ctx := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{APIKey: "k1"})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.False(t, seenHasHeader, "tenant-id header must be omitted when TenantID is empty")
}

func TestTenantTransport_SetsHeaderWhenTenantPresent(t *testing.T) {
	t.Parallel()

	var seenValue string
	rt := &tenantTransport{base: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		seenValue = r.Header.Get(TenantIDHeader)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})}

	ctx := authctx.WithAuthInfo(context.Background(), authctx.AuthInfo{APIKey: "k1", TenantID: "tenant-Z"})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com/", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, "tenant-Z", seenValue)
}

// roundTripperFunc adapts a function value to http.RoundTripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLiveDORAClient_ListCopyTraders(t *testing.T) {
	fullPageTraders := func(prefix string) []doraclient.CopyTrader {
		traders := make([]doraclient.CopyTrader, 0, int(copyTraderPageSize))
		for i := range int(copyTraderPageSize) {
			traders = append(traders, doraclient.CopyTrader{
				UserId:   fmt.Sprintf("019c0000-0000-7000-8000-%s%010d", prefix, i),
				UserName: fmt.Sprintf("%s_trader_%010d", prefix, i),
			})
		}
		return traders
	}

	wantTraders := func(prefix string) []CopyTrader {
		ids := fullPageTraders(prefix)
		out := make([]CopyTrader, 0, len(ids))
		for _, t := range ids {
			out = append(out, CopyTrader{UserID: t.UserId, UserName: t.UserName})
		}
		return out
	}

	cases := []struct {
		name      string
		pages     [][]doraclient.CopyTrader
		want      []CopyTrader
		wantCalls int
		wantPages []int
	}{
		{
			name: "happy path paginated",
			pages: [][]doraclient.CopyTrader{
				fullPageTraders("a"),
				fullPageTraders("b")[:50],
			},
			want:      append(wantTraders("a"), wantTraders("b")[:50]...),
			wantCalls: 2,
			wantPages: []int{1, 2},
		},
		{
			name:      "empty first page terminates",
			pages:     [][]doraclient.CopyTrader{nil},
			want:      nil,
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name: "short first page terminates",
			pages: [][]doraclient.CopyTrader{{
				{UserId: "019c0000-0000-7000-8000-000000000001", UserName: "alice"},
				{UserId: "019c0000-0000-7000-8000-000000000002", UserName: "bob"},
			}},
			want: []CopyTrader{
				{UserID: "019c0000-0000-7000-8000-000000000001", UserName: "alice"},
				{UserID: "019c0000-0000-7000-8000-000000000002", UserName: "bob"},
			},
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name:      "5xx propagated",
			pages:     [][]doraclient.CopyTrader{{}}, // unused; handler returns 500 immediately
			want:      nil,
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name: "full pages then a short page terminates",
			pages: [][]doraclient.CopyTrader{
				fullPageTraders("c"),
				fullPageTraders("d"),
				{{UserId: "019c0000-0000-7000-8000-000000000099", UserName: "solo"}},
			},
			want: append(append(
				wantTraders("c"),
				wantTraders("d")...,
			), CopyTrader{UserID: "019c0000-0000-7000-8000-000000000099", UserName: "solo"}),
			wantCalls: 3,
			wantPages: []int{1, 2, 3},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu    sync.Mutex
				pages []int
				calls int
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/user/copy_traders" {
					http.NotFound(w, r)
					return
				}
				page, err := strconv.Atoi(r.URL.Query().Get("page"))
				if err != nil || page < 1 {
					http.Error(w, "bad page", http.StatusBadRequest)
					return
				}

				mu.Lock()
				if calls >= len(tc.pages) {
					mu.Unlock()
					http.Error(w, "unexpected extra request", http.StatusInternalServerError)
					return
				}
				if tc.name == "5xx propagated" {
					mu.Unlock()
					http.Error(w, "boom", http.StatusInternalServerError)
					return
				}
				pageData := tc.pages[calls]
				pages = append(pages, page)
				calls++
				mu.Unlock()

				resp := doraclient.GetCopyTradersResponse{
					Metadata: doraclient.Metadata{
						StatusCode: 200,
						TraceId:    "trace",
						RequestId:  "req",
					},
					Data: pageData,
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
			}))
			defer srv.Close()

			cfg := doraclient.NewConfiguration()
			cfg.Servers = doraclient.ServerConfigurations{
				{URL: srv.URL, Description: "test"},
			}
			client := &liveDORAClient{client: doraclient.NewAPIClient(cfg)}

			ctx := authctx.WithAPIKey(context.Background(), "test-key")
			got, err := client.ListCopyTraders(ctx)

			if tc.name == "5xx propagated" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "list copy traders")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			mu.Lock()
			gotCalls := calls
			gotPages := append([]int(nil), pages...)
			mu.Unlock()
			assert.Equal(t, tc.wantCalls, gotCalls)
			assert.Equal(t, tc.wantPages, gotPages)
		})
	}
}
