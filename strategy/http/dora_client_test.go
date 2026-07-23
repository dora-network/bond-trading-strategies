package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ doraClient = (*liveDORAClient)(nil)

func TestLiveDORAClientGetUserIDIgnoresUnknownUserFields(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/user/self", r.URL.Path)
		assert.Equal(t, "ApiKey test-key", r.Header.Get("Authorization"))

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
				"tenant_id":                              "tenant-123",
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

	got, err := client.GetUserID(authctx.WithAPIKey(context.Background(), "test-key"))
	require.NoError(t, err)
	assert.Equal(t, "user-123", got)
}

func TestLiveDORAClient_ListCopyTraders(t *testing.T) {
	t.Parallel()

	fullPageIDs := func(prefix string) []string {
		ids := make([]string, 0, int(copyTraderPageSize))
		for i := range int(copyTraderPageSize) {
			ids = append(ids, fmt.Sprintf("019c0000-0000-7000-8000-%s%010d", prefix, i))
		}
		return ids
	}

	cases := []struct {
		name      string
		pages     [][]string
		wantIDs   []string
		wantCalls int
		wantPages []int
	}{
		{
			name: "happy path paginated",
			pages: [][]string{
				fullPageIDs("a"),
				fullPageIDs("b")[:50],
			},
			wantIDs:   append(fullPageIDs("a"), fullPageIDs("b")[:50]...),
			wantCalls: 2,
			wantPages: []int{1, 2},
		},
		{
			name:      "empty first page terminates",
			pages:     [][]string{nil},
			wantIDs:   nil,
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name:      "short first page terminates",
			pages:     [][]string{{"019c0000-0000-7000-8000-000000000001", "019c0000-0000-7000-8000-000000000002"}},
			wantIDs:   []string{"019c0000-0000-7000-8000-000000000001", "019c0000-0000-7000-8000-000000000002"},
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name:      "5xx propagated",
			pages:     [][]string{{}}, // unused; handler returns 500 immediately
			wantIDs:   nil,
			wantCalls: 1,
			wantPages: []int{1},
		},
		{
			name: "full pages then a short page terminates",
			pages: [][]string{
				fullPageIDs("c"),
				fullPageIDs("d"),
				{"019c0000-0000-7000-8000-000000000099"},
			},
			wantIDs:   append(append(fullPageIDs("c"), fullPageIDs("d")...), "019c0000-0000-7000-8000-000000000099"),
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
			assert.Equal(t, tc.wantIDs, got)

			mu.Lock()
			gotCalls := calls
			gotPages := append([]int(nil), pages...)
			mu.Unlock()
			assert.Equal(t, tc.wantCalls, gotCalls)
			assert.Equal(t, tc.wantPages, gotPages)
		})
	}
}
