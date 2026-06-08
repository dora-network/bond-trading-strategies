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

	"github.com/dora-network/dora-client-go/doraclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ doraClient = (*liveDORAClient)(nil)

func TestIsBotUser(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		firstName string
		lastName  string
		want      bool
	}{
		{"trader underscore prefix in first name", "TRADER_01", "Smith", true},
		{"mm underscore prefix in first name", "MM_Alice", "Brown", true},
		{"trader underscore prefix in last name", "Alice", "TRADER_99", true},
		{"mm prefix in last name", "Alice", "mm_bot", true},
		{"lowercase variants", "trader_42", "doe", true},
		{"no prefix in either", "Alice", "Smith", false},
		{"empty names", "", "", false},
		{"only first name no prefix", "Alice", "", false},
		{"only last name no prefix", "", "Smith", false},
		{"trader without underscore is not a bot", "Trader", "Smith", false},
		{"mm without underscore is not a bot", "Mm", "Smith", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isBotUser(tc.firstName, tc.lastName); got != tc.want {
				t.Errorf("isBotUser(%q, %q) = %v, want %v", tc.firstName, tc.lastName, got, tc.want)
			}
		})
	}
}

func TestLiveDORAClient_ListBotUsers(t *testing.T) {
	t.Parallel()

	makeUser := func(id, firstName, lastName string) doraclient.User {
		return doraclient.User{
			Id:                id,
			Email:             id + "@example.com",
			FirstName:         firstName,
			LastName:          lastName,
			CountryOfDomicile: doraclient.CountryCode("US"),
			NativeAssetId:     "USD",
			Roles:             []doraclient.UserRole{doraclient.USERROLE_TRADER},
			TenantId:          "test-tenant",
		}
	}

	fullBotPage := func(prefix string) []doraclient.User {
		users := make([]doraclient.User, 0, copyTraderPageSize)
		for i := 0; i < int(copyTraderPageSize); i++ {
			users = append(users, makeUser(
				fmt.Sprintf("bot-%s-%03d", prefix, i),
				fmt.Sprintf("TRADER_%s_%03d", prefix, i),
				"Bot",
			))
		}
		return users
	}

	fullBotIDs := func(prefix string) []string {
		ids := make([]string, 0, copyTraderPageSize)
		for i := 0; i < int(copyTraderPageSize); i++ {
			ids = append(ids, fmt.Sprintf("bot-%s-%03d", prefix, i))
		}
		return ids
	}

	cases := []struct {
		name        string
		pages       [][]doraclient.User
		wantIDs     []string
		wantCalls   int
		wantOffsets []int32
	}{
		{
			name: "single short page filters non-bots",
			pages: [][]doraclient.User{
				{
					makeUser("u1", "TRADER_01", "Bot"),
					makeUser("u2", "Alice", "Smith"),
					makeUser("u3", "MM_Bot", "X"),
				},
			},
			wantIDs:     []string{"u1", "u3"},
			wantCalls:   1,
			wantOffsets: []int32{0},
		},
		{
			name: "multi-page with short final page",
			pages: [][]doraclient.User{
				fullBotPage("A"),
				{
					makeUser("last-1", "TRADER_99", "Z"),
					makeUser("last-2", "Bob", "Builder"),
				},
			},
			wantIDs:     append(fullBotIDs("A"), "last-1"),
			wantCalls:   2,
			wantOffsets: []int32{0, 100},
		},
		{
			name:        "empty first page returns no bots",
			pages:       [][]doraclient.User{nil},
			wantIDs:     []string{},
			wantCalls:   1,
			wantOffsets: []int32{0},
		},
		{
			name: "max pages cap stops after 10 full pages",
			pages: func() [][]doraclient.User {
				p := make([][]doraclient.User, int(copyTraderMaxPages))
				for i := range p {
					p[i] = fullBotPage("M")
				}
				return p
			}(),
			wantIDs: func() []string {
				ids := make([]string, 0, int(copyTraderMaxPages)*int(copyTraderPageSize))
				for i := 0; i < int(copyTraderMaxPages); i++ {
					ids = append(ids, fullBotIDs("M")...)
				}
				return ids
			}(),
			wantCalls: int(copyTraderMaxPages),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu      sync.Mutex
				offsets []int
				calls   int
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/user" {
					http.NotFound(w, r)
					return
				}
				offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
				if err != nil {
					http.Error(w, "bad offset", http.StatusBadRequest)
					return
				}

				mu.Lock()
				if calls >= len(tc.pages) {
					mu.Unlock()
					http.Error(w, "unexpected extra request", http.StatusInternalServerError)
					return
				}
				page := tc.pages[calls]
				offsets = append(offsets, offset)
				calls++
				mu.Unlock()

				resp := doraclient.ListUsersResponseEnvelope{
					Data: page,
					Metadata: doraclient.Metadata{
						StatusCode: 200,
						TraceId:    "trace",
						RequestId:  "req",
					},
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

			ctx := context.WithValue(context.Background(), authContextKey{}, authInfo{APIKey: "test-key"})

			got, err := client.ListBotUsers(ctx)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(got))
			for _, b := range got {
				gotIDs = append(gotIDs, b.ID)
			}
			assert.Equal(t, tc.wantIDs, gotIDs)

			mu.Lock()
			gotCalls := calls
			gotOffsets := append([]int(nil), offsets...)
			mu.Unlock()

			assert.Equal(t, tc.wantCalls, gotCalls)
			if tc.wantOffsets != nil {
				want := make([]int, len(tc.wantOffsets))
				for i, o := range tc.wantOffsets {
					want[i] = int(o)
				}
				assert.Equal(t, want, gotOffsets)
			}
		})
	}
}
