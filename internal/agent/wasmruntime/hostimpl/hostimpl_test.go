package hostimpl_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/dora-client-go/doraclient"

	"github.com/dora-network/bond-trading-strategies/internal/agent/agenttest"
	"github.com/dora-network/bond-trading-strategies/internal/agent/orderbroker"
	"github.com/dora-network/bond-trading-strategies/internal/agent/safety"
	"github.com/dora-network/bond-trading-strategies/internal/agent/wasmruntime/hostimpl"
)

// testUser is a valid UUID used as the dora_user_id in every test.
// The safety + users tables key on uuid, not free-form text.
const testUser = "77777777-7777-7777-7777-777777777777"

// fakeDora returns a CreateOrderResponseEnvelope with a fixed order
// id. Mirrors the orderbroker_test fake but trimmed to what the
// hostimpl test needs.
func fakeDora(t *testing.T) *doraclient.APIClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		id := "ord-1"
		_ = json.NewEncoder(w).Encode(doraclient.CreateOrderResponseEnvelope{
			Data:     &doraclient.OrderId{OrderId: &id},
			Metadata: doraclient.Metadata{StatusCode: 201},
		})
	}))
	t.Cleanup(srv.Close)
	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: srv.URL}}
	return doraclient.NewAPIClient(cfg)
}

func TestSubmitOrder_AllowsAndForwards(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	seedUserRow(t, pool, testUser)
	kernel, err := safety.NewKernel(pool)
	if err != nil {
		t.Fatalf("NewKernel: %v", err)
	}

	ob := orderbroker.New(fakeDora(t))

	h := hostimpl.New(hostimpl.Deps{
		Kernel:     kernel,
		Orders:     ob,
		UserID:     testUser,
		StrategyID: "strat-1",
		APIKey:     "user-key",
	})

	orderID, err := h.SubmitOrder(t.Context(), hostimpl.Intent{
		OrderBookID: "OB-1", Side: "buy", Quantity: 1, Type: "market",
	})
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	if orderID != "ord-1" {
		t.Errorf("orderID: got %q want ord-1", orderID)
	}
}

func TestSubmitOrder_BlocksWhenHalted(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	seedUserRow(t, pool, testUser)
	kernel, err := safety.NewKernel(pool)
	if err != nil {
		t.Fatalf("NewKernel: %v", err)
	}

	// Point at an unused URL; the kernel halts before the broker fires.
	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: "http://unused"}}
	ob := orderbroker.New(doraclient.NewAPIClient(cfg))

	if err := kernel.Halt(t.Context(), testUser, "test halt"); err != nil {
		t.Fatalf("Halt: %v", err)
	}
	// The kill switch persists in the shared test DB; lift it so later
	// runs (and other tests using testUser) are not left halted.
	t.Cleanup(func() { _ = kernel.Resume(t.Context(), testUser) })

	h := hostimpl.New(hostimpl.Deps{
		Kernel: kernel, Orders: ob, UserID: testUser, StrategyID: "strat-1", APIKey: "user-key",
	})

	_, err = h.SubmitOrder(t.Context(), hostimpl.Intent{
		OrderBookID: "OB-1", Side: "buy", Quantity: 1, Type: "market",
	})
	if err == nil {
		t.Fatal("expected error when halted")
	}
}

func TestSubmitOrder_BlocksOnCapExceeded(t *testing.T) {
	pool := agenttest.StartPostgres(t)
	seedUserRow(t, pool, testUser)
	kernel, err := safety.NewKernel(pool)
	if err != nil {
		t.Fatalf("NewKernel: %v", err)
	}
	if err := kernel.SetCaps(t.Context(), testUser, safety.Caps{
		MaxOpenOrders: 50, MaxNotionalPerOrder: 100,
		MaxTotalNotional: 1000, MaxOrdersPerMinute: 60,
	}); err != nil {
		t.Fatalf("SetCaps: %v", err)
	}

	cfg := doraclient.NewConfiguration()
	cfg.Servers = []doraclient.ServerConfiguration{{URL: "http://unused"}}
	ob := orderbroker.New(doraclient.NewAPIClient(cfg))

	h := hostimpl.New(hostimpl.Deps{
		Kernel: kernel, Orders: ob, UserID: testUser, StrategyID: "strat-1", APIKey: "user-key",
	})

	_, err = h.SubmitOrder(t.Context(), hostimpl.Intent{
		OrderBookID: "OB-1", Side: "buy", Quantity: 100, Price: 100, // notional 10000 > 100
		Type: "limit",
	})
	if err == nil {
		t.Fatal("expected cap-exceeded error")
	}
}

// seedUserRow inserts a minimal users row so the safety FK passes.
func seedUserRow(t *testing.T, pool *pgxpool.Pool, userID string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 'test', '{}')
		on conflict (dora_user_id) do nothing
	`, userID); err != nil {
		t.Fatalf("seed user %s: %v", userID, err)
	}
}
