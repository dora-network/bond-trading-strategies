package dora

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	doraclient "github.com/dora-network/dora-client-go/doraclient"
)

// userSelfBody is a minimal but SDK-valid *doraclient.UserEnvelope payload.
// User.UnmarshalJSON is strict, so the placeholder fields the tool handlers
// never read are included to let the SDK decode the body without error. The
// assertions below only care about the request, not the decoded user.
const userSelfBody = `{"data":{"id":"u-1","user_name":"u-1","email":"a@b.c","first_name":"","last_name":"","country_of_domicile":"US","native_asset_id":"","roles":["TRADER"],"show_tutorial_cards":false,"notifications_enabled":false,"tenant_id":"t-1","allow_email_notifications":false,"allow_liquidations_notifications":false,"allow_deposit_withdrawal_notifications":false,"allow_orders_notifications":false,"allow_copy_trading":false},"metadata":{"status_code":200,"trace_id":"","request_id":""}}`

// TestNewClient_PointsAtBaseURL confirms NewClient configures the SDK to send
// requests to the given base URL and returns a non-nil client.
func TestNewClient_PointsAtBaseURL(t *testing.T) {
	var hit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(userSelfBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	if c == nil {
		t.Fatal("NewClient returned nil")
	}

	ctx := WithAPIKey(t.Context(), "test-key")
	doGetUserSelf(t, c, ctx)
	if !hit {
		t.Error("request never reached the test server; base URL not applied")
	}
}

// TestWithAPIKey_AttachesAuthorization confirms the per-request API key flows
// through the SDK's context value so the outbound Authorization header carries
// the "ApiKey <key>" scheme Dora expects, matching the per-request auth idiom.
func TestWithAPIKey_AttachesAuthorization(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(userSelfBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	ctx := WithAPIKey(t.Context(), "test-key")
	doGetUserSelf(t, c, ctx)

	if want := "ApiKey test-key"; gotAuth != want {
		t.Errorf("Authorization header: want %q, got %q", want, gotAuth)
	}
}

// TestWithAPIKey_ReturnsContext confirms WithAPIKey returns a non-nil context
// distinct from its parent that carries the SDK's ContextAPIKeys value.
func TestWithAPIKey_ReturnsContext(t *testing.T) {
	parent := t.Context()
	ctx := WithAPIKey(parent, "test-key")
	if ctx == nil {
		t.Fatal("WithAPIKey returned nil context")
	}
	v := ctx.Value(doraclient.ContextAPIKeys)
	keys, ok := v.(map[string]doraclient.APIKey)
	if !ok {
		t.Fatalf("ContextAPIKeys value type: want map[string]doraclient.APIKey, got %T", v)
	}
	k, present := keys["apiKeyAuthHeader"]
	if !present {
		t.Fatal("apiKeyAuthHeader entry missing from context API keys")
	}
	if k.Key != "test-key" {
		t.Errorf("APIKey.Key: want %q, got %q", "test-key", k.Key)
	}
	if k.Prefix != "ApiKey" {
		t.Errorf("APIKey.Prefix: want %q, got %q", "ApiKey", k.Prefix)
	}
}

// doGetUserSelf runs GetUserSelf via the SDK and closes the response body,
// failing the test on error. bodyclose requires the *http.Response.Body be
// closed even when the caller discards the envelope.
func doGetUserSelf(t *testing.T, c *doraclient.APIClient, ctx context.Context) {
	t.Helper()
	_, httpResp, err := c.DefaultAPI.GetUserSelf(ctx).Execute()
	if err != nil {
		t.Fatalf("GetUserSelf: %v", err)
	}
	if httpResp != nil && httpResp.Body != nil {
		_ = httpResp.Body.Close()
	}
}
