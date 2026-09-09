package httpapi

// Session validation: the request-shape errors that come back before any
// provider lookup or runner work. These pin down the response contract
// for clients sending malformed session creation requests.

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

// TestIntegration_CreateSession_MissingField_Returns409 asserts that
// omitting either provider or model from the request body returns 409
// with the honest "request must include provider and model" message,
// not the misleading old "provider config not set".
func TestIntegration_CreateSession_MissingField_Returns409(t *testing.T) {
	srv, pool := newIntegrationServer(t)
	defer srv.Close()
	seedIntegrationsUser(t, pool)

	body := bytes.NewBufferString(`{"provider":"openai"}`)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/sessions", body)
	req.Header.Set("Authorization", "ApiKey k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: want 409, got %d", resp.StatusCode)
	}
	bs := make([]byte, 256)
	n, _ := resp.Body.Read(bs)
	if msg := string(bs[:n]); msg != "request must include provider and model\n" {
		t.Errorf("body: want 'request must include provider and model', got %q", msg)
	}
}

// TestIntegration_CreateSession_NoSavedConfig_Returns409 asserts that a
// session create with valid provider/model but no saved provider_configs
// row returns 409 with the actual provider name, not a generic 500.
func TestIntegration_CreateSession_NoSavedConfig_Returns409(t *testing.T) {
	srv, pool := newIntegrationServer(t)
	defer srv.Close()
	seedIntegrationsUser(t, pool)
	_, _ = pool.Exec(t.Context(), `delete from provider_configs where dora_user_id = $1`, integrationUserID)
	t.Cleanup(func() {
		_, _ = pool.Exec(t.Context(), `delete from provider_configs where dora_user_id = $1`, integrationUserID)
	})

	body := bytes.NewBufferString(`{"provider":"openai","model":"gpt-4o-mini"}`)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/sessions", body)
	req.Header.Set("Authorization", "ApiKey k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: want 409, got %d", resp.StatusCode)
	}
	bs := make([]byte, 256)
	n, _ := resp.Body.Read(bs)
	if msg := string(bs[:n]); !strings.Contains(msg, `provider "openai" not configured for user`) {
		t.Errorf("body: want to mention provider openai, got %q", msg)
	}
}
