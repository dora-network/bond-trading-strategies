package httpapi

// Prompt classification: the runner-level guard that emits a refusal
// SSE event before any LLM call or tool dispatch when the classifier
// returns off_topic. The classifier hop is fake; the SSE writer, the
// runner driver, and the message persistence are real production code.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/providerconfig"
	"github.com/dora-network/bond-trading-strategies/internal/agent/sanitize"
	agentsecrets "github.com/dora-network/bond-trading-strategies/internal/agent/secrets"
	"github.com/dora-network/bond-trading-strategies/internal/agent/session"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
)

// offTopicRunner drives the real runStrategyTurn with a fake classifier
// returning off_topic. The providerFactory returns a no-op scriptProvider
// so the classifier hop can run; off_topic short-circuits before any
// LLM call. The poolAuditWriter persists the strategy.refused audit row.
type offTopicRunner struct {
	pool *pgxpool.Pool
}

func (r *offTopicRunner) Run(ctx context.Context, userID, sessionID, _, _ string, _ []session.StoredMessage, prompt string, emit func(EventPayload)) (string, error) {
	var recordedPrompt string
	cls := &fakeClassifier{
		verdict: sanitize.ClassifyVerdict{Verdict: "off_topic", Reason: "not about bonds"},
		record:  &recordedPrompt,
	}
	provider := &scriptProvider{}
	deps := turnDeps{
		prompt:        prompt,
		userID:        userID,
		sessionID:     sessionID,
		emit:          emit,
		audit:         poolAuditWriter{Pool: r.pool},
		newProvider:   providerFactoryFor(provider),
		newClassifier: func(_ llm.Provider, _ string) Classifier { return cls },
		newTools:      func(_, _ string) ([]llm.ToolSpec, map[string]llm.ToolHandler) { return nil, nil },
	}
	return runStrategyTurn(ctx, deps)
}

// poolAuditWriter is a minimal AuditWriter backed by a pgxpool.
type poolAuditWriter struct{ Pool *pgxpool.Pool }

func (p poolAuditWriter) Insert(ctx context.Context, userID, action string, detail []byte) error {
	_, err := p.Pool.Exec(ctx, `insert into agent.audit_log (dora_user_id, action, detail) values ($1, $2, $3)`, userID, action, detail)
	return err
}

// TestIntegration_PromptOffTopic_EmitsRefusalThenDone drives the real
// runStrategyTurn driver with a fake classifier returning off_topic.
// Asserts the SSE stream contains [refusal, done] in that order, and
// the refusal text is persisted as the assistant message.
func TestIntegration_PromptOffTopic_EmitsRefusalThenDone(t *testing.T) {
	pool := servertest.StartPostgres(t)
	ctx := t.Context()

	sealer := agentsecrets.NewSealer(bytes.Repeat([]byte{0xCD}, 32))
	sessStore := session.New(pool)
	cfgStore := providerconfig.New(pool, sealer)

	// Wipe all per-test data for the integration user.
	_, _ = cfgStore.Pool.Exec(ctx, `delete from agent.strategies where dora_user_id = $1`, integrationUserID)
	_, _ = cfgStore.Pool.Exec(ctx, `delete from agent.audit_log where dora_user_id = $1`, integrationUserID)
	_, _ = cfgStore.Pool.Exec(ctx, `delete from agent.sessions where dora_user_id = $1`, integrationUserID)
	_, _ = cfgStore.Pool.Exec(ctx, `delete from agent.provider_configs where dora_user_id = $1`, integrationUserID)
	_, _ = cfgStore.Pool.Exec(ctx, `delete from agent.users where dora_user_id = $1`, integrationUserID)

	srv := New(
		sessStore, cfgStore,
		WithAgentRunner(&offTopicRunner{pool: cfgStore.Pool}),
	)
	srvHTTP := startLocalServer(PrincipalMiddleware(integrationPrincipal, "k")(srv.Routes()))
	defer srvHTTP.Close()
	seedIntegrationsUser(t, cfgStore.Pool)
	seedProviderConfig(t, cfgStore.Pool, "openai", "sk-test", "gpt-4o-mini")

	sessBody := bytes.NewBufferString(`{"provider":"openai","model":"gpt-4o-mini","title":"off-topic"}`)
	sr, _ := http.NewRequestWithContext(ctx, http.MethodPost, srvHTTP.URL+"/v1/sessions", sessBody)
	sr.Header.Set("Authorization", "ApiKey k")
	srs, err := http.DefaultClient.Do(sr)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	var sess struct {
		SessionID string `json:"session_id"`
	}
	_ = json.NewDecoder(srs.Body).Decode(&sess)
	srs.Body.Close()
	if sess.SessionID == "" {
		t.Fatal("no session_id")
	}

	msgBody := bytes.NewBufferString(`{"prompt":"What is the weather?"}`)
	mr, _ := http.NewRequestWithContext(ctx, http.MethodPost, srvHTTP.URL+"/v1/sessions/"+sess.SessionID+"/messages", msgBody)
	mr.Header.Set("Authorization", "ApiKey k")
	mr.Header.Set("Accept", "text/event-stream")
	mrs, err := http.DefaultClient.Do(mr)
	if err != nil {
		t.Fatalf("post message: %v", err)
	}
	defer mrs.Body.Close()
	if mrs.StatusCode != http.StatusOK {
		t.Fatalf("post message: want 200, got %d", mrs.StatusCode)
	}
	if ct := mrs.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type: want text/event-stream, got %q", ct)
	}
	_, sawDone, sawRefusal := readSSE(t, mrs.Body)
	if !sawRefusal {
		t.Error("SSE stream did not contain a refusal event")
	}
	if !sawDone {
		t.Error("SSE stream did not contain a done event")
	}

	gr, _ := http.NewRequestWithContext(ctx, http.MethodGet, srvHTTP.URL+"/v1/sessions/"+sess.SessionID, nil)
	gr.Header.Set("Authorization", "ApiKey k")
	gres, err := http.DefaultClient.Do(gr)
	if err != nil {
		t.Fatalf("get session history: %v", err)
	}
	defer gres.Body.Close()
	var hist struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(gres.Body).Decode(&hist)
	if len(hist.Messages) != 2 {
		t.Fatalf("persisted messages: want 2, got %d", len(hist.Messages))
	}
	if hist.Messages[1].Role != "assistant" {
		t.Errorf("assistant role: want 'assistant', got %q", hist.Messages[1].Role)
	}
	if !strings.Contains(hist.Messages[1].Content, "trading strategies") {
		t.Errorf("refusal text: want to mention 'trading strategies', got %q", hist.Messages[1].Content)
	}
}
