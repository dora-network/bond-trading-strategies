package config

import (
	"strings"
	"testing"
	"time"
)

// clearEnv unsets every env var the agent reads, so tests don't
// inherit values from the developer's shell.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DORA_BASE_URL", "LOG_LEVEL", "CORS_ALLOWED_ORIGINS", "DORA_AUTH_CACHE_TTL",
		"AGENT_RATE_LIMIT_PER_MIN", "AGENT_LLM_TIMEOUT", "AGENT_LLM_MAX_ITERS",
		"AGENT_MAX_PROMPT_BYTES", "AGENT_DORA_TOOLS_ENABLED",
		"AGENT_MODEL_CAPS_PATH",
	} {
		t.Setenv(k, "")
	}
}

func TestLoad_Success(t *testing.T) {
	clearEnv(t)
	t.Setenv("DORA_BASE_URL", "https://staging.dora.co")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DoraBaseURL != "https://staging.dora.co" {
		t.Errorf("DoraBaseURL: want staging URL, got %q", c.DoraBaseURL)
	}
	if c.LogLevel != "info" {
		t.Errorf("LogLevel default: want \"info\", got %q", c.LogLevel)
	}
	if c.AuthCacheTTL != 5*time.Minute {
		t.Errorf("AuthCacheTTL default: want 5m, got %v", c.AuthCacheTTL)
	}
	if c.RateLimitPerMin != 20 {
		t.Errorf("RateLimitPerMin default: want 20, got %d", c.RateLimitPerMin)
	}
	if c.MaxPromptBytes != 32*1024 {
		t.Errorf("MaxPromptBytes default: want 32768, got %d", c.MaxPromptBytes)
	}
	if c.LLMTimeout != 10*time.Minute {
		t.Errorf("LLMTimeout default: want 10m, got %v", c.LLMTimeout)
	}
	if c.LLMMaxIters != 50 {
		t.Errorf("LLMMaxIters default: want 50, got %d", c.LLMMaxIters)
	}
	if c.DoraToolsEnabled != true {
		t.Errorf("DoraToolsEnabled default: want true, got %v", c.DoraToolsEnabled)
	}
}

func TestLoad_MissingDoraBaseURL(t *testing.T) {
	clearEnv(t)
	_, err := Load()
	if err == nil {
		t.Fatal("Load: expected error when DORA_BASE_URL is missing")
	}
	if !strings.Contains(err.Error(), "DORA_BASE_URL") {
		t.Errorf("error %q should mention DORA_BASE_URL", err.Error())
	}
}

func TestLoad_Overrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("DORA_BASE_URL", "https://staging.dora.co")
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("DORA_AUTH_CACHE_TTL", "90s")
	t.Setenv("AGENT_LLM_TIMEOUT", "3m")
	t.Setenv("AGENT_RATE_LIMIT_PER_MIN", "5")
	t.Setenv("AGENT_LLM_MAX_ITERS", "25")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.LogLevel != "warn" {
		t.Errorf("LogLevel: want warn, got %q", c.LogLevel)
	}
	if c.AuthCacheTTL != 90*time.Second {
		t.Errorf("AuthCacheTTL: want 90s, got %v", c.AuthCacheTTL)
	}
	if c.LLMTimeout != 3*time.Minute {
		t.Errorf("LLMTimeout: want 3m, got %v", c.LLMTimeout)
	}
	if c.RateLimitPerMin != 5 {
		t.Errorf("RateLimitPerMin: want 5, got %d", c.RateLimitPerMin)
	}
	if c.LLMMaxIters != 25 {
		t.Errorf("LLMMaxIters: want 25, got %d", c.LLMMaxIters)
	}
}
