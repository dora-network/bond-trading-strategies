package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadModelCaps_EmptyPath covers the default-empty case: an unset
// AGENT_MODEL_CAPS_PATH resolves to a built-in 16384 default and no
// per-model entries, so every model resolves to the same budget.
// The 16384 floor matches configs/model_caps.json and is safe for
// every commonly-deployed OpenAI model.
func TestLoadModelCaps_EmptyPath(t *testing.T) {
	caps, err := LoadModelCaps("")
	if err != nil {
		t.Fatalf("LoadModelCaps(\"\"): %v", err)
	}
	if caps.Default != 16384 {
		t.Errorf("default: want 16384, got %d", caps.Default)
	}
	if len(caps.Models) != 0 {
		t.Errorf("models: want empty, got %d", len(caps.Models))
	}
}

// TestLoadModelCaps_HappyPath writes a small JSON config to a temp
// file, loads it, and verifies the substring-match lookup picks the
// right cap per model and falls back to the default for unknown
// models. OpenRouter-prefixed forms resolve to the same ceiling as
// the bare model name via substring match.
func TestLoadModelCaps_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model_caps.json")
	contents := `{
  "default": 64000,
  "models": [
    {"match": "claude-sonnet-5", "max_tokens": 128000},
    {"match": "gpt-4o", "max_tokens": 16384}
  ]
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	caps, err := LoadModelCaps(path)
	if err != nil {
		t.Fatalf("LoadModelCaps: %v", err)
	}
	if caps.Default != 64000 {
		t.Errorf("default: want 64000, got %d", caps.Default)
	}
	cases := []struct {
		model string
		want  int
	}{
		{"claude-sonnet-5", 128000},
		{"anthropic/claude-sonnet-5", 128000},
		{"gpt-4o", 16384},
		{"unknown-model", 64000},
		{"", 64000},
	}
	for _, tc := range cases {
		got := caps.MaxTokensFor(tc.model)
		if got != tc.want {
			t.Errorf("MaxTokensFor(%q) = %d, want %d", tc.model, got, tc.want)
		}
	}
}

// TestLoadModelCaps_MissingFile covers the typo-in-env-var case: a
// non-existent path is an error rather than silently falling
// through to the default, so the operator notices the misconfig
// instead of debugging why every model returns the same cap.
func TestLoadModelCaps_MissingFile(t *testing.T) {
	if _, err := LoadModelCaps("/nonexistent/path.json"); err == nil {
		t.Fatal("LoadModelCaps on missing file: want error, got nil")
	}
}

// TestLoadModelCaps_BadJSON covers the malformed-file case: the
// operator edited the JSON incorrectly and the loader reports the
// parse error rather than silently defaulting.
func TestLoadModelCaps_BadJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{not valid json`), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if _, err := LoadModelCaps(path); err == nil {
		t.Fatal("LoadModelCaps on bad json: want error, got nil")
	}
}

// TestLoadModelCaps_InvalidEntry covers the validation guards:
// default must be > 0, every model entry must have a non-empty
// match and a positive max_tokens. The validator catches these at
// startup so the agent surfaces a clear config error rather than
// silently using a zero cap that would break Anthropic.
func TestLoadModelCaps_InvalidEntry(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad_default.json")
	if err := os.WriteFile(bad, []byte(`{"default":0}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadModelCaps(bad); err == nil {
		t.Fatal("zero default: want error, got nil")
	}

	bad2 := filepath.Join(dir, "bad_match.json")
	if err := os.WriteFile(bad2, []byte(`{"default":1000,"models":[{"match":"","max_tokens":100}]}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadModelCaps(bad2); err == nil {
		t.Fatal("empty match: want error, got nil")
	}
}
