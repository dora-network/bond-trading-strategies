package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotenv_NonExistentReturnsNil(t *testing.T) {
	// A nonexistent path should silently return nil (no error).
	if err := LoadDotenv(filepath.Join(t.TempDir(), "does-not-exist.env")); err != nil {
		t.Fatalf("LoadDotenv on missing file: %v", err)
	}
}

func TestLoadDotenv_LoadsPairs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(`
# this is a comment
KEY1=value1
KEY2=value2
empty=

`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv: %v", err)
	}
	if got := os.Getenv("KEY1"); got != "value1" {
		t.Errorf("KEY1: want value1, got %q", got)
	}
	if got := os.Getenv("KEY2"); got != "value2" {
		t.Errorf("KEY2: want value2, got %q", got)
	}
	if got, set := os.LookupEnv("empty"); !set || got != "" {
		t.Errorf("empty: want set to \"\", got set=%v value=%q", set, got)
	}
	// Clean up the env vars we just set so subsequent tests aren't affected.
	_ = os.Unsetenv("KEY1")
	_ = os.Unsetenv("KEY2")
	_ = os.Unsetenv("empty")
}

func TestLoadDotenv_StripsQuotes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(`
DOUBLE="quoted value"
SINGLE='quoted value'
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv: %v", err)
	}
	if got := os.Getenv("DOUBLE"); got != "quoted value" {
		t.Errorf("DOUBLE: want %q, got %q", "quoted value", got)
	}
	if got := os.Getenv("SINGLE"); got != "quoted value" {
		t.Errorf("SINGLE: want %q, got %q", "quoted value", got)
	}
	_ = os.Unsetenv("DOUBLE")
	_ = os.Unsetenv("SINGLE")
}

func TestLoadDotenv_DoesNotOverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("EXISTING=from-dotenv"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXISTING", "from-process-env")
	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv: %v", err)
	}
	if got := os.Getenv("EXISTING"); got != "from-process-env" {
		t.Errorf("EXISTING: want from-process-env (explicit env wins), got %q", got)
	}
}

func TestLoadDotenv_HandlesEmptyKeyLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(`
GOOD=ok
=missing-key
=missing-key-with-value=
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv: %v", err)
	}
	if got := os.Getenv("GOOD"); got != "ok" {
		t.Errorf("GOOD: want ok, got %q", got)
	}
	_ = os.Unsetenv("GOOD")
	// Lines with empty keys are silently ignored by godotenv.
}
