package strategies_test

import (
	"testing"

	"github.com/google/uuid"
)

// TestCaptureWASM_RecordsWasmFields asserts CaptureWASM stamps the
// version with target=go-wasm + the wasm_ref and manifest_hash, and
// that GetVersion reads them back. Mirrors TestCapture_RecordsImageRef.
func TestCaptureWASM_RecordsWasmFields(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	m := meta("alpha")
	m.Validation = []byte(`{"build_ok":true}`)
	files := map[string]string{"main.go": "v1", "manifest.json": "{}"}

	v1, err := s.CaptureWASM(t.Context(), sessionID, userID, "openai", "gpt",
		m, files, "sha256:abc", "sha256:def")
	if err != nil {
		t.Fatalf("CaptureWASM: %v", err)
	}
	if v1.Target != "go-wasm" {
		t.Errorf("Target: got %q want go-wasm", v1.Target)
	}
	if v1.WasmRef != "sha256:abc" {
		t.Errorf("WasmRef: got %q want sha256:abc", v1.WasmRef)
	}
	if v1.ManifestHash != "sha256:def" {
		t.Errorf("ManifestHash: got %q want sha256:def", v1.ManifestHash)
	}
	if v1.ImageRef != "" {
		t.Errorf("ImageRef should be empty for wasm target, got %q", v1.ImageRef)
	}

	strategyID := strategyForUser(t, s, userID).ID

	got, err := s.GetVersion(t.Context(), strategyID, v1.Revision)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.Target != "go-wasm" {
		t.Errorf("GetVersion Target: got %q want go-wasm", got.Target)
	}
	if got.WasmRef != "sha256:abc" {
		t.Errorf("GetVersion WasmRef: got %q want sha256:abc", got.WasmRef)
	}
	if got.ManifestHash != "sha256:def" {
		t.Errorf("GetVersion ManifestHash: got %q want sha256:def", got.ManifestHash)
	}
}

// TestCaptureWASM_AppendsV2ParentedToHead asserts CaptureWASM advances
// head and parents the new version on the prior head, like Capture.
func TestCaptureWASM_AppendsV2ParentedToHead(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	v1, err := s.CaptureWASM(t.Context(), sessionID, userID, "openai", "gpt",
		meta("alpha"), map[string]string{"main.go": "v1"}, "w1", "m1")
	if err != nil {
		t.Fatalf("CaptureWASM v1: %v", err)
	}
	v2, err := s.CaptureWASM(t.Context(), sessionID, userID, "openai", "gpt",
		meta("alpha"), map[string]string{"main.go": "v2"}, "w2", "m2")
	if err != nil {
		t.Fatalf("CaptureWASM v2: %v", err)
	}
	if v1.Revision != v2.ParentRevision {
		t.Errorf("v2 parent: got %q want %q", v2.ParentRevision, v1.Revision)
	}

	strategyID := strategyForUser(t, s, userID).ID
	if head(t, s, strategyID) != v2.Revision {
		t.Errorf("head: got %q want %q", head(t, s, strategyID), v2.Revision)
	}
}

// TestCapture_DefaultsToGoWasmTarget asserts that newly-captured
// strategies land target='go-wasm' — either via CaptureWASM's explicit
// write or via the post-migration column default (Capture path).
func TestCapture_DefaultsToGoWasmTarget(t *testing.T) {
	s, userID := newStore(t)
	sessionID := uuid.NewString()
	v1, err := s.Capture(t.Context(), sessionID, userID, "openai", "gpt",
		meta("alpha"), map[string]string{"main.go": "v1"}, "")
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if v1.Target != "" && v1.Target != "go-wasm" {
		t.Errorf("Capture Target: got %q want go-wasm", v1.Target)
	}

	strategyID := strategyForUser(t, s, userID).ID
	got, err := s.GetVersion(t.Context(), strategyID, v1.Revision)
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.Target != "go-wasm" {
		t.Errorf("GetVersion Target: got %q want go-wasm", got.Target)
	}
}
