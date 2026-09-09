package scan

import (
	"strings"
	"testing"
)

func TestScan_ExecCommand(t *testing.T) {
	_ = t.Context()
	// Fixture isolates exec.Command: the qualified call alone, without an
	// `import "os/exec"` line (which would also match the os/exec pattern).
	files := map[string]string{
		"main.go": "package main\nfunc main(){ _ = exec.Command(\"ls\") }\n",
	}
	hits := Scan(files)
	if len(hits) != 1 {
		t.Fatalf("hits: want 1, got %d (%+v)", len(hits), hits)
	}
	if !strings.Contains(hits[0].Pattern, "exec.Command") {
		t.Errorf("pattern: got %q", hits[0].Pattern)
	}
}

func TestScan_NetDial(t *testing.T) {
	_ = t.Context()
	files := map[string]string{
		"main.go": "package main\nfunc main(){ _, _ = net.Dial(\"tcp\", \"x\") }\n",
	}
	hits := Scan(files)
	if len(hits) == 0 {
		t.Fatalf("hits: want >=1, got 0")
	}
}

func TestScan_Clean(t *testing.T) {
	_ = t.Context()
	files := map[string]string{
		"main.go": "package main\nfunc Add(a, b int) int { return a + b }\n",
	}
	hits := Scan(files)
	if len(hits) != 0 {
		t.Fatalf("hits: want 0, got %+v", hits)
	}
}

func TestScan_OnlyGoFiles(t *testing.T) {
	_ = t.Context()
	files := map[string]string{
		"README.md": "exec.Command net.Dial os.Open",
	}
	hits := Scan(files)
	if len(hits) != 0 {
		t.Fatalf("README.md must be ignored, got %+v", hits)
	}
}

// TestScan_FirstHitPerFilePerPattern verifies that a pattern matching on
// multiple lines in the same file yields exactly one hit per pattern per
// file (the first occurrence), per spec §16.4.
func TestScan_FirstHitPerFilePerPattern(t *testing.T) {
	_ = t.Context()
	files := map[string]string{
		"main.go": "package main\n" +
			"func main(){ _ = exec.Command(\"a\"); _ = exec.Command(\"b\") }\n",
	}
	hits := Scan(files)
	if len(hits) != 1 {
		t.Fatalf("hits: want 1 (first only), got %d (%+v)", len(hits), hits)
	}
	if !strings.Contains(hits[0].Pattern, "exec.Command") {
		t.Errorf("pattern: got %q", hits[0].Pattern)
	}
}

// TestScanner_Method exercises the Scanner façade so the generate tool
// (Phase 5 Task 20) can hold a *scan.Scanner and call Scan directly.
func TestScanner_Method(t *testing.T) {
	_ = t.Context()
	files := map[string]string{
		"main.go": "package main\nfunc main(){ _ = exec.Command(\"ls\") }\n",
	}
	s := New()
	hits := s.Scan(files)
	if len(hits) != 1 {
		t.Fatalf("hits: want 1, got %d (%+v)", len(hits), hits)
	}
	if !strings.Contains(hits[0].Pattern, "exec.Command") {
		t.Errorf("pattern: got %q", hits[0].Pattern)
	}
}
