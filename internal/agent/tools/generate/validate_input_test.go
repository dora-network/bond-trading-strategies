package generate

import (
	"strings"
	"testing"
)

// file is a minimal {path, content} record matching the generate_strategy
// tool input. Tests build small modules from these.
type file struct {
	Path    string
	Content string
}

func fileSet(fs ...file) []File {
	out := make([]File, len(fs))
	for i, f := range fs {
		out[i] = File(f)
	}
	return out
}

// okFiles is the canonical fixture for a Phase-4 module: main.go +
// go.mod + a single strategy source file. No *_test.go is required.
func okFiles() []File {
	return fileSet(
		file{Path: "main.go", Content: "package main\nfunc main() {}\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
		file{Path: "strategy.go", Content: "package main\n\n// strategy implementation here\n"},
	)
}

func TestValidateInput_MissingMain(t *testing.T) {
	files := fileSet(
		file{Path: "go.mod", Content: "module example\n"},
		file{Path: "strategy.go", Content: "package main\n"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil || !strings.Contains(err.Error(), "main.go") {
		t.Fatalf("expected error mentioning main.go, got %v", err)
	}
}

func TestValidateInput_MissingGoMod(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "strategy.go", Content: "package main\n"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil || !strings.Contains(err.Error(), "go.mod") {
		t.Fatalf("expected error mentioning go.mod, got %v", err)
	}
}

// TestValidateInput_OK_NoTest asserts the Phase 4 contract: a
// framework-conforming module with main.go + go.mod + a strategy *.go
// source file is valid. The legacy `*_test.go` requirement is gone.
func TestValidateInput_OK_NoTest(t *testing.T) {
	if err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      okFiles(),
	}); err != nil {
		t.Fatalf("Phase 4 module must validate without *_test.go: %v", err)
	}
}

// TestValidateInput_RejectsMissingStrategyFile: a module with only
// main.go + go.mod has no strategy implementation, so the validator must
// reject it even though the legacy rules passed.
func TestValidateInput_RejectsMissingStrategyFile(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil {
		t.Fatal("validator must reject a module with no strategy source file")
	}
	if !strings.Contains(err.Error(), "strategy source") {
		t.Errorf("error must mention 'strategy source', got %q", err)
	}
}

// TestValidateInput_RejectsTestOnly: a module that is only tests
// compiles to a runnable binary but produces no decisions. The Phase 4
// rewrite drops the mandate that *_test.go exists, but a *_test.go in
// lieu of a real strategy file is still rejected.
func TestValidateInput_RejectsTestOnly(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
		file{Path: "main_test.go", Content: "package main\nimport \"testing\"\nfunc TestMain_(t *testing.T) {}\n"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil {
		t.Fatal("validator must reject a test-only module")
	}
	if !strings.Contains(err.Error(), "strategy source") {
		t.Errorf("error must mention 'strategy source', got %q", err)
	}
}

// TestValidateInput_ErrorMentionsFrameworkImport: the validator's
// missing-strategy error hints the framework module path so the model can
// copy the import into the next retry.
func TestValidateInput_ErrorMentionsFrameworkImport(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "github.com/dora-network/dora-agent-strategy") {
		t.Errorf("error should hint the framework import path, got %q", err)
	}
}

func TestValidateInput_PathTraversal(t *testing.T) {
	files := append(okFiles(),
		File{Path: "../etc/passwd", Content: "root"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil || !strings.Contains(err.Error(), "path") {
		t.Fatalf("expected error mentioning path, got %v", err)
	}
}

func TestValidateInput_TooManyFiles(t *testing.T) {
	files := make([]File, 0, 51)
	for i := 0; i < 51; i++ {
		files = append(files, File{Path: "f.go", Content: "package x\n"})
	}
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
		MaxFiles:   50,
	})
	if err == nil || !strings.Contains(err.Error(), "too many files") {
		t.Fatalf("expected error mentioning too many files, got %v", err)
	}
}

func TestValidateInput_TooLarge(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
		file{Path: "strategy.go", Content: "package main\n" + strings.Repeat("x", 2<<20)},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
		MaxBytes:   1 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "combined file size") {
		t.Fatalf("expected error mentioning combined file size, got %v", err)
	}
}

func TestValidateInput_OK(t *testing.T) {
	if err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      okFiles(),
	}); err != nil {
		t.Fatalf("ValidateInput: %v", err)
	}
}

func TestValidateInput_DisallowedExtension(t *testing.T) {
	files := fileSet(
		file{Path: "main.go", Content: "package main\n"},
		file{Path: "go.mod", Content: "module example\n\ngo 1.26.5\n"},
		file{Path: "strategy.go", Content: "package main\n"},
		file{Path: "evil.bin", Content: "xxxx"},
	)
	err := ValidateInput(t.Context(), Input{
		ModuleName: "example",
		Summary:    "a strategy",
		Files:      files,
	})
	if err == nil || !strings.Contains(err.Error(), "extension") {
		t.Fatalf("expected error mentioning extension, got %v", err)
	}
}
