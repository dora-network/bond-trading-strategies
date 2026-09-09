// Package generate implements the generate_strategy tool.
// This file holds the input-validation logic (spec §6, spec §8 prompt
// redesign): the model authors a set of files and the host validates them
// before handing the file map to the per-build validator. Validation is
// pure (no I/O): it checks the in-memory file map against the path, size,
// and presence rules described in the spec.
package generate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Default caps from spec §13 (AGENT_GENERATE_MAX_FILES, AGENT_GENERATE_MAX_BYTES).
const (
	defaultMaxFiles = 50
	defaultMaxBytes = 1 << 20 // 1 MiB
)

// File is a single {path, content} record from the generate_strategy input.
type File struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// Input is the validated view of a generate_strategy tool call. MaxFiles and
// MaxBytes default when zero, matching the env vars in spec §13.
type Input struct {
	ModuleName string
	Summary    string
	Files      []File
	MaxFiles   int
	MaxBytes   int
}

// frameworkImportPath is the canonical module path the generated strategy
// must import. surfacing this in the prompt and the validator's error
// message keeps the string in one place.
const frameworkImportPath = "github.com/dora-network/dora-agent-strategy"

// ValidateInput checks a generate_strategy input against the spec §6 rules
// and the Phase 4 prompt redesign:
//
//   - non-empty module name and summary
//   - file count and combined-size caps
//   - no path traversal or absolute paths
//   - mandatory presence of main.go and go.mod
//   - at least one *.go strategy source file beyond main.go (the *_test.go
//     requirement was dropped in Phase 4; a strategy that is _only_ tests
//     would not compile to a runnable binary, so it is rejected too)
//
// It returns the first violation as a descriptive error, or nil when the
// input is valid.
func ValidateInput(_ context.Context, in Input) error {
	if strings.TrimSpace(in.ModuleName) == "" {
		return errors.New("module_name is required")
	}
	if strings.TrimSpace(in.Summary) == "" {
		return errors.New("summary is required")
	}

	maxFiles := in.MaxFiles
	if maxFiles == 0 {
		maxFiles = defaultMaxFiles
	}
	if len(in.Files) > maxFiles {
		return fmt.Errorf("too many files: %d exceeds limit of %d", len(in.Files), maxFiles)
	}

	maxBytes := in.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	var total int
	hasMain, hasGoMod, hasStrategySrc := false, false, false
	for _, f := range in.Files {
		total += len(f.Content)
		if total > maxBytes {
			return fmt.Errorf("combined file size exceeds limit of %d bytes", maxBytes)
		}
		if err := validatePath(f.Path); err != nil {
			return err
		}
		switch {
		case f.Path == "main.go":
			hasMain = true
		case f.Path == "go.mod":
			hasGoMod = true
		case strings.HasSuffix(f.Path, "_test.go"):
			// allowed but does not count as a strategy source file
		case strings.HasSuffix(f.Path, ".go"):
			hasStrategySrc = true
		}
	}
	if !hasMain {
		return errors.New("main.go is required")
	}
	if !hasGoMod {
		return errors.New("go.mod is required")
	}
	if !hasStrategySrc {
		return fmt.Errorf(
			"at least one strategy source file (*.go) is required\n"+
				"hint: import %q and implement dorastrategy.Strategy {Init, OnCandle}", frameworkImportPath,
		)
	}
	return nil
}

// validatePath rejects traversal, absolute paths, and disallowed extensions.
// Every path must live under the module root (spec §6, §11).
func validatePath(p string) error {
	if p == "" {
		return errors.New("empty file path")
	}
	if strings.HasPrefix(p, "/") {
		return errors.New("path must not be absolute")
	}
	if strings.HasPrefix(p, "..") || strings.Contains(p, "/../") || strings.HasSuffix(p, "/..") {
		return errors.New("path must not contain '..' traversal")
	}
	ext := filepath.Ext(p)
	if !hasAllowedExtension(ext) {
		return fmt.Errorf("path %q uses disallowed extension %q", p, ext)
	}
	return nil
}

// hasAllowedExtension reports whether ext is in the spec §6/§11 allowlist
// (.go, .mod, .sum, .md). Kept as a function rather than a package global so
// the allowlist is not a mutable value.
func hasAllowedExtension(ext string) bool {
	switch ext {
	case ".go", ".mod", ".sum", ".md":
		return true
	}
	return false
}
