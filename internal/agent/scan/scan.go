// Package scan runs the source-pattern red-flag check on a generated
// Go module before the validator container is invoked. See spec §16.4.
package scan

import (
	"regexp"
	"strings"
)

// Hit is one red-flag match: the offending file, 1-based line number, and
// the human-readable pattern category written to audit_log detail.
type Hit struct {
	File    string
	Line    int
	Pattern string
}

// pattern pairs a human-readable category name with its compiled regex.
type pattern struct {
	Name  string
	Regex *regexp.Regexp
}

// buildPatterns returns the closed set of red-flag patterns from spec
// §16.4. Scanner caches the result so the generate tool compiles the
// regexes once; the package-level Scan rebuilds per call (the scan runs
// once per generation turn, so the cost is negligible). Adding a
// pattern requires updating the spec and the test fixtures.
func buildPatterns() []pattern {
	return []pattern{
		{"exec.Command", regexp.MustCompile(`\bexec\.Command\b`)},
		{"os/exec", regexp.MustCompile(`\bos/exec\b`)},
		{"syscall.Exec", regexp.MustCompile(`syscall\.Exec`)},
		{"net.Dial", regexp.MustCompile(`\bnet\.Dial\b`)},
		{"net.Listen", regexp.MustCompile(`\bnet\.Listen\b`)},
		{"http.Get/Post/DefaultClient", regexp.MustCompile(`\bhttp\.(Get|Post|DefaultClient)\b`)},
		{"net/http", regexp.MustCompile(`\bnet/http\b`)},
		{"cgo import", regexp.MustCompile(`import "C"`)},
		{"cgo directive", regexp.MustCompile(`#cgo`)},
		{"plugin.Open", regexp.MustCompile(`plugin\.Open`)},
	}
}

// scanPatterns runs patterns against the file map and returns the first
// red-flag match per file per pattern. Files without a .go extension are
// ignored. It is the single loop shared by the package-level Scan and the
// Scanner.Scan method so the two call paths cannot drift.
func scanPatterns(files map[string]string, ps []pattern) []Hit {
	var hits []Hit
	for path, content := range files {
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		lines := strings.Split(content, "\n")
		matched := make(map[string]bool)
		for i, line := range lines {
			for _, p := range ps {
				if matched[p.Name] {
					continue
				}
				if p.Regex.MatchString(line) {
					hits = append(hits, Hit{File: path, Line: i + 1, Pattern: p.Name})
					matched[p.Name] = true
				}
			}
		}
	}
	return hits
}

// Scan returns the first red-flag match per file. Files without a .go
// extension are ignored. The pattern name in the hit is the human-readable
// category written to audit_log detail.
func Scan(files map[string]string) []Hit {
	return scanPatterns(files, buildPatterns())
}

// Scanner is the public façade for the package. It lets the generate tool
// (Phase 5 Task 20) hold a *scan.Scanner and call Scan directly without
// an interface.
type Scanner struct {
	patterns []pattern
}

// New returns a Scanner with the default pattern set.
func New() *Scanner { return &Scanner{patterns: buildPatterns()} }

// Scan runs the scanner with its patterns. It mirrors the package-level
// Scan so both call paths share one implementation.
func (s *Scanner) Scan(files map[string]string) []Hit {
	return scanPatterns(files, s.patterns)
}
