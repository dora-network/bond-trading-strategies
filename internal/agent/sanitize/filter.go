// Package sanitize provides the first layer of input sanitisation for
// strategy turns: a cheap structural filter that runs in the HTTP handler
// before any provider call. It is Layer 1 of the four-layer sanitisation
// pass described in spec §16; it does not understand prompt content, it
// only rejects prompts that are clearly unfit for any LLM.
package sanitize

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Verdict is the result of a filter pass. VerdictOK means the prompt
// passed structurally; the rejection values form a closed set whose
// Category is recorded in audit_log detail.
type Verdict int8

const (
	VerdictOK Verdict = iota
	RejectOversize
	RejectControlChars
	RejectEmpty
)

// Category returns the audit_log detail category for a verdict:
// "oversize", "control_chars", "empty", or "" for OK.
func (v Verdict) Category() string {
	switch v {
	case RejectOversize:
		return "oversize"
	case RejectControlChars:
		return "control_chars"
	case RejectEmpty:
		return "empty"
	default:
		return ""
	}
}

// MaxPromptBytes is the byte cap applied by the filter. It matches
// the core AGENT_MAX_PROMPT_BYTES default (32 KiB) so layer 1 and
// layer 2 enforce the same limit.
const MaxPromptBytes = 32 * 1024

// Filter applies the structural checks and returns either VerdictOK or
// a rejection verdict with an explanatory error suitable as audit_log
// detail. Checks are ordered cheapest-first: size, emptiness, UTF-8
// validity, then a rune scan for NUL and zero-width characters.
func Filter(prompt string) (Verdict, error) {
	if len(prompt) > MaxPromptBytes {
		return RejectOversize, fmt.Errorf("prompt exceeds %d bytes", MaxPromptBytes)
	}
	if strings.TrimSpace(prompt) == "" {
		return RejectEmpty, errors.New("prompt is empty or whitespace")
	}
	if !utf8.ValidString(prompt) {
		return RejectControlChars, errors.New("prompt contains invalid UTF-8")
	}
	for _, r := range prompt {
		if r == 0 {
			return RejectControlChars, errors.New("prompt contains NUL byte")
		}
		// Zero-width characters that can be used to hide instructions.
		if r == '\u200B' || r == '\u200C' || r == '\u200D' || r == '\uFEFF' {
			return RejectControlChars, fmt.Errorf("prompt contains zero-width character U+%04X", r)
		}
	}
	return VerdictOK, nil
}
