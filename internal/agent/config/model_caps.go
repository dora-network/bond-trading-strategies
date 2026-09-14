package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ModelCaps describes per-model token budgets. Loaded from a JSON
// file at startup (path configurable via AGENT_MODEL_CAPS_PATH)
// so operators can add new model entries without a recompile.
type ModelCaps struct {
	// Default is the max output tokens applied when no model entry matches.
	Default int
	// DefaultContext is the assumed context window (input tokens) when no
	// model entry matches. Used for the compaction threshold. Defaults to
	// Default * 2 when unset (conservative estimate).
	DefaultContext int
	// Models is the per-model cap table. Order matters: the FIRST
	// entry whose substring match contains the lowercased model name
	// wins, so more-specific needles must come first (e.g.
	// "claude-sonnet-4-5" before "claude-sonnet-4").
	Models []ModelCap
}

// ModelCap is a single substring match: when the lowercased model
// name contains Match, the cap is MaxTokens and the context window
// is ContextWindow (0 means use MaxTokens * 2).
type ModelCap struct {
	Match         string `json:"match"`
	MaxTokens     int    `json:"max_tokens"`
	ContextWindow int    `json:"context_window,omitempty"`
}

// LoadModelCaps reads the JSON config at path and parses it into
// a ModelCaps. An empty path returns a ModelCaps with a 16384
// default and no per-model entries so every model resolves to the
// same budget -- the 16384 floor matches configs/model_caps.json
// and is safe for every commonly-deployed OpenAI model. A
// non-existent file is an error: a typo in the env var would
// otherwise silently fall through to the default and the operator
// would debug cryptic provider 400s instead of fixing the env.
// defaultModelContextWindow is the floor used when no caps file is
// configured; it matches configs/model_caps.json.
const defaultModelContextWindow = 16384

func LoadModelCaps(path string) (ModelCaps, error) {
	if path == "" {
		return ModelCaps{Default: defaultModelContextWindow}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ModelCaps{}, fmt.Errorf("read model caps at %q: %w", path, err)
	}
	var raw struct {
		Default        int        `json:"default"`
		DefaultContext int        `json:"default_context"`
		Models         []ModelCap `json:"models"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return ModelCaps{}, fmt.Errorf("parse model caps at %q: %w", path, err)
	}
	if raw.Default <= 0 {
		return ModelCaps{}, errors.New("model caps default must be > 0")
	}
	for i, m := range raw.Models {
		if m.Match == "" {
			return ModelCaps{}, fmt.Errorf("model caps entry %d has empty match", i)
		}
		if m.MaxTokens <= 0 {
			return ModelCaps{}, fmt.Errorf("model caps entry %q has non-positive max_tokens", m.Match)
		}
	}
	return ModelCaps{Default: raw.Default, DefaultContext: raw.DefaultContext, Models: raw.Models}, nil
}

// MaxTokensFor returns the cap for the named model. Empty model
// resolves to the default. Match is case-insensitive substring.
func (c ModelCaps) MaxTokensFor(model string) int {
	if model == "" {
		return c.Default
	}
	low := strings.ToLower(model)
	for _, m := range c.Models {
		if strings.Contains(low, strings.ToLower(m.Match)) {
			return m.MaxTokens
		}
	}
	return c.Default
}

// ContextWindowFor returns the context window for the named model.
// When the model has an explicit context_window, that value is used.
// Otherwise falls back to DefaultContext, or Default * 2 as a last resort.
func (c ModelCaps) ContextWindowFor(model string) int {
	if model == "" {
		return c.defaultContext()
	}
	low := strings.ToLower(model)
	for _, m := range c.Models {
		if strings.Contains(low, strings.ToLower(m.Match)) {
			if m.ContextWindow > 0 {
				return m.ContextWindow
			}
			return m.MaxTokens * defaultContextMultiplier
		}
	}
	return c.defaultContext()
}

// defaultContextMultiplier is used when no explicit context window
// is configured — the assumed context window is MaxTokens * this.
const defaultContextMultiplier = 2

func (c ModelCaps) defaultContext() int {
	if c.DefaultContext > 0 {
		return c.DefaultContext
	}
	return c.Default * defaultContextMultiplier
}
