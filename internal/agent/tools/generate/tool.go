// Package generate implements the generate_strategy tool.
// This file holds the tool handler (spec §6): it validates input shape,
// runs the source-pattern red-flag scan, drives the repair state machine,
// and returns the artifact JSON for the runner to persist.
package generate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/scan"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
)

// debugToolDumpDir is the env var that gates on-disk persistence of
// every generate_strategy tool result. When unset (production), the tool
// runs with zero overhead. When set, each Invoke writes one JSON file
// under the directory capturing the full tool input bytes (what the LLM
// sent) and the full tool result bytes (what the runner will see), so an
// operator can grep what the LLM is generating when something goes wrong.
const debugToolDumpDir = "AGENT_DEBUG_TOOL_DIR"

// dumpArtifactDirPerm / dumpArtifactFilePerm are the perms used when the
// debug dumper writes a directory or a file. Named so gosec/mnd can see
// them as named constants instead of magic numbers.
const (
	dumpArtifactDirPerm  = 0o755
	dumpArtifactFilePerm = 0o600
)

// dumpAndReturn marshals payload to JSON, writes the input/result pair
// to the debug dump dir when AGENT_DEBUG_TOOL_DIR is set, and returns
// the marshalled bytes. The filesystem write is best-effort: errors are
// swallowed so the tool path is never affected by a broken disk.
func dumpAndReturn(name string, input []byte, payload any) ([]byte, error) {
	out, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	dumpToolArtifact(name, input, out)
	return out, nil
}

// dumpToolArtifact writes a single JSON file containing the tool input +
// result for the current Invoke, when AGENT_DEBUG_TOOL_DIR is set.
func dumpToolArtifact(name string, input, result []byte) {
	dir := os.Getenv(debugToolDumpDir)
	if dir == "" {
		return
	}
	//nolint:gosec // G703: operator env var, debug-only. TODO validate path.
	if err := os.MkdirAll(dir, dumpArtifactDirPerm); err != nil {
		return
	}
	fp := filepath.Join(dir,
		fmt.Sprintf("%s-%d.json", name, time.Now().UTC().UnixNano()))
	rec := struct {
		Timestamp string          `json:"timestamp"`
		Tool      string          `json:"tool"`
		Input     json.RawMessage `json:"input"`
		Result    json.RawMessage `json:"result"`
	}{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Tool:      name,
		Input:     json.RawMessage(input),
		Result:    json.RawMessage(result),
	}
	//nolint:gosec // G304/G306: operator dir, debug-only. TODO validate path.
	_ = os.WriteFile(fp, mustMarshalDebug(rec), dumpArtifactFilePerm)
}

// mustMarshalDebug is the dump helper's marshal-and-swallow. Keeps the
// dump helper allocation-free on the hot path and avoids leaking errors.
func mustMarshalDebug(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return []byte("{\"err\":\"" + err.Error() + "\"}")
	}
	return b
}

// Config is the generate_strategy handler's runtime configuration. The
// values come from cfg.Generate.* env vars in production (spec §13);
// tests pass them in directly.
type Config struct {
	MaxFiles   int
	MaxBytes   int
	MaxRepairs int
}

// fileInput is one entry in the generate_strategy files array.
type fileInput struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// generateInput is the typed input for generate_strategy.
type generateInput struct {
	ModuleName string      `json:"module_name"`
	Summary    string      `json:"summary"`
	Files      []fileInput `json:"files"`
	Rationale  string      `json:"rationale"`
	// WasmManifest is the base64-encoded manifest.json sidecar for
	// go-wasm strategies (Plan 4). Empty for legacy go-docker strategies.
	WasmManifest string `json:"wasm_manifest,omitempty"`
	// WasmFiles names the files the validator should compile to .wasm
	// (the TinyGo entry points). Empty for go-docker strategies.
	WasmFiles []string `json:"wasm_files,omitempty"`
	// StrategyID, when non-empty, signals "rebuild this strategy on
	// the current framework". The migrator reserves the in-flight
	// slot for the duration of the call so concurrent rebuilds don't
	// race for head. Empty when generating a brand-new strategy.
	StrategyID string `json:"strategy_id,omitempty"`
}

// artifactOutput is the typed output written to the assistant message
// body. The version capture reader parses this shape (spec §6, §17.1).
// ImageRef carries the validator's local-daemon image tag (Phase 3)
// so the capture observer can stamp it on the version row.
type artifactOutput struct {
	ModuleName string            `json:"module_name"`
	Summary    string            `json:"summary"`
	Files      map[string]string `json:"files"`
	Rationale  string            `json:"rationale"`
	Verified   bool              `json:"verified"`
	Validation validationOut     `json:"validation"`
	ImageRef   string            `json:"image_ref,omitempty"`
	// WasmRef/ManifestHash are the content-addressed references the
	// validator produces for go-wasm strategies (Plan 4). Empty for
	// go-docker. WasmManifest echoes the input manifest so the capture
	// observer persists it alongside the source.
	WasmRef      string   `json:"wasm_ref,omitempty"`
	ManifestHash string   `json:"manifest_hash,omitempty"`
	WasmManifest string   `json:"wasm_manifest,omitempty"`
	WasmFiles    []string `json:"wasm_files,omitempty"`
}

// validationOut is the validation summary embedded in the artifact. It
// mirrors validate.Result via the local Diagnostic/Result types.
type validationOut struct {
	BuildOK      bool         `json:"build_ok"`
	VetOK        bool         `json:"vet_ok"`
	TestsOK      bool         `json:"tests_ok"`
	GoVersion    string       `json:"go_version"`
	DurationMs   int64        `json:"duration_ms"`
	Diagnostics  []Diagnostic `json:"diagnostics"`
	ImageRef     string       `json:"image_ref,omitempty"`
	WasmRef      string       `json:"wasm_ref,omitempty"`
	ManifestHash string       `json:"manifest_hash,omitempty"`
}

// Handler is the generate_strategy tool. It validates input, runs the
// source-pattern scan, drives the repair state machine, and returns the
// artifact JSON for the runner to persist.
type Handler struct {
	repairer *Repairer
	versions strategies.Store
	migrator *migration.Migrator
	cfg      Config
	scanner  *scan.Scanner
}

// NewHandler returns a Handler with the given repairer, version store,
// migrator, and config. The source-pattern scanner is the default
// scan.New(). versions / migrator gate rebuilds of legacy rows: when the
// LLM passes strategy_id and the strategy's head is on a superseded
// framework, the migrator reserves the in-flight slot for the call.
func NewHandler(r *Repairer, versions strategies.Store, migrator *migration.Migrator, cfg Config) *Handler {
	return &Handler{repairer: r, versions: versions, migrator: migrator, cfg: cfg, scanner: scan.New()}
}

// ForUser binds the handler to userID and returns the llm.ToolHandler the
// tool factory registers, mirroring strategiestool.Handler / backtesttool.
// RunHandler. The rebuild reserve gate needs the caller's identity for the
// ownership check; Invoke (user-less) skips the gate.
func (h *Handler) ForUser(userID string) llm.ToolHandler {
	return func(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
		return h.invoke(ctx, userID, name, input)
	}
}

// isLegacyRebuild reports whether the generate_strategy call is
// rebuilding an existing strategy whose current head is on a
// superseded framework (target != go-wasm or empty WasmRef). The
// migrator reserves the slot in this case so concurrent rebuilds
// don't race for head.
//
// Reads via strategies.Store so the ownership gate matches the
// read tools; wrong-owner strategies surface as not-found and skip
// the reserve.
func isLegacyRebuild(ctx context.Context, versions strategies.Store, userID, strategyID string) bool {
	if strategyID == "" || versions == nil {
		return false
	}
	if _, err := versions.GetStrategy(ctx, userID, strategyID); err != nil {
		return false
	}
	headRev, err := versions.Head(ctx, strategyID)
	if err != nil || headRev == "" {
		return false
	}
	head, err := versions.GetVersion(ctx, strategyID, headRev)
	if err != nil {
		return false
	}
	return head.Target != "go-wasm" || head.WasmRef == ""
}

// WithScanner overrides the source-pattern scanner (used in tests to
// inject a fake). It mutates and returns the receiver.
func (h *Handler) WithScanner(s *scan.Scanner) *Handler {
	h.scanner = s
	return h
}

// Tools returns the single generate_strategy tool spec. The description and
// schema point at the strategy framework interface (spec §8, Phase 4): the
// model implements dorastrategy.Strategy {Init, OnCandle} on a strategy source
// file, plus a one-line main that calls dorastrategy.Run. The legacy
// *_test.go requirement is gone.
func (h *Handler) Tools() []llm.ToolSpec {
	return []llm.ToolSpec{{
		Name: "generate_strategy",
		Description: "Generate a runnable Go strategy module (go-wasm target) " +
			"that implements the " + frameworkImportPath + " framework. " +
			"**First call `get_strategy` to check whether the user already " +
			"has a built strategy — if so, call `run_backtest` directly " +
			"instead of regenerating.** Only generate when the user has no " +
			"strategy yet, asks for a new one, or the existing build is " +
			"missing. Inputs: module_name, summary, files (main.go, go.mod, " +
			"and at least one strategy source file (*.go) containing the " +
			"dorastrategy.Strategy implementation), rationale, wasm_manifest " +
			"(base64-encoded manifest.json declaring order_books, " +
			"host_functions, params_schema), and wasm_files (the file paths " +
			"the validator compiles to .wasm via TinyGo). The module is " +
			"validated in-process via TinyGo + wazero with --network=none; " +
			"on failure, sanitized diagnostics are returned for repair. On " +
			"success the response carries strategy_id and revision_id so " +
			"the next turn can call run_backtest directly. " +
			"If `get_strategy` returned `head.stale=true`, call " +
			"`read_strategy_sources(strategy_id, head.revision_id)` first to " +
			"read the legacy source, then pass those files through " +
			"`generate_strategy`.",
		JSONSchema: json.RawMessage(generateStrategySchema),
	}}
}

// generateStrategySchema is the typed JSON schema for generate_strategy
// (spec §6): module_name, summary, rationale, and an ordered files array
// of {path, content} records.
const generateStrategySchema = `{
  "type":"object",
  "required":["module_name","summary","files","rationale"],
  "properties":{
    "module_name":{"type":"string"},
    "summary":{"type":"string"},
    "rationale":{"type":"string"},
    "files":{
      "type":"array",
      "items":{
        "type":"object",
        "required":["path","content"],
        "properties":{
          "path":{"type":"string"},
          "content":{"type":"string"}
        }
      }
    },
    "wasm_manifest":{"type":"string","description":"base64-encoded manifest.json for the go-wasm target"},
    "wasm_files":{
      "type":"array",
      "items":{"type":"string"},
      "description":"file paths the validator compiles to .wasm (go-wasm target)"
    },
    "strategy_id":{
      "type":"string",
      "description":"Existing strategy id when rebuilding on a newer framework. Empty when creating a brand-new strategy."
    }
  }
}`

// Invoke dispatches a tool call by name. Only generate_strategy is
// registered; any other name is an error. The pipeline (spec §6): parse
// input, build the file map, validate input shape, run the source-pattern
// scan, drive the repair loop, and marshal the artifact JSON.
func (h *Handler) Invoke(ctx context.Context, name string, input json.RawMessage) (json.RawMessage, error) {
	return h.invoke(ctx, "", name, input)
}

func (h *Handler) invoke(ctx context.Context, userID, name string, input json.RawMessage) (json.RawMessage, error) {
	slog.Info("tool invoke start", "name", name, "input_len", len(input))
	if name != "generate_strategy" {
		return nil, fmt.Errorf("generate: unknown tool %q", name)
	}
	// Defensive: if the shim passes a zero-byte buffer (the LLM emitted
	// an empty tool_use input), return a structured error with the
	// expected shape. Otherwise json.Unmarshal on "" would yield
	// "unexpected end of JSON input" with no hint to the LLM.
	if len(bytes.TrimSpace(input)) == 0 {
		//nolint:lll
		return nil, fmt.Errorf(`generate: parse input: empty input. The tool expects a JSON object with fields: module_name (string), summary (string), files (array of {path, content}). Required file paths are main.go, go.mod, and at least one *_test.go`)
	}
	var in generateInput
	if err := json.Unmarshal(input, &in); err != nil {
		// LLM streaming can truncate the closing '}' on a tool_use
		// input (the SDK concatenates partial-JSON fragments and the
		// last fragment sometimes lacks the closing brace). Retry with
		// one appended '}' before surfacing the parse error.
		if strings.Contains(err.Error(), "unexpected end of JSON input") {
			if err2 := json.Unmarshal(append(append([]byte(nil), input...), '}'), &in); err2 == nil {
				err = nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("generate: parse input: %w", err)
		}
	}
	if isLegacyRebuild(ctx, h.versions, userID, in.StrategyID) {
		if err := h.migrator.Reserve(userID, in.StrategyID); err != nil {
			return nil, llm.NewRecoveryError(
				"generate_strategy: rebuild already in flight for this strategy_id; "+
					"call get_strategy to read the new revision_id, then retry the original request.",
				"get_strategy",
			)
		}
		defer h.migrator.Release(userID, in.StrategyID)
	}
	files := make(map[string]string, len(in.Files))
	for _, f := range in.Files {
		files[f.Path] = f.Content
	}
	// Inject the wasm manifest into the file map so the validator's
	// files["manifest.json"] lookup succeeds without making the LLM
	// include a .json file in the input — the validate_input extension
	// check rejects .json. The wasm_manifest field is base64-encoded;
	// for go-docker paths the field is empty and this branch is skipped.
	if in.WasmManifest != "" {
		decoded, derr := base64.StdEncoding.DecodeString(in.WasmManifest)
		if derr != nil {
			return nil, fmt.Errorf("generate: decode wasm_manifest: %w", derr)
		}
		files["manifest.json"] = string(decoded)
	}
	if err := ValidateInput(ctx, Input{
		ModuleName: in.ModuleName,
		Summary:    in.Summary,
		Files:      toFiles(in.Files),
		MaxFiles:   h.cfg.MaxFiles,
		MaxBytes:   h.cfg.MaxBytes,
	}); err != nil {
		// Input-shape failures (missing main.go, bad paths, oversize,
		// etc.) are model-recoverable, not infra. Return a
		// verified:false artifact so the agent loop surfaces the
		// message to the model instead of terminating the turn.
		// `validated:"input"` lets the model distinguish input-shape
		// failure from a real build/vet/test failure.
		out := artifactOutput{
			ModuleName: in.ModuleName,
			Summary:    in.Summary,
			Files:      files,
			Rationale:  in.Rationale,
			Verified:   false,
			Validation: validationOut{
				Diagnostics: []Diagnostic{{Category: "input", Message: err.Error()}},
			},
		}
		return dumpAndReturn(name, input, out)
	}
	if hits := h.scanner.Scan(files); len(hits) > 0 {
		body := map[string]any{
			"error":   "source_pattern",
			"file":    hits[0].File,
			"line":    hits[0].Line,
			"pattern": hits[0].Pattern,
		}
		return dumpAndReturn(name, input, body)
	}
	res, _, err := h.repairer.Run(ctx, files)
	out := validationOut{
		BuildOK: res.BuildOK, VetOK: res.VetOK, TestsOK: res.TestsOK,
		GoVersion: res.GoVersion, DurationMs: res.DurationMs,
		Diagnostics: res.Diagnostics, ImageRef: res.ImageRef,
		WasmRef: res.WasmRef, ManifestHash: res.ManifestHash,
	}
	// Infra-classified validator failures (docker daemon / image pull /
	// OOM / vendor setup) return ErrInfra with no Result. Without
	// surfacing the message here the LLM sees verified:false with
	// empty diagnostics and concludes "infrastructure failure" without
	// anything actionable, while the operator has no record of what
	// the validator actually said. Fold the wrapped error into a
	// diagnostic so the next retry attempt has the failure detail.
	// Preserve any existing diagnostics (logic failures carry their
	// own); the infra message is the only signal when the validator
	// returned zero-value Result alongside ErrInfra.
	if err != nil && len(out.Diagnostics) == 0 {
		out.Diagnostics = []Diagnostic{{Category: "infra", Message: err.Error()}}
	}
	return dumpAndReturn(name, input, artifactOutput{
		ModuleName:   in.ModuleName,
		Summary:      in.Summary,
		Files:        files,
		Rationale:    in.Rationale,
		Verified:     err == nil,
		Validation:   out,
		ImageRef:     res.ImageRef,
		WasmRef:      res.WasmRef,
		ManifestHash: res.ManifestHash,
		WasmManifest: in.WasmManifest,
		WasmFiles:    in.WasmFiles,
	})
}

// toFiles converts the JSON fileInput slice to the validate.File slice.
func toFiles(in []fileInput) []File {
	out := make([]File, len(in))
	for i, f := range in {
		out[i] = File(f)
	}
	return out
}
