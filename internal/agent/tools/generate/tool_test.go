// Package generate implements the generate_strategy tool.
// This file holds the generate_strategy tool-handler tests (spec §6).
package generate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dora-network/bond-trading-strategies/internal/agent/llm"
	"github.com/dora-network/bond-trading-strategies/internal/agent/migration"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies"
	"github.com/dora-network/bond-trading-strategies/internal/agent/strategies/servertest"
)

// Phase 4 fixture: main.go + go.mod + a strategy source file (no
// *_test.go required). The validator expects exactly this shape.
const phase4OKInput = `{
	"module_name":"x",
	"summary":"x",
	"files":[
		{"path":"main.go","content":"package main\nfunc main() {}\n"},
		{"path":"go.mod","content":"module x\ngo 1.26.5\n"},
		{"path":"strategy.go","content":"package main\n// no-op strategy implementation\n"}
	],
	"rationale":"x"
}`

func TestTool_Generate_Success(t *testing.T) {
	v := &fakeValidator{results: []Result{{BuildOK: true, VetOK: true, TestsOK: true, GoVersion: "1.26.5"}}}
	r := NewRepairer(v, 2)
	h := NewHandler(r, nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1024 * 1024, MaxRepairs: 2})

	tools := h.Tools()
	var found bool
	for _, s := range tools {
		if s.Name == "generate_strategy" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("generate_strategy not found")
	}

	out, err := h.Invoke(t.Context(), "generate_strategy", json.RawMessage(phase4OKInput))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !strings.Contains(string(out), `"verified":true`) {
		t.Errorf("output: want verified:true, got %q", out)
	}
}

// TestTool_Generate_ToolSpec_Phase4: the tool description drops the
// legacy *_test.go requirement and points at the framework interface.
func TestTool_Generate_ToolSpec_Phase4(t *testing.T) {
	h := NewHandler(NewRepairer(&fakeValidator{}, 0), nil, migration.New(), Config{})
	tools := h.Tools()
	if len(tools) != 1 {
		t.Fatalf("Tools: want 1 spec, got %d", len(tools))
	}
	desc := tools[0].Description
	if strings.Contains(desc, "*_test.go") {
		t.Errorf("Phase 4 description must NOT require *_test.go, got %q", desc)
	}
	for _, kw := range []string{
		"strategy source file",
		"dorastrategy",
		"main.go",
		"go.mod",
	} {
		if !strings.Contains(desc, kw) {
			t.Errorf("Phase 4 description missing %q in %q", kw, desc)
		}
	}
}

func TestTool_Generate_InputRejected(t *testing.T) {
	v := &fakeValidator{}
	r := NewRepairer(v, 2)
	h := NewHandler(r, nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1024 * 1024, MaxRepairs: 2})

	input := json.RawMessage(`{
		"module_name":"x",
		"summary":"x",
		"files":[
			{"path":"go.mod","content":"module x\ngo 1.26.5\n"},
			{"path":"main_test.go","content":"package main\n"}
		],
		"rationale":"x"
	}`)
	out, err := h.Invoke(t.Context(), "generate_strategy", input)
	if err != nil {
		t.Fatalf("Invoke must NOT error on missing main.go (the model must be able to retry); got err=%v body=%s", err, out)
	}
	if !strings.Contains(string(out), `"verified":false`) {
		t.Errorf("output: want verified:false, got %q", out)
	}
}

func TestTool_Generate_SourcePatternHit(t *testing.T) {
	v := &fakeValidator{results: []Result{{BuildOK: true, VetOK: true, TestsOK: true}}}
	r := NewRepairer(v, 2)
	h := NewHandler(r, nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1024 * 1024, MaxRepairs: 2})

	// main.go uses net.Dial (a still-banned source-pattern) so the
	// scan short-circuits before ValidateInput. The os.Open pattern
	// was dropped because strategies need os.Getenv for config; net
	// access is the framework's responsibility. We keep a strategy.go
	// so the validator would pass if it ran.
	input := json.RawMessage(`{
		"module_name":"x",
		"summary":"x",
		"files":[
			{"path":"main.go","content":"package main\nimport \"net\"\nfunc main() { _, _ = net.Dial(\"tcp\", \"x\") }\n"},
			{"path":"go.mod","content":"module x\ngo 1.26.5\n"},
			{"path":"strategy.go","content":"package main\n"}
		],
		"rationale":"x"
	}`)
	out, err := h.Invoke(t.Context(), "generate_strategy", input)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	body := string(out)
	if !strings.Contains(body, "source_pattern") {
		t.Fatalf("output: want source_pattern error, got %q", body)
	}
	if v.calls != 0 {
		t.Errorf("validator must not run on a source-pattern hit; calls=%d", v.calls)
	}
}

func TestTool_Generate_LogicFailure(t *testing.T) {
	// A logic failure (compile/vet/test) returns a verified:false artifact
	// carrying the diagnostics, with no error, so the agent loop can hand it
	// back to the model for repair (spec §8).
	v := &fakeValidator{results: []Result{{
		BuildOK:     false,
		Diagnostics: []Diagnostic{{Category: "output", Message: "undefined: foo"}},
	}}}
	h := NewHandler(NewRepairer(v, 0), nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1024 * 1024, MaxRepairs: 2})
	out, err := h.Invoke(t.Context(), "generate_strategy", json.RawMessage(phase4OKInput))
	if err != nil {
		t.Fatalf("logic failure must not error (repairable); got %v", err)
	}
	if !strings.Contains(string(out), `"verified":false`) {
		t.Errorf("output: want verified:false, got %q", out)
	}
	if !strings.Contains(string(out), "undefined: foo") {
		t.Errorf("output: want the diagnostic carried through; got %q", out)
	}
}

// TestTool_Generate_InfraFailure covers the case where the validator
// returns an infrastructure-classified error (docker daemon down,
// image pull failure, OOM, vendor setup). The previous behaviour
// dropped the validator's error message on the floor and emitted a
// verified:false artifact with empty diagnostics, leaving the LLM and
// the operator with no record of what actually failed. The handler
// must fold the wrapped error into a category:infra diagnostic so the
// next repair attempt has the failure detail.
func TestTool_Generate_InfraFailure(t *testing.T) {
	v := &fakeValidator{err: errSentinel("validator: docker: connection refused")}
	h := NewHandler(NewRepairer(v, 0), nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1024 * 1024, MaxRepairs: 2})
	out, err := h.Invoke(t.Context(), "generate_strategy", json.RawMessage(phase4OKInput))
	if err != nil {
		t.Fatalf("infra failure must not bubble up; got %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"verified":false`) {
		t.Errorf("output: want verified:false, got %q", out)
	}
	if !strings.Contains(s, `"category":"infra"`) {
		t.Errorf("output: want category:infra diagnostic, got %q", out)
	}
	if !strings.Contains(s, "docker: connection refused") {
		t.Errorf("output: want validator error in diagnostic, got %q", out)
	}
}

// errSentinel is a tiny helper so the test can declare a typed error
// without pulling in a separate errors-as-values package.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }
func TestTool_Generate_ToolSpec(t *testing.T) {
	h := NewHandler(NewRepairer(&fakeValidator{}, 0), nil, migration.New(), Config{})
	tools := h.Tools()
	if len(tools) != 1 {
		t.Fatalf("Tools: want 1 spec, got %d", len(tools))
	}
	s := tools[0]
	if s.Name != "generate_strategy" {
		t.Errorf("Name: want generate_strategy, got %q", s.Name)
	}
	if len(s.Description) == 0 {
		t.Error("Description is empty")
	}
	if len(s.JSONSchema) == 0 {
		t.Error("JSONSchema is empty")
	}
}

// TestTool_Description_MentionsWASM guards Plan 4: the tool description
// names the wasm target and the schema carries wasm_manifest +
// wasm_files so the model emits them for go-wasm strategies.
func TestTool_Description_MentionsWASM(t *testing.T) {
	h := NewHandler(NewRepairer(&fakeValidator{}, 0), nil, migration.New(), Config{})
	tools := h.Tools()
	if len(tools) != 1 {
		t.Fatalf("Tools: want 1 spec, got %d", len(tools))
	}
	desc := tools[0].Description
	for _, kw := range []string{"wasm", "wasm_manifest", "wasm_files"} {
		if !strings.Contains(desc, kw) {
			t.Errorf("description missing %q", kw)
		}
	}
	schema := string(tools[0].JSONSchema)
	for _, kw := range []string{"wasm_manifest", "wasm_files"} {
		if !strings.Contains(schema, kw) {
			t.Errorf("schema missing %q", kw)
		}
	}
}

// TestTool_Generate_ManifestInjectedIntoFiles verifies that a base64
// wasm_manifest input is decoded and added to the file map as
// manifest.json, so the validator's files["manifest.json"] lookup
// succeeds without the LLM having to include a .json file in its
// input files array (which validate_input rejects on extension).
func TestTool_Generate_ManifestInjectedIntoFiles(t *testing.T) {
	const manifest = `{"schema_version":1,"module_name":"x","language":"tinygo","capabilities":{"order_books":["btcusd"],"host_functions":["host_log"]}}`
	encoded := base64.StdEncoding.EncodeToString([]byte(manifest))
	var gotFiles map[string]string
	v := &fakeValidator{capture: &gotFiles}
	input := `{"module_name":"x","summary":"s","files":[` +
		`{"path":"main.go","content":"package main\nfunc main() {}\n"},` +
		`{"path":"go.mod","content":"module x\ngo 1.26.5\n"},` +
		`{"path":"strategy.go","content":"package main\n// stub\n"}],` +
		`"rationale":"r","wasm_manifest":"` + encoded + `"}`
	h := NewHandler(NewRepairer(v, 0), nil, migration.New(), Config{MaxFiles: 50, MaxBytes: 1 << 20, MaxRepairs: 2})
	if _, err := h.Invoke(t.Context(), "generate_strategy", json.RawMessage(input)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got, ok := gotFiles["manifest.json"]; !ok {
		t.Fatalf("manifest.json not injected into files map; got keys %v", mapKeys(gotFiles))
	} else if got != manifest {
		t.Errorf("manifest.json content mismatch:\n got %q\nwant %q", got, manifest)
	}
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

const rebuildOwnerID = "00000000-0000-0000-0000-0000000000f1"

// seedRebuildFixture provisions a Postgres, an owner, a strategy, and a
// legacy (go-docker) head version so isLegacyRebuild fires. Returns the
// pool and strategy id.
func seedRebuildFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := servertest.StartPostgres(t)
	_, err := pool.Exec(t.Context(), `
		insert into agent.users (dora_user_id, tenant_id, roles)
		values ($1, 'test', ARRAY['TRADER'])
		on conflict (dora_user_id) do nothing`, rebuildOwnerID)
	require.NoError(t, err)
	strategyID := uuid.NewString()
	_, err = pool.Exec(t.Context(), `
		insert into agent.strategies (id, dora_user_id, name, source_session_id)
		values ($1, $2, 'rebuild', $3)`, strategyID, rebuildOwnerID, uuid.NewString())
	require.NoError(t, err)
	legacyRev := uuid.NewString()
	_, err = pool.Exec(t.Context(), `
		insert into agent.strategy_versions (revision, strategy_id, provider, model,
		    module_name, summary, rationale, validation, target)
		values ($1, $2, 'openai', 'gpt', 'legacy', 's', 'r', '{}'::jsonb, 'go-docker')`,
		legacyRev, strategyID)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `
		update agent.strategies set head_revision=$1 where id=$2`, legacyRev, strategyID)
	require.NoError(t, err)
	return pool, strategyID
}

func rebuildInput(strategyID, files string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"strategy_id":%q,"module_name":"x","summary":"y","rationale":"z","files":[%s]}`,
		strategyID, files))
}

// TestGenerateStrategy_RebuildReserveBlocks: when a rebuild is already
// reserved, a second generate_strategy call for the same strategy gets a
// RecoveryError pointing at get_strategy.
func TestGenerateStrategy_RebuildReserveBlocks(t *testing.T) {
	t.Parallel()
	pool, strategyID := seedRebuildFixture(t)
	mig := migration.New()
	require.NoError(t, mig.Reserve(rebuildOwnerID, strategyID))

	h := NewHandler(NewRepairer(&fakeValidator{}, 0), strategies.NewPgStore(pool), mig, Config{})
	_, err := h.ForUser(rebuildOwnerID)(t.Context(), "generate_strategy",
		rebuildInput(strategyID, `{"path":"main.go","content":"package main\n"}`))
	var rec llm.RecoveryError
	require.ErrorAs(t, err, &rec)
	require.Equal(t, "get_strategy", rec.RecoveryTool())
	mig.Release(rebuildOwnerID, strategyID)
}

// TestGenerateStrategy_RebuildReserveReleasesOnSuccess: after a call that
// gets past the reserve gate, the slot is released on return (a second
// Reserve succeeds).
func TestGenerateStrategy_RebuildReserveReleasesOnSuccess(t *testing.T) {
	t.Parallel()
	pool, strategyID := seedRebuildFixture(t)
	mig := migration.New()
	h := NewHandler(NewRepairer(&fakeValidator{}, 0), strategies.NewPgStore(pool), mig, Config{})

	_, _ = h.ForUser(rebuildOwnerID)(t.Context(), "generate_strategy",
		rebuildInput(strategyID, `{"path":"main.go","content":"package main\n"}`))

	require.NoError(t, mig.Reserve(rebuildOwnerID, strategyID))
	mig.Release(rebuildOwnerID, strategyID)
}

// TestGenerateStrategy_RebuildReserveReleasesOnFailure: an input-shape
// failure after the reserve still releases the slot.
func TestGenerateStrategy_RebuildReserveReleasesOnFailure(t *testing.T) {
	t.Parallel()
	pool, strategyID := seedRebuildFixture(t)
	mig := migration.New()
	h := NewHandler(NewRepairer(&fakeValidator{}, 0), strategies.NewPgStore(pool), mig, Config{})

	_, _ = h.ForUser(rebuildOwnerID)(t.Context(), "generate_strategy",
		rebuildInput(strategyID, ``))

	require.NoError(t, mig.Reserve(rebuildOwnerID, strategyID))
	mig.Release(rebuildOwnerID, strategyID)
}
