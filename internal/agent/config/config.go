// Package config loads agent service configuration from the environment.
package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env-var names read by Load. Kept as exported constants so callers, error
// messages, and tests share one source of truth.
const (
	EnvAuthCacheTTL         = "DORA_AUTH_CACHE_TTL"
	EnvDoraBaseURL          = "DORA_BASE_URL"
	EnvDoraToolsEnabled     = "AGENT_DORA_TOOLS_ENABLED"
	EnvGenerateBaseImage    = "AGENT_GENERATE_BASE_IMAGE"
	EnvGenerateCPU          = "AGENT_GENERATE_CPU"
	EnvGenerateAllowlist    = "AGENT_GENERATE_ALLOWLIST"
	EnvGenerateGOPROXY      = "AGENT_GENERATE_GOPROXY"
	EnvGenerateMaxBytes     = "AGENT_GENERATE_MAX_BYTES"
	EnvGenerateMaxFiles     = "AGENT_GENERATE_MAX_FILES"
	EnvGenerateMaxRepairs   = "AGENT_GENERATE_MAX_REPAIRS"
	EnvGenerateMemory       = "AGENT_GENERATE_MEMORY"
	EnvGenerateStartTimeout = "AGENT_GENERATE_START_TIMEOUT"
	EnvGenerateTimeout      = "AGENT_GENERATE_TIMEOUT"
	EnvLLMMaxIters          = "AGENT_LLM_MAX_ITERS"
	EnvLLMTimeout           = "AGENT_LLM_TIMEOUT"
	EnvLogLevel             = "LOG_LEVEL"
	EnvMaxPromptBytes       = "AGENT_MAX_PROMPT_BYTES"
	EnvModelCapsPath        = "AGENT_MODEL_CAPS_PATH"
	EnvCORSAllowedOrigins   = "CORS_ALLOWED_ORIGINS"
	EnvRateLimitPerMin      = "AGENT_RATE_LIMIT_PER_MIN"
	// Live deployment runtime (orchestrator).
	EnvLiveMemoryLimit             = "AGENT_LIVE_MEMORY_LIMIT"
	EnvLiveRestartWindow           = "AGENT_LIVE_RESTART_WINDOW"
	EnvLiveMaxRestarts             = "AGENT_LIVE_MAX_RESTARTS"
	EnvWsBrokerURL                 = "AGENT_WSBROKER_URL"
	EnvCapturePendingRetention     = "AGENT_CAPTURE_PENDING_RETENTION"
	EnvCapturePendingSweepInterval = "AGENT_CAPTURE_PENDING_SWEEP_INTERVAL"
)

const (
	defaultRateLimitPerMin      = 20
	DefaultAuthCacheTTL         = 5 * time.Minute
	defaultMaxPromptBytes       = 32 * 1024
	defaultLLMMaxIters          = 50
	defaultLLMTimeout           = 10 * time.Minute
	defaultLogLevel             = "info"
	defaultGenerateBaseImage    = "golang:1.26.5-bookworm"
	defaultGenerateCPU          = 1.0
	defaultGenerateMemory       = "1Gi"
	defaultGenerateTimeout      = 60 * time.Second
	defaultGenerateStartTimeout = 15 * time.Second
	defaultGenerateMaxRepairs   = 2
	defaultGenerateMaxFiles     = 50
	defaultGenerateMaxBytes     = 1 << 20
	defaultGenerateGOPROXY      = "https://proxy.golang.org,direct"
	defaultModelCapsPath        = "configs/model_caps.json"
	// defaultGenerateAllowlist lists the module paths a generated
	// strategy may require. The framework module ships its own
	// dorastrategy package; generated strategies import it directly.
	defaultDoraClientModule  = "github.com/dora-network/dora-client-go"
	defaultFrameworkModule   = "github.com/dora-network/dora-agent-strategy"
	defaultGenerateAllowlist = defaultDoraClientModule + "," + defaultFrameworkModule
	defaultDoraToolsEnabled  = true
	// Live deployment runtime defaults (orchestrator).
	defaultLiveMemoryLimit = uint64(256 * 1024 * 1024) // 256 MiB per wazero instance
	// defaultCapturePendingRetention is how long a stashed failed
	// capture survives before the janitor sweeps it. 7 days covers
	// a long weekend retry; longer would silently bloat the table.
	defaultCapturePendingRetention     = 7 * 24 * time.Hour
	defaultCapturePendingSweepInterval = 5 * time.Minute
	defaultLiveRestartWindow           = 60 * time.Second
	defaultLiveMaxRestarts             = 3
)

// Config holds all agent service configuration.
type Config struct {
	DoraBaseURL      string
	AuthCacheTTL     time.Duration
	RateLimitPerMin  int
	MaxPromptBytes   int
	LLMMaxIters      int
	LLMTimeout       time.Duration
	LogLevel         string
	Generate         GenerateConfig
	DoraToolsEnabled bool
	// CORSAllowedOrigins is a list of origins that may call the API
	// from a browser. Empty (default) = CORS disabled. "*" = any origin.
	// Otherwise exact match against the Origin header. Comma-separated
	// in the AGENT_CORS_ALLOWED_ORIGINS env var.
	CORSAllowedOrigins []string
	// ModelCapsPath points at a JSON file with per-model max output
	// token caps loaded at startup. Operators update the file when
	ModelCapsPath string
	// AdminAddr is the admin listener bind (loopback only). Serves the
	// operator CLI surface (/admin/init, /admin/rotate-key, /admin/whoami).
	// AdminTokenHash is the optional base64 SHA-256 of the admin bearer
	// token. When set (AGENT_ADMIN_TOKEN_HASH), the server starts
	// pre-initialized and /admin/init returns 409.
	AdminTokenHash string
	// LiveMemoryLimit is the per-instance wazero memory cap for live
	// deployment plugins. Defaults to 256 MiB; live strategies on the
	// order side may need a higher cap.
	LiveMemoryLimit uint64
	// LiveRestartWindow is the sliding window for the per-deployment
	// restart budget. Defaults to 60s.
	LiveRestartWindow time.Duration
	// LiveMaxRestarts is the max restarts allowed within LiveRestartWindow
	// before the deployment is marked crashed. Defaults to 3.
	LiveMaxRestarts int
	// WsBrokerURL is the wsplex endpoint URL the live deployment
	// runtime subscribes to. When empty, defaults to
	// <DoraBaseURL>/plex (relative path concatenation). Set
	// explicitly to point at a separate wsplex host or to override
	// the path.
	WsBrokerURL string
	// CapturePendingRetention is the maximum age of a
	// strategy_capture_pending row before the in-process janitor
	// sweeps it. Defaults to 7 days. Stashes older than this are
	// considered abandoned (no retry expected).
	CapturePendingRetention time.Duration
	// CapturePendingSweepInterval is how often the janitor runs.
	// Defaults to 5 minutes. Should be much smaller than the
	// retention window so a brief DB outage doesn't lose stashes.
	CapturePendingSweepInterval time.Duration
}

type GenerateConfig struct {
	// BaseImage is the image used as the FROM line of the per-build Dockerfile
	// the validator (internal/tools/generate/validate) writes into its temp dir
	// before each strategy-validation pass. The validator then COPYs the
	// generated strategy + freshly-vendored deps on top of this image and runs
	// `go vet && go build && go test` inside it (spec §7). Defaults to
	// `golang:1.26.5-bookworm` (matches the repo's Go toolchain). Use the same
	// image series the strategy-generation handler expects to run against.
	BaseImage    string
	CPU          float64
	Memory       string
	Timeout      time.Duration
	StartTimeout time.Duration
	MaxRepairs   int
	MaxFiles     int
	MaxBytes     int
	// GOPROXY is passed to the validator's host `go mod tidy && go mod
	// vendor` step. Deps are public, so it defaults to the public Go module
	// proxy; set a private proxy or "off" as needed.
	GOPROXY string
	// Allowlist is the curated set of module paths a strategy may require;
	// the validator strips any other require line from the strategy's
	// go.mod. Defaults to the Dora client SDK.
	Allowlist []string
}

// Validate checks that the loaded Config holds semantically valid values
// (positive durations/limits, non-empty required fields, finite CPU). It
// does not re-check env-var parse errors — those are caught at read time in
// Load(). Validate is the single
// place callers go to assert "this Config is usable".
//
// problems collects the env-var names whose values are out of range or
// otherwise unusable; the empty slice means the config is valid.
func (c *Config) Validate() []string {
	var problems []string
	if c.DoraBaseURL == "" {
		problems = append(problems, EnvDoraBaseURL)
	}
	if c.AuthCacheTTL <= 0 {
		problems = append(problems, EnvAuthCacheTTL)
	}
	if c.RateLimitPerMin <= 0 {
		problems = append(problems, EnvRateLimitPerMin)
	}
	if c.MaxPromptBytes <= 0 {
		problems = append(problems, EnvMaxPromptBytes)
	}
	if c.LLMMaxIters <= 0 {
		problems = append(problems, EnvLLMMaxIters)
	}
	if c.LLMTimeout <= 0 {
		problems = append(problems, EnvLLMTimeout)
	}
	if c.Generate.BaseImage == "" {
		problems = append(problems, EnvGenerateBaseImage)
	}
	if c.Generate.CPU <= 0 || math.IsNaN(c.Generate.CPU) || math.IsInf(c.Generate.CPU, 0) {
		problems = append(problems, EnvGenerateCPU)
	}
	if c.Generate.Memory == "" {
		problems = append(problems, EnvGenerateMemory)
	}
	if c.Generate.Timeout <= 0 {
		problems = append(problems, EnvGenerateTimeout)
	}
	if c.Generate.StartTimeout <= 0 {
		problems = append(problems, EnvGenerateStartTimeout)
	}
	if c.Generate.MaxRepairs < 0 {
		problems = append(problems, EnvGenerateMaxRepairs)
	}
	if c.Generate.MaxFiles <= 0 {
		problems = append(problems, EnvGenerateMaxFiles)
	}
	if c.Generate.MaxBytes <= 0 {
		problems = append(problems, EnvGenerateMaxBytes)
	}
	if c.LiveMemoryLimit == 0 {
		problems = append(problems, EnvLiveMemoryLimit)
	}
	if c.LiveRestartWindow <= 0 {
		problems = append(problems, EnvLiveRestartWindow)
	}
	if c.LiveMaxRestarts < 0 {
		problems = append(problems, EnvLiveMaxRestarts)
	}
	if c.CapturePendingRetention <= 0 {
		problems = append(problems, EnvCapturePendingRetention)
	}
	if c.CapturePendingSweepInterval <= 0 {
		problems = append(problems, EnvCapturePendingSweepInterval)
	}
	if c.CapturePendingSweepInterval >= c.CapturePendingRetention {
		// A sweep interval at or above the retention means rows
		// could be deleted before they ever become "stale" — the
		// janitor would clear everything on its first tick.
		problems = append(problems, EnvCapturePendingSweepInterval+
			" must be < "+EnvCapturePendingRetention)
	}
	return problems
}

// Load reads configuration from the environment and validates required fields.
func Load() (Config, error) {
	var c Config
	var missing []string
	var badParse []string

	c.DoraBaseURL = os.Getenv(EnvDoraBaseURL)

	ttl, err := envDurationDefault(EnvAuthCacheTTL, DefaultAuthCacheTTL)
	if err != nil {
		badParse = append(badParse, EnvAuthCacheTTL)
	}
	c.AuthCacheTTL = ttl

	rateLimit, err := envIntDefault(EnvRateLimitPerMin, defaultRateLimitPerMin)
	if err != nil {
		badParse = append(badParse, EnvRateLimitPerMin)
	}
	c.RateLimitPerMin = rateLimit

	maxPrompt, err := envIntDefault(EnvMaxPromptBytes, defaultMaxPromptBytes)
	if err != nil {
		badParse = append(badParse, EnvMaxPromptBytes)
	}
	c.CORSAllowedOrigins = splitCSV(os.Getenv(EnvCORSAllowedOrigins))
	c.MaxPromptBytes = maxPrompt

	llmTimeout, err := envDurationDefault(EnvLLMTimeout, defaultLLMTimeout)
	if err != nil {
		badParse = append(badParse, EnvLLMTimeout)
	}
	c.LLMTimeout = llmTimeout

	c.LLMMaxIters, _ = envIntDefault(EnvLLMMaxIters, defaultLLMMaxIters)

	c.LogLevel = envDefault(EnvLogLevel, defaultLogLevel)
	gen, genBad := readGenerateEnv()
	c.Generate = gen
	badParse = append(badParse, genBad...)

	toolsEnabled, err := envBoolDefault(EnvDoraToolsEnabled, defaultDoraToolsEnabled)
	if err != nil {
		badParse = append(badParse, EnvDoraToolsEnabled)
	}
	c.DoraToolsEnabled = toolsEnabled

	c.ModelCapsPath = envDefault(EnvModelCapsPath, defaultModelCapsPath)
	if c.DoraBaseURL == "" {
		missing = append(missing, EnvDoraBaseURL)
	}

	// Live deployment runtime (orchestrator). The defaults
	// match what orchestrator.New applies itself; we surface them
	// here so operators can tune without rebuilding.
	liveMem, err := envUint64Default(EnvLiveMemoryLimit, defaultLiveMemoryLimit)
	if err != nil {
		badParse = append(badParse, EnvLiveMemoryLimit)
	}
	c.LiveMemoryLimit = liveMem
	liveWindow, err := envDurationDefault(EnvLiveRestartWindow, defaultLiveRestartWindow)
	if err != nil {
		badParse = append(badParse, EnvLiveRestartWindow)
	}
	c.LiveRestartWindow = liveWindow
	liveMax, err := envIntDefault(EnvLiveMaxRestarts, defaultLiveMaxRestarts)
	if err != nil {
		badParse = append(badParse, EnvLiveMaxRestarts)
	}
	c.LiveMaxRestarts = liveMax
	c.WsBrokerURL = os.Getenv(EnvWsBrokerURL)
	captureRetention, captureSweep, captureErrs := readCapturePendingConfig()
	badParse = append(badParse, captureErrs...)
	c.CapturePendingRetention = captureRetention
	c.CapturePendingSweepInterval = captureSweep

	badParse = append(badParse, c.Validate()...)

	var problems []string
	if len(missing) > 0 {
		problems = append(problems, "missing required env vars: "+strings.Join(missing, ", "))
	}
	if len(badParse) > 0 {
		problems = append(problems, "invalid value for env vars: "+strings.Join(badParse, ", "))
	}
	if len(problems) > 0 {
		return Config{}, fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return c, nil
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// readCapturePendingConfig parses the capture-pending janitor env
// vars in one place so Load() stays under the funlen budget. Returns
// the retention + sweep intervals and a slice of env-var names that
// failed to parse (caller appends to badParse).
func readCapturePendingConfig() (time.Duration, time.Duration, []string) {
	var badParse []string
	retention, err := envDurationDefault(EnvCapturePendingRetention, defaultCapturePendingRetention)
	if err != nil {
		badParse = append(badParse, EnvCapturePendingRetention)
	}
	sweep, err := envDurationDefault(EnvCapturePendingSweepInterval, defaultCapturePendingSweepInterval)
	if err != nil {
		badParse = append(badParse, EnvCapturePendingSweepInterval)
	}
	return retention, sweep, badParse
}

func envIntDefault(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envUint64Default(key string, def uint64) (uint64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDurationDefault(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func envFloatDefault(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

func envBoolDefault(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}

// splitCSV parses a comma-separated string into non-empty trimmed parts.
// Returns nil for empty input.
func splitCSV(v string) []string {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// readGenerateEnv parses the strategy generation env vars into a GenerateConfig,
// returning the config and the list of vars that failed to parse.
func readGenerateEnv() (GenerateConfig, []string) {
	var badParse []string
	var g GenerateConfig

	g.Memory = envDefault(EnvGenerateMemory, defaultGenerateMemory)
	g.BaseImage = envDefault(EnvGenerateBaseImage, defaultGenerateBaseImage)
	g.GOPROXY = envDefault(EnvGenerateGOPROXY, defaultGenerateGOPROXY)
	g.Allowlist = splitCSV(envDefault(EnvGenerateAllowlist, defaultGenerateAllowlist))

	cpu, err := envFloatDefault(EnvGenerateCPU, defaultGenerateCPU)
	if err != nil {
		badParse = append(badParse, EnvGenerateCPU)
	}
	g.CPU = cpu

	timeout, err := envDurationDefault(EnvGenerateTimeout, defaultGenerateTimeout)
	if err != nil {
		badParse = append(badParse, EnvGenerateTimeout)
	}
	g.Timeout = timeout

	startTimeout, err := envDurationDefault(EnvGenerateStartTimeout, defaultGenerateStartTimeout)
	if err != nil {
		badParse = append(badParse, EnvGenerateStartTimeout)
	}
	g.StartTimeout = startTimeout

	maxRepairs, err := envIntDefault(EnvGenerateMaxRepairs, defaultGenerateMaxRepairs)
	if err != nil {
		badParse = append(badParse, EnvGenerateMaxRepairs)
	}
	g.MaxRepairs = maxRepairs

	maxFiles, err := envIntDefault(EnvGenerateMaxFiles, defaultGenerateMaxFiles)
	if err != nil {
		badParse = append(badParse, EnvGenerateMaxFiles)
	}
	g.MaxFiles = maxFiles

	maxBytes, err := envIntDefault(EnvGenerateMaxBytes, defaultGenerateMaxBytes)
	if err != nil {
		badParse = append(badParse, EnvGenerateMaxBytes)
	}
	g.MaxBytes = maxBytes

	return g, badParse
}
