package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dora-network/bond-trading-strategies/notifications"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/ratelimit"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	"github.com/dora-network/bond-trading-strategies/strategy/copytrading"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	flag "github.com/spf13/pflag"
)

//nolint:funlen, mnd // main function with flag setup and orchestration
func main() {
	addr := flag.StringP("addr", "a", envOr("ADDR", ":8081"), "HTTP address to listen on")
	dbURL := flag.StringP("db-url", "d", envOr("DATABASE_URL", ""), "Postgres connection string (required)")
	wsURL := flag.StringP("ws-url", "s", envOr("WS_URL", "wss://dev.dora.co"), "WebSocket base URL")
	apiKey := flag.StringP("api-key", "k", envOr("WS_API_KEY", envOr("API_KEY", "")), "API key for the DORA WebSocket price feed")
	doraBaseURL := flag.StringP("dora-base-url", "b", envOr("DORA_BASE_URL", ""), "DORA HTTP base URL")
	fredAPIKey := flag.StringP("fred-api-key", "f", envOr("FRED_API_KEY", ""), "FRED API key")
	encryptionKeyHex := flag.StringP("encryption-key", "e", envOr("ENCRYPTION_KEY", ""),
		"32-byte AES-256 key (hex) for encrypting user API keys at rest")
	reconnectDelay := flag.DurationP("reconnect-delay", "r", 5*time.Second, "Delay between reconnect attempts") //nolint:mnd
	logLevel := flag.StringP("log-level", "l", "", "Log level (DEBUG, INFO, WARN, ERROR); overrides LOG_LEVEL env")
	corsAllowedOrigins := flag.String("cors-allowed-origins",
		envOr("CORS_ALLOWED_ORIGINS", ""),
		"Comma-separated list of allowed CORS origins; * allows all")

	// Rate-limiting flags
	rateLimitEnabled := flag.Bool("rate-limit", envOrBool("RATE_LIMIT", true), "Enable rate limiting")
	rateLimitReadRPS := flag.Float64("rate-limit-read-rps", envOrFloat("RATE_LIMIT_READ_RPS", 20), "Per-user read requests per second")
	rateLimitReadBurst := flag.Int("rate-limit-read-burst", envOrInt("RATE_LIMIT_READ_BURST", 40), "Per-user read burst capacity")
	rateLimitWriteRPS := flag.Float64("rate-limit-write-rps", envOrFloat("RATE_LIMIT_WRITE_RPS", 2), "Per-user write requests per second")
	rateLimitWriteBurst := flag.Int("rate-limit-write-burst", envOrInt("RATE_LIMIT_WRITE_BURST", 5), "Per-user write burst capacity")
	rateLimitGlobalRPS := flag.Float64("rate-limit-global-rps", envOrFloat("RATE_LIMIT_GLOBAL_RPS", 100), "Global requests per second")
	rateLimitGlobalBurst := flag.Int("rate-limit-global-burst", envOrInt("RATE_LIMIT_GLOBAL_BURST", 200), "Global burst capacity")
	rateLimitIPRPS := flag.Float64("rate-limit-ip-rps", envOrFloat("RATE_LIMIT_IP_RPS", 30), "Per-IP requests per second")
	rateLimitIPBurst := flag.Int("rate-limit-ip-burst", envOrInt("RATE_LIMIT_IP_BURST", 60), "Per-IP burst capacity")
	rateLimitTrustProxy := flag.Bool("rate-limit-trust-proxy",
		envOrBool("RATE_LIMIT_TRUST_PROXY", false),
		"Trust X-Forwarded-For for IP extraction")

	notificationsEnabled := flag.Bool("notifications-enabled", envOrBool("NOTIFICATIONS_ENABLED", true),
		"Enable the /v1/notifications/ws WebSocket endpoint")
	flag.Parse()

	setLogLevel(*logLevel)
	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "error: -db-url (or DATABASE_URL) is required")
		flag.Usage()
		os.Exit(1)
	}

	var encryptionKey []byte
	if *encryptionKeyHex != "" {
		var err2 error
		encryptionKey, err2 = hex.DecodeString(*encryptionKeyHex)
		if err2 != nil {
			fmt.Fprintln(os.Stderr, "error: --encryption-key must be a valid hex string")
			os.Exit(1)
		}
		if len(encryptionKey) != 32 {
			fmt.Fprintln(os.Stderr, "error: --encryption-key must decode to exactly 32 bytes (64 hex chars)")
			os.Exit(1)
		}
	}
	if *doraBaseURL != "" {
		if err := os.Setenv("DORA_BASE_URL", *doraBaseURL); err != nil {
			slog.Error("failed to set DORA_BASE_URL", "err", err)
			os.Exit(1)
		}
	}
	if *fredAPIKey != "" {
		if err := os.Setenv("FRED_API_KEY", *fredAPIKey); err != nil {
			slog.Error("failed to set FRED_API_KEY", "err", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		slog.Error("failed to connect to Postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	pricesHandler := prices.New(prices.Config{
		BaseURL: *wsURL,
		DBURL:   *dbURL,
		APIKey:  *apiKey,
	})
	pricesDaemon := streams.New(streams.Config{ReconnectDelay: *reconnectDelay})
	errCh := make(chan error, 2) //nolint:mnd
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := pricesDaemon.Run(ctx, pricesHandler.Stream); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Start the live trade stream for copy-trading runs.
	tradeStream := streams.NewTradeStream()
	if *apiKey != "" {
		doraClient := strategyhttp.NewDORAClient()
		obCtx := strategyhttp.WithAPIKey(ctx, *apiKey)
		orderbooks, err := doraClient.ListOrderBooks(obCtx)
		if err != nil {
			slog.Error("failed to list order books for trade stream", "err", err)
		} else {
			openBooks := make([]uuid.UUID, 0, len(orderbooks))
			for _, ob := range orderbooks {
				if ob.Status == "OPEN" {
					openBooks = append(openBooks, uuid.MustParse(ob.ID))
				}
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := tradeStream.Start(ctx, *wsURL, *apiKey, openBooks); err != nil && !errors.Is(err, context.Canceled) {
					errCh <- err
				}
			}()
		}
	}

	log := slog.With("service", "strategy-server")
	service := strategycore.NewService(strategycore.WithBaseContext(ctx))

	var notifier notifications.Notifier
	if *notificationsEnabled {
		bus := notifications.NewBus(
			notifications.NewPGLog(pool),
			notifications.NewHub(),
			notifications.WithLogger(func(format string, args ...any) { log.Info(format, args...) }),
		)
		notifier = bus
	}

	handlerImpl := strategyhttp.NewHandler(
		service,
		strategyhttp.WithRunStore(strategyhttp.NewPGRunStore(pool)),
		strategyhttp.WithBacktestStore(strategyhttp.NewPGBacktestStore(pool)),
		strategyhttp.WithTradesHistoryStore(copytrading.NewPGTradesHistoryStore(pool)),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithTradeStream(tradeStream),
		strategyhttp.WithLogger(log),
		strategyhttp.WithEncryptionKey(encryptionKey),
		strategyhttp.WithNotifier(notifier),
	)
	restorer, ok := handlerImpl.(interface{ RestoreRuns(context.Context) error })
	if !ok {
		slog.Error("strategy handler does not support run restore")
		os.Exit(1)
	}
	if err := restorer.RestoreRuns(ctx); err != nil {
		slog.Error("failed to restore strategy runs", "err", err)
		os.Exit(1)
	}

	backtestRestorer, ok := handlerImpl.(interface{ RestoreBacktests(context.Context) error })
	if !ok {
		slog.Error("strategy handler does not support backtest restore")
		os.Exit(1)
	}
	if err := backtestRestorer.RestoreBacktests(ctx); err != nil {
		slog.Error("failed to restore strategy backtests", "err", err)
		os.Exit(1)
	}

	rlCfg := ratelimit.Config{
		Enabled:    *rateLimitEnabled,
		TrustProxy: *rateLimitTrustProxy,
		Read:       ratelimit.TierConfig{RPS: *rateLimitReadRPS, Burst: *rateLimitReadBurst},
		Write:      ratelimit.TierConfig{RPS: *rateLimitWriteRPS, Burst: *rateLimitWriteBurst},
		Global:     ratelimit.TierConfig{RPS: *rateLimitGlobalRPS, Burst: *rateLimitGlobalBurst},
		IP:         ratelimit.TierConfig{RPS: *rateLimitIPRPS, Burst: *rateLimitIPBurst},
	}
	rl := ratelimit.NewLimiter(rlCfg, log)
	defer rl.Stop()
	wrappedHandler := rl.Middleware(handlerImpl)

	if *corsAllowedOrigins != "" {
		wrappedHandler = corsMiddleware(*corsAllowedOrigins, wrappedHandler)
	}

	if notifier != nil {
		wsSubMux := http.NewServeMux()
		wsSubMux.Handle("/v1/notifications/ws", notifications.NewHandler(
			notifier,
			func(ctx context.Context) (string, error) {
				if _, ok := strategyhttp.AuthInfoFromContext(ctx); !ok {
					return "", errors.New("missing auth info in context")
				}
				client := strategyhttp.NewDORAClient()
				return client.GetUserID(ctx)
			},
			notifications.WithHandlerLogger(log),
		))
		wrappedHandler = notificationsRouter{fallback: wrappedHandler, sub: wsSubMux}
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           wrappedHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:mnd
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("strategy server shutdown failed", "err", err)
		}
	}()

	slog.Info("strategy server starting", "addr", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("strategy server exited", "err", err)
		os.Exit(1)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	for err := range errCh {
		if err != nil {
			slog.Error("strategy server background worker exited", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("strategy server stopped")
}

// notificationsRouter dispatches /v1/notifications/ws to a sub-mux that
// owns the WebSocket route, and falls through to fallback for every other
// path. The sub-mux only registers the WS endpoint, so an unmatched
// request is also a 404 — the router treats that as a fall-through rather
// than letting the sub-mux emit its own 404, which would shadow the
// REST handler's responses for unrelated paths.
type notificationsRouter struct {
	fallback http.Handler
	sub      *http.ServeMux
}

func (r notificationsRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/v1/notifications/ws" {
		r.fallback.ServeHTTP(w, req)
		return
	}
	// Parse the Authorization header and put it into the request context
	// so the WebSocket handler's ResolveUserID callback can read it via
	// strategyhttp.AuthInfoFromContext.
	authHeader := req.Header.Get("Authorization")
	if ctx, ok := authContextFromHeader(req.Context(), authHeader); ok {
		req = req.WithContext(ctx)
	}
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	r.sub.ServeHTTP(rec, req)
	if rec.status == http.StatusNotFound {
		r.fallback.ServeHTTP(w, req)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

// authContextFromHeader returns a context that carries the credentials
// extracted from the supplied Authorization header, using the same
// ApiKey/Bearer scheme recognition as strategy/http.requireAuth. The
// second return value is false when the header is absent or unrecognised.
func authContextFromHeader(ctx context.Context, authHeader string) (context.Context, bool) {
	switch {
	case strings.HasPrefix(authHeader, "ApiKey "):
		key := strings.TrimPrefix(authHeader, "ApiKey ")
		if key == "" {
			return ctx, false
		}
		return strategyhttp.WithAPIKey(ctx, key), true
	case strings.HasPrefix(authHeader, "Bearer "):
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" {
			return ctx, false
		}
		return strategyhttp.WithBearerToken(ctx, token), true
	}
	return ctx, false
}

// corsMiddleware returns an HTTP handler that adds CORS headers. origins is a
// comma-separated list; a single "*" allows any origin.
func corsMiddleware(origins string, next http.Handler) http.Handler {
	allowed := make(map[string]bool)
	allowAll := false
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
		}
		allowed[o] = true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		headers := w.Header()

		if allowAll {
			headers.Set("Access-Control-Allow-Origin", "*")
		} else if allowed[origin] {
			headers.Set("Access-Control-Allow-Origin", origin)
			headers.Add("Vary", "Origin")
		}

		headers.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		headers.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		headers.Set("Access-Control-Allow-Credentials", "true")
		headers.Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func setLogLevel(flagValue string) {
	raw := flagValue
	if raw == "" {
		raw = os.Getenv("LOG_LEVEL")
	}
	if raw == "" {
		return
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		fmt.Fprintf(os.Stderr, "warning: invalid log level %q, using default\n", raw)
		return
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v == "true" || v == "1" || v == "yes"
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}

func envOrFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
		return fallback
	}
	return f
}
