package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dora-network/bond-trading-strategies/prices"
	strategycore "github.com/dora-network/bond-trading-strategies/strategy"
	strategyhttp "github.com/dora-network/bond-trading-strategies/strategy/http"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/jackc/pgx/v5/pgxpool"
	flag "github.com/spf13/pflag"
)

//nolint:funlen // main function with flag setup and orchestration
func main() {
	addr := flag.StringP("addr", "a", envOr("ADDR", ":8081"), "HTTP address to listen on")
	dbURL := flag.StringP("db-url", "d", envOr("DATABASE_URL", ""), "Postgres connection string (required)")
	wsURL := flag.StringP("ws-url", "s", envOr("WS_URL", "wss://dev.dora.co"), "WebSocket base URL")
	apiKey := flag.StringP("api-key", "k", envOr("WS_API_KEY", envOr("API_KEY", "")), "API key for the DORA WebSocket price feed")
	doraBaseURL := flag.StringP("dora-base-url", "b", envOr("DORA_BASE_URL", ""), "DORA HTTP base URL")
	fredAPIKey := flag.StringP("fred-api-key", "f", envOr("FRED_API_KEY", ""), "FRED API key")
	reconnectDelay := flag.DurationP("reconnect-delay", "r", 5*time.Second, "Delay between reconnect attempts") //nolint:mnd
	logLevel := flag.StringP("log-level", "l", "", "Log level (DEBUG, INFO, WARN, ERROR); overrides LOG_LEVEL env")
	flag.Parse()

	setLogLevel(*logLevel)
	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "error: -db-url (or DATABASE_URL) is required")
		flag.Usage()
		os.Exit(1)
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

	log := slog.With("service", "strategy-server")
	service := strategycore.NewService(strategycore.WithBaseContext(ctx))
	handlerImpl := strategyhttp.NewHandler(
		service,
		strategyhttp.WithRunStore(strategyhttp.NewPGRunStore(pool)),
		strategyhttp.WithBacktestStore(strategyhttp.NewPGBacktestStore(pool)),
		strategyhttp.WithPricesHandler(pricesHandler),
		strategyhttp.WithLogger(log),
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

	server := &http.Server{
		Addr:              *addr,
		Handler:           handlerImpl,
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
