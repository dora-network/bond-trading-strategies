// Command price-daemon connects to the DORA WebSocket price stream and saves
// every price update to the price_history Postgres table. It also discovers
// active order books via the DORA REST API and subscribes to candle streams
// for each one.
//
// Usage:
//
//	price-daemon [flags]
//
// Flags:
//
//	-w, --ws-url                  WebSocket base URL (env WS_URL)
//	-d, --db-url                  Postgres connection string, required (env DATABASE_URL)
//	-k, --dora-api-key            DORA API key (env DORA_API_KEY)
//	-b, --dora-base-url           DORA REST API base URL for order book discovery (env DORA_BASE_URL)
//	-a, --asset-id                Filter stream to a single asset UUID (env ASSET_ID)
//	-s, --since                   Only stream candles after this RFC3339 timestamp
//	-r, --reconnect-delay         Delay between reconnect attempts (default 5s)
//	-A, --http-addr               HTTP listen address for health server (env HTTP_ADDR)
//	-z, --health-stale-after      Duration after which stream/write activity is unhealthy (env HEALTH_STALE_AFTER)
//	-g, --health-startup-grace   Startup grace period before health requires activity (env HEALTH_STARTUP_GRACE)
//	-p, --health-db-ping         Enable database ping in health endpoint (env HEALTH_DB_PING)
//	-t, --health-db-ping-timeout  Database ping timeout for health endpoint (env HEALTH_DB_PING_TIMEOUT)
//
// Example:
//
//	price-daemon -d $DATABASE_URL -k $DORA_API_KEY -b https://dev.dora.co -w wss://dev.dora.co
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/dora-network/bond-trading-strategies/candles"
	"github.com/dora-network/bond-trading-strategies/dora"
	"github.com/dora-network/bond-trading-strategies/prices"
	"github.com/dora-network/bond-trading-strategies/streams"
	"github.com/jackc/pgx/v5/pgxpool"
	flag "github.com/spf13/pflag"
)

//nolint:funlen // main function with flag setup and orchestration
func main() {
	wsURL := flag.StringP("ws-url", "w", envOr("WS_URL", "wss://staging.dora.co"), "WebSocket base URL")
	dbURL := flag.StringP("db-url", "d", envOr("DATABASE_URL", ""), "Postgres connection string (required)")
	apiKey := flag.StringP("dora-api-key", "k", envOr("DORA_API_KEY", ""), "DORA API key")
	doraBaseURL := flag.StringP("dora-base-url", "b", envOr("DORA_BASE_URL", ""), "DORA REST API base URL for order book discovery")
	assetID := flag.StringP("asset-id", "a", envOr("ASSET_ID", ""), "Filter to single asset UUID")
	sinceStr := flag.StringP("since", "s", "", "Only stream candles after this RFC3339 timestamp")
	reconnectDelay := flag.DurationP("reconnect-delay", "r", 5*time.Second, "Delay between reconnect attempts") //nolint:mnd
	httpAddr := flag.StringP("http-addr", "A", envOr("HTTP_ADDR", ":8080"), "HTTP listen address for health server")
	healthStaleAfter := flag.DurationP("health-stale-after", "z", envOrDuration("HEALTH_STALE_AFTER", time.Minute), "Duration after which stream/write activity is unhealthy")       //nolint:lll
	healthStartupGrace := flag.DurationP("health-startup-grace", "g", envOrDuration("HEALTH_STARTUP_GRACE", time.Second*10), "Startup grace period before health requires activity") //nolint:lll,mnd
	healthDBPing := flag.BoolP("health-db-ping", "p", envOrBool("HEALTH_DB_PING", true), "Enable database ping in health endpoint")
	healthDBPingTimeout := flag.DurationP("health-db-ping-timeout", "t", envOrDuration("HEALTH_DB_PING_TIMEOUT", 2*time.Second), "Database ping timeout for health endpoint") //nolint:lll,mnd
	flag.Parse()

	if *dbURL == "" {
		fmt.Fprintln(os.Stderr, "error: --db-url (or DATABASE_URL) is required")
		flag.Usage()
		os.Exit(1)
	}

	if *apiKey == "" {
		fmt.Fprintln(os.Stderr, "error: --dora-api-key (or DORA_API_KEY) is required")
		flag.Usage()
		os.Exit(1)
	}

	var since time.Time
	if *sinceStr != "" {
		var err error
		since, err = time.Parse(time.RFC3339, *sinceStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --since value %q: %v\n", *sinceStr, err)
			os.Exit(1)
		}
	}

	// Discover active order books from the DORA API.
	doraClient := dora.NewClient(*apiKey, *doraBaseURL)
	orderBooks, err := doraClient.ListOrderBooks(context.Background(), "OPEN")
	if err != nil {
		slog.Error("failed to list order books from DORA", "err", err)
		os.Exit(1)
	}
	orderBookIDs := make([]string, 0, len(orderBooks))
	for _, ob := range orderBooks {
		orderBookIDs = append(orderBookIDs, ob.ID)
	}
	slog.Info("discovered order books", "count", len(orderBookIDs), "ids", orderBookIDs)

	cfg := prices.Config{
		BaseURL: *wsURL,
		DBURL:   *dbURL,
		APIKey:  *apiKey,
		AssetID: *assetID,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, *dbURL)
	if err != nil {
		slog.Error("failed to connect to Postgres", "err", err)
		os.Exit(1)
	}

	staleAfter := *healthStaleAfter
	if staleAfter <= 0 {
		staleAfter = maxDuration(15*time.Second, 2*(*reconnectDelay)) //nolint:mnd
	}
	startupGrace := *healthStartupGrace
	if startupGrace <= 0 {
		startupGrace = staleAfter
	}

	checker := newHealthChecker(
		time.Now(),
		startupGrace,
		staleAfter,
		len(orderBookIDs) > 0,
		*healthDBPing,
		*healthDBPingTimeout,
		pool.Ping,
	)

	var wg sync.WaitGroup
	errCh := make(chan error, 4) //nolint:mnd

	mux := http.NewServeMux()
	mux.Handle("/healthz", newHealthHandler(checker, time.Now))
	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("health server starting", "addr", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second) //nolint:mnd
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Setup and run Prices Stream
	pricesDaemon := streams.New(streams.Config{
		ReconnectDelay: *reconnectDelay,
	})
	pricesStore := prices.NewPGStore(pool)
	pricesHandler := prices.New(cfg, prices.WithMessageHook(func() {
		checker.markPriceStream(time.Now())
	}))
	pricesStoreSubscriber := prices.NewStoreSubscriber(
		pricesStore,
		pricesHandler.Subscribe,
		prices.WithWriteHook(func() {
			checker.markPriceWrite(time.Now())
		}),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("price stream starting", "ws_url", *wsURL)
		if err := pricesDaemon.Run(ctx, pricesHandler.Stream); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		slog.Info("price store subscriber starting")
		if err := pricesStoreSubscriber.Start(ctx); err != nil {
			errCh <- err
		}
	}()

	// Setup and run Candles Stream for all discovered order books.
	if len(orderBookIDs) > 0 {
		candlesCfg := candles.Config{
			BaseURL:      *wsURL,
			DBURL:        *dbURL,
			APIKey:       *apiKey,
			OrderBookIDs: orderBookIDs,
			Since:        since,
		}
		candlesStore := candles.NewPGStore(pool)
		candlesHandler := candles.New(candlesCfg, candlesStore, candles.WithMessageHook(func() {
			checker.markCandleStream(time.Now())
		}))
		candlesStoreSubscriber := candles.NewStoreSubscriber(
			candlesStore,
			candlesHandler.Subscribe,
			candles.WithWriteHook(func() {
				checker.markCandleWrite(time.Now())
			}),
		)
		candlesDaemon := streams.New(streams.Config{
			ReconnectDelay: *reconnectDelay,
		})

		wg.Add(1)
		go func() {
			defer wg.Done()

			slog.Info("candle store subscriber starting")
			if err := candlesStoreSubscriber.Start(ctx); err != nil {
				errCh <- err
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("candles stream starting", "order_books", orderBookIDs)
			if err := candlesDaemon.Run(ctx, candlesHandler.Stream); err != nil {
				errCh <- err
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("daemon exited with error", "err", err)
			os.Exit(1)
		}
	}
	slog.Info("price-daemon stopped")
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
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
