// Command mcp-server starts the bond-trading-strategies MCP server over HTTP/SSE.
//
// Usage:
//
//	mcp-server [flags]
//
// Flags:
//
//	-a, --addr                TCP address to listen on (default ":8080")
//	-b, --base-url             Externally-reachable base URL (default "http://localhost:8080")
//	-s, --strategy-base-url    Base URL for the strategy REST server (or STRATEGY_BASE_URL)
//	-f, --fred-api-key         FRED API key (or set FRED_API_KEY env var)
//	-k, --dora-api-key         DORA API key (or set DORA_API_KEY env var)
//
// The FRED API key is required only for FRED tools (fred_fetch_*). Strategy
// tools proxy requests to strategy-server over HTTP. For run-oriented
// questions, prefer the natural-language MCP tools strategy_run_status and
// strategy_run_describe; strategy_run_list and strategy_run_get return raw JSON.
//
// Example — local dev:
//
//	mcp-server -a :8080 -b http://localhost:8080 -s http://localhost:8081 -f $FRED_API_KEY -k $DORA_API_KEY
//
// The server exposes two HTTP endpoints:
//
//	GET  /sse      — SSE event stream (MCP client connects here)
//	POST /message  — JSON-RPC message endpoint
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	mcpserver "github.com/dora-network/bond-trading-strategies/mcp"
	flag "github.com/spf13/pflag"
)

func main() {
	addr := flag.StringP("addr", "a", envOr("ADDR", ":8080"), "TCP address to listen on")
	baseURL := flag.StringP("base-url", "b", envOr("MCP_BASE_URL", "http://localhost:8080"), "Externally-reachable base URL")
	strategyBaseURL := flag.StringP("strategy-base-url", "s", envOr("STRATEGY_BASE_URL", ""), "Base URL for strategy-server")
	fredKey := flag.StringP("fred-api-key", "f", envOr("FRED_API_KEY", ""), "FRED API key")
	doraAPIKey := flag.StringP("dora-api-key", "k", envOr("DORA_API_KEY", ""), "DORA API key")
	flag.Parse()

	if *doraAPIKey == "" {
		fmt.Fprintln(os.Stderr, "error: --dora-api-key (or DORA_API_KEY) is required")
		flag.Usage()
		os.Exit(1)
	}

	if *strategyBaseURL == "" {
		fmt.Fprintln(os.Stderr, "error: --strategy-base-url (or STRATEGY_BASE_URL) is required")
		flag.Usage()
		os.Exit(1)
	}
	if *fredKey == "" {
		log.Printf(
			"warning: no FRED API key provided. FRED tools will return errors when called.\n" +
				"  Set --fred-api-key flag or FRED_API_KEY environment variable.")
	}

	srvCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mcpSrv, sseSrv := mcpserver.NewSSEServerWithNotifications(*fredKey, *doraAPIKey, *strategyBaseURL, *baseURL)
	wsURL := mcpserver.NotificationsWSURL(*strategyBaseURL)
	go func() {
		if err := mcpserver.StartNotificationsRelay(srvCtx, mcpSrv, wsURL, *doraAPIKey); err != nil &&
			srvCtx.Err() == nil {
			log.Printf("notifications relay stopped: %v", err)
		}
	}()

	log.Printf("bond-trading-strategies MCP server listening on %s (base URL: %s), strategy-server URL: %s", *addr, *baseURL, *strategyBaseURL)
	if err := sseSrv.Start(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
