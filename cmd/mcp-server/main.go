// Command mcp-server starts the bond-trading-strategies MCP server over HTTP/SSE.
//
// Usage:
//
//	mcp-server [flags]
//
// Flags:
//
//	-addr          TCP address to listen on (default ":8080")
//	-base-url           Externally-reachable base URL (default "http://localhost:8080")
//	-strategy-base-url  Base URL for the strategy REST server (or STRATEGY_BASE_URL)
//	-fred-api-key       FRED API key; can also be set via the FRED_API_KEY env var
//
// The DORA_API_KEY environment variable is required. The server will not start
// without it and terminates immediately with an error if it is absent.
//
// The FRED API key is required only for FRED tools (fred_fetch_*). Strategy
// tools proxy requests to strategy-server over HTTP. For run-oriented
// questions, prefer the natural-language MCP tools strategy_run_status and
// strategy_run_describe; strategy_run_list and strategy_run_get return raw JSON.
//
// Example — local dev:
//
//	export DORA_API_KEY=your_dora_key
//	export FRED_API_KEY=your_fred_key
//	mcp-server -addr :8080 -base-url http://localhost:8080 -strategy-base-url http://localhost:8081
//
// The server exposes two HTTP endpoints:
//
//	GET  /sse      — SSE event stream (MCP client connects here)
//	POST /message  — JSON-RPC message endpoint
package main

import (
	"flag"
	"log"
	"os"

	mcpserver "github.com/dora-network/bond-trading-strategies/mcp"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address to listen on")
	baseURL := flag.String("base-url", "http://localhost:8080", "Externally-reachable base URL")
	strategyBaseURL := flag.String("strategy-base-url", "", "Base URL for strategy-server")
	fredKey := flag.String("fred-api-key", "", "FRED API key (or set FRED_API_KEY env var)")
	flag.Parse()

	// DORA_API_KEY is mandatory — strategy-server requires it on every request.
	doraAPIKey := os.Getenv("DORA_API_KEY")
	if doraAPIKey == "" {
		log.Fatalf("DORA_API_KEY environment variable is required but not set")
	}

	// Environment variable takes precedence over flag when flag is empty.
	if *strategyBaseURL == "" {
		*strategyBaseURL = os.Getenv("STRATEGY_BASE_URL")
	}
	if *fredKey == "" {
		*fredKey = os.Getenv("FRED_API_KEY")
	}

	if *strategyBaseURL == "" {
		log.Fatalf("strategy server base URL is required (set -strategy-base-url or STRATEGY_BASE_URL)")
	}
	if *fredKey == "" {
		log.Printf(
			"warning: no FRED API key provided. FRED tools will return errors when called.\n" +
				"  Set -fred-api-key flag or FRED_API_KEY environment variable.")
	}

	srv := mcpserver.NewSSEServer(*fredKey, doraAPIKey, *strategyBaseURL, *baseURL)

	log.Printf("bond-trading-strategies MCP server listening on %s (base URL: %s), strategy-server URL: %s", *addr, *baseURL, *strategyBaseURL)
	if err := srv.Start(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
