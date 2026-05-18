// Package mcpserver wires the bond-trading-strategies domain logic into an
// MCP server exposed over HTTP/SSE.
//
// The server registers two groups of tools:
//
//   - Strategy tools: configure and run the mean-reversion strategy engine,
//     and back-test it against historical observations — no network I/O.
//   - FRED tools: fetch live US Treasury yield data from the FRED API.
//
// Use New to build the underlying *server.MCPServer (useful in tests), and
// NewSSEServer to wrap it in the HTTP/SSE transport ready to call Start.
package mcpserver

import (
	"github.com/mark3labs/mcp-go/server"
)

const (
	serverName    = "bond-trading-strategies"
	serverVersion = "0.1.0"
)

// New creates the MCP server with all tools registered.
// fredAPIKey may be empty; FRED tools will return an error if called without one.
// doraAPIKey is forwarded as the Authorization header on every strategy-server request.
func New(fredAPIKey, doraAPIKey, strategyBaseURL string) *server.MCPServer {
	return newServer(fredAPIKey, doraAPIKey, strategyBaseURL, "")
}

// NewWithFREDBaseURL creates the MCP server with a custom FRED API base URL.
// This is primarily useful in tests that spin up a local mock FRED HTTP server.
func NewWithFREDBaseURL(fredAPIKey, doraAPIKey, strategyBaseURL, fredBaseURL string) *server.MCPServer {
	return newServer(fredAPIKey, doraAPIKey, strategyBaseURL, fredBaseURL)
}

func newServer(fredAPIKey, doraAPIKey, strategyBaseURL, fredBaseURL string) *server.MCPServer {
	s := server.NewMCPServer(
		serverName,
		serverVersion,
		server.WithToolCapabilities(true),
		server.WithRecovery(), // turn panics into tool errors, not crashes
	)

	registerStrategyTools(s, strategyBaseURL, doraAPIKey)
	registerFREDTools(s, fredAPIKey, fredBaseURL)

	return s
}

// NewSSEServer wraps the MCP server in an SSE transport.
// addr is the TCP address to listen on, e.g. ":8080".
// baseURL must be the externally reachable URL, e.g. "http://localhost:8080".
func NewSSEServer(fredAPIKey, doraAPIKey, strategyBaseURL, baseURL string) *server.SSEServer {
	mcpSrv := New(fredAPIKey, doraAPIKey, strategyBaseURL)
	return server.NewSSEServer(mcpSrv,
		server.WithBaseURL(baseURL),
		server.WithKeepAlive(true),
	)
}
