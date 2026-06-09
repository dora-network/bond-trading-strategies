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
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/mark3labs/mcp-go/server"

	"github.com/dora-network/bond-trading-strategies/notifications"
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

// NewSSEServerWithNotifications builds the SSE server plus returns the
// underlying *server.MCPServer so the caller can run
// StartNotificationsRelay against the same instance. The SSEServer type
// does not expose the MCPServer it wraps (mcp-go keeps the field
// unexported), so the only way to forward events to MCP clients is for
// the caller to hold the MCPServer reference directly.
//
// Pair with StartNotificationsRelay in main to forward strategy-server
// lifecycle events to every connected MCP client as `notifications/event`
// JSON-RPC notifications (MCP leaves the `notifications/*` namespace open
// for app-defined methods).
func NewSSEServerWithNotifications(
	fredAPIKey, doraAPIKey, strategyBaseURL, baseURL string,
) (*server.MCPServer, *server.SSEServer) {
	mcpSrv := New(fredAPIKey, doraAPIKey, strategyBaseURL)
	sseSrv := server.NewSSEServer(mcpSrv,
		server.WithBaseURL(baseURL),
		server.WithKeepAlive(true),
	)
	return mcpSrv, sseSrv
}

// StartNotificationsRelay dials the strategy-server's
// /v1/notifications/ws endpoint and forwards every received Event to all
// MCP clients connected to mcpSrv via SendNotificationToAllClients. The
// method used is `notifications/event`; the params are the Event encoded
// as a map[string]any so the MCP serialiser includes the fields
// (NotificationParams.AdditionalFields has `json:"-"`).
//
// The relay runs until ctx is cancelled. Callers are expected to invoke
// this in a goroutine.
func StartNotificationsRelay(
	ctx context.Context,
	mcpSrv *server.MCPServer,
	wsURL, doraAPIKey string,
) error {
	authHeader := "ApiKey " + doraAPIKey
	client := notifications.NewClient(wsURL, authHeader,
		func(_ context.Context) (string, error) { return authHeader, nil },
		notifications.ClientOnEvent(func(_ context.Context, evt notifications.Event) error {
			params, err := eventToMap(evt)
			if err != nil {
				return err
			}
			mcpSrv.SendNotificationToAllClients("notifications/event", params)
			return nil
		}),
	)
	return client.Run(ctx)
}

// toWSURL converts an http(s) base URL to its ws(s) equivalent with the
// given path. An unparseable input yields an empty string.
func toWSURL(httpURL, path string) string {
	u, err := url.Parse(httpURL)
	if err != nil || u.Scheme == "" {
		return ""
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return ""
	}
	u.Path = path
	return u.String()
}

// NotificationsWSURL returns the WebSocket URL for the strategy-server's
// notification stream given the strategy-server's HTTP base URL.
func NotificationsWSURL(strategyBaseURL string) string {
	return toWSURL(strings.TrimRight(strategyBaseURL, "/"), "/v1/notifications/ws")
}

func eventToMap(evt notifications.Event) (map[string]any, error) {
	data, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
