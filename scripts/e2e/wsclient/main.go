// Command wsclient connects to the strategy-server's
// /v1/notifications/ws endpoint and prints received events as JSON, one
// per line on stdout. Diagnostic output goes to stderr.
//
// This binary is used by scripts/e2e/notifications-websocket.sh.
//
// Environment:
//
//	WS_BASE_URL        required. HTTP base URL of the strategy-server,
//	                   e.g. http://localhost:8081. The /v1/notifications/ws
//	                   path is appended.
//	WS_LAST_EVENT_ID   optional. UUIDv7 of the last event the caller has
//	                   already seen. When set, the client passes it as
//	                   the Last-Event-ID query param so the server
//	                   replays the missed history before live delivery.
//	WS_READ_FOR        optional, default 10s. Go duration string. The
//	                   client exits cleanly when this elapses.
//
// Authorization is fixed to "ApiKey <DORA_API_KEY>".
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/coder/websocket"
)

const (
	exitCodeFailure = 1
	exitCodeError   = 2
)

func main() {
	apiKey := os.Getenv("DORA_API_KEY")
	base := os.Getenv("WS_BASE_URL")
	lastID := os.Getenv("WS_LAST_EVENT_ID")
	readFor := os.Getenv("WS_READ_FOR")
	if readFor == "" {
		readFor = "10s"
	}
	d, err := time.ParseDuration(readFor)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad WS_READ_FOR:", err)
		os.Exit(exitCodeError)
	}

	u, err := url.Parse(base + "/v1/notifications/ws")
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad base url:", err)
		os.Exit(exitCodeError)
	}
	if lastID != "" {
		q := u.Query()
		q.Set("Last-Event-ID", lastID)
		u.RawQuery = q.Encode()
	}

	header := http.Header{}
	header.Set("Authorization", "ApiKey "+apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	//nolint:bodyclose // coder/websocket docs: caller never closes resp.Body
	conn, _, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial error:", err)
		os.Exit(exitCodeFailure)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	fmt.Fprintln(os.Stderr, "connected")
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			// Clean exit on context deadline.
			return
		}
		var evt map[string]any
		if err := json.Unmarshal(data, &evt); err != nil {
			fmt.Fprintln(os.Stderr, "rx (invalid json):", string(data))
			continue
		}
		fmt.Fprintln(os.Stderr, string(data))
	}
}
