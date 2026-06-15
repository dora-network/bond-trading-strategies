// Package cors provides a small, dependency-free HTTP middleware that
// adds the CORS response headers needed for cross-origin browsers to
// call the strategy-server. The middleware is intentionally minimal:
// it does not parse the Access-Control-Request-Headers echo, does not
// support credentialed Allow-Origin: *, and does not implement a
// preflight cache beyond Max-Age.
//
// Origins: a comma-separated allow-list. A single "*" allows any
// origin and echoes the request's Origin header (so that
// Access-Control-Allow-Credentials can stay set to "true" — the CORS
// spec forbids the literal "*" + "true" combination, but every
// major CORS library implements the echo-Origin form and every
// modern browser accepts it). An empty string disables CORS entirely
// and the returned function is a pass-through.
package cors

import (
	"net/http"
	"strings"
)

// New returns an HTTP middleware that adds CORS headers to responses.
// origins is a comma-separated list; "*" allows any origin. An empty
// string disables CORS — the returned function calls next with no
// header mutation.
func New(origins string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool)
	allowAll := false
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		allowed[o] = true
	}

	if origins == "" {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			headers := w.Header()

			switch {
			case allowAll && origin != "":
				headers.Set("Access-Control-Allow-Origin", origin)
				headers.Add("Vary", "Origin")
			case allowAll:
				headers.Set("Access-Control-Allow-Origin", "*")
			case allowed[origin]:
				headers.Set("Access-Control-Allow-Origin", origin)
				headers.Add("Vary", "Origin")
			}

			headers.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			headers.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Sec-WebSocket-Protocol, Sec-WebSocket-Extensions")
			headers.Set("Access-Control-Allow-Credentials", "true")
			headers.Set("Access-Control-Max-Age", "86400")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// OriginPatterns parses the same input format as New and returns the
// values needed to configure coder/websocket.AcceptOptions. OriginPatterns
// lists the host patterns that the WS library should match against
// the request Origin. allowAll is true when the input contained a
// bare "*"; in that case the caller should set
// AcceptOptions.InsecureSkipVerify = true (and leave OriginPatterns
// empty) — the library's documentation recommends this over a "*"
// pattern entry because it is more visible at the call site.
//
// Entries are stripped of their URL scheme when present (e.g.
// "https://app.example.com" becomes "app.example.com"); the library
// re-adds the scheme before matching. Glob patterns (entries
// containing "*") are passed through unchanged after scheme stripping.
// The library uses path.Match for pattern matching, so "*" matches
// any sequence of non-"/" characters — origin hosts never contain
// "/", so this is the expected behaviour.
func OriginPatterns(origins string) (patterns []string, allowAll bool) {
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		// Strip scheme if present: "https://app.example.com" -> "app.example.com".
		// The library re-adds the scheme before pattern matching.
		if i := strings.Index(o, "://"); i >= 0 {
			o = o[i+3:]
		}
		patterns = append(patterns, o)
	}
	return patterns, allowAll
}
