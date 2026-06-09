package http

import (
	"context"
	"net/http"
	"strings"
)

type authContextKey struct{}
type doraUserIDContextKey struct{}

// AuthInfo holds the parsed Authorization header credentials extracted by
// requireAuth. Exactly one of APIKey or BearerToken will be non-empty.
type AuthInfo struct {
	// APIKey is populated when the Authorization header carries the "ApiKey" prefix.
	APIKey string
	// BearerToken is populated when the Authorization header carries the "Bearer" prefix.
	BearerToken string
}

// requireAuth is an HTTP middleware that:
//  1. Validates the Authorization header (returns 401 if absent or unrecognised).
//  2. Calls resolveUserID — which contacts DORA — to confirm the credentials
//     belong to a real user (returns 401 if DORA rejects them).
//  3. Stores the parsed AuthInfo and the verified DORA user ID in the request
//     context so that downstream handlers can retrieve them without making
//     additional DORA calls.
//
// Recognised schemes:
//
//	Authorization: ApiKey <key>
//	Authorization: Bearer <token>
func requireAuth(resolveUserID func(context.Context) (string, error), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeError(w, http.StatusUnauthorized, "missing Authorization header")
			return
		}

		var info AuthInfo
		switch {
		case strings.HasPrefix(authHeader, "ApiKey "):
			key := strings.TrimPrefix(authHeader, "ApiKey ")
			if key == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty API key")
				return
			}
			info.APIKey = key
		case strings.HasPrefix(authHeader, "Bearer "):
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty bearer token")
				return
			}
			info.BearerToken = token
		default:
			writeError(w, http.StatusUnauthorized, "invalid Authorization header: unsupported scheme")
			return
		}

		// Store AuthInfo first so that resolveUserID (and the underlying DORA
		// client) can read the credentials from context when making its request.
		ctx := context.WithValue(r.Context(), authContextKey{}, info)

		userID, err := resolveUserID(ctx)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorised")
			return
		}

		ctx = context.WithValue(ctx, doraUserIDContextKey{}, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authFromContext retrieves the AuthInfo stored in ctx by requireAuth.
// The second return value is false when no auth info is present.
func authFromContext(ctx context.Context) (AuthInfo, bool) {
	info, ok := ctx.Value(authContextKey{}).(AuthInfo)
	return info, ok
}

// AuthInfoFromContext is the exported counterpart of authFromContext. It
// allows request handlers (e.g. the WebSocket notifications endpoint)
// that sit outside the standard REST auth middleware to retrieve the
// credentials stored in the request context by middleware that wraps
// them. The second return value is nil when no auth info is present.
func AuthInfoFromContext(ctx context.Context) (*AuthInfo, bool) {
	info, ok := authFromContext(ctx)
	if !ok {
		return nil, false
	}
	return &info, true
}

// WithAPIKey returns a context that carries the given API key for DORA
// authentication. It is intended for server startup code that needs to
// make DORA API calls outside of an HTTP request context.
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, authContextKey{}, AuthInfo{APIKey: apiKey})
}

// WithBearerToken returns a context that carries the given bearer token
// for DORA authentication. The token is forwarded as the DORA access
// token on subsequent API calls.
func WithBearerToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, authContextKey{}, AuthInfo{BearerToken: token})
}

// doraUserIDFromContext retrieves the DORA user ID stored in ctx by requireAuth.
// The second return value is false when no user ID is present.
func doraUserIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(doraUserIDContextKey{}).(string)
	return id, ok && id != ""
}
