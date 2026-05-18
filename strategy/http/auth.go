package http

import (
	"context"
	"net/http"
	"strings"
)

type authContextKey struct{}
type doraUserIDContextKey struct{}

// authInfo holds the parsed Authorization header credentials extracted by
// requireAuth. Exactly one of APIKey or BearerToken will be non-empty.
type authInfo struct {
	// APIKey is populated when the Authorization header carries the "ApiKey" prefix.
	APIKey string
	// BearerToken is populated when the Authorization header carries the "Bearer" prefix.
	BearerToken string
}

// requireAuth is an HTTP middleware that:
//  1. Validates the Authorization header (returns 401 if absent or unrecognised).
//  2. Calls resolveUserID — which contacts DORA — to confirm the credentials
//     belong to a real user (returns 401 if DORA rejects them).
//  3. Stores the parsed authInfo and the verified DORA user ID in the request
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

		var info authInfo
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

		// Store authInfo first so that resolveUserID (and the underlying DORA
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

// authFromContext retrieves the authInfo stored in ctx by requireAuth.
// The second return value is false when no auth info is present.
func authFromContext(ctx context.Context) (authInfo, bool) {
	info, ok := ctx.Value(authContextKey{}).(authInfo)
	return info, ok
}

// doraUserIDFromContext retrieves the DORA user ID stored in ctx by requireAuth.
// The second return value is false when no user ID is present.
func doraUserIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(doraUserIDContextKey{}).(string)
	return id, ok && id != ""
}
