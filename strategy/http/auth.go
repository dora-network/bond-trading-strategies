package http

import (
	"context"
	"net/http"
	"strings"

	"github.com/dora-network/bond-trading-strategies/authctx"
)

type doraUserIDContextKey struct{}

// requireAuth is an HTTP middleware that:
//  1. Validates the Authorization header (returns 401 if absent or unrecognised).
//  2. Calls resolveUserID — which contacts DORA — to confirm the credentials
//     belong to a real user (returns 401 if DORA rejects them).
//  3. Stores the parsed credentials in the request context via authctx
//     so that downstream handlers and the liveDORAClient can read them
//     using the same key, regardless of whether the request was
//     authenticated here or upstream (e.g. the WS router).
//  4. Stores the verified DORA user ID in the request context so that
//     downstream handlers can retrieve it without making additional DORA
//     calls.
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

		var ctx context.Context
		switch {
		case strings.HasPrefix(authHeader, "ApiKey "):
			key := strings.TrimPrefix(authHeader, "ApiKey ")
			if key == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty API key")
				return
			}
			ctx = authctx.WithAPIKey(r.Context(), key)
		case strings.HasPrefix(authHeader, "Bearer "):
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty bearer token")
				return
			}
			ctx = authctx.WithBearerToken(r.Context(), token)
		default:
			writeError(w, http.StatusUnauthorized, "invalid Authorization header: unsupported scheme")
			return
		}

		userID, err := resolveUserID(ctx)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorised")
			return
		}

		ctx = context.WithValue(ctx, doraUserIDContextKey{}, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// doraUserIDFromContext retrieves the DORA user ID stored in ctx by requireAuth.
// The second return value is false when no user ID is present.
func doraUserIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(doraUserIDContextKey{}).(string)
	return id, ok && id != ""
}
