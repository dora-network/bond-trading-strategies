package http

import (
	"context"
	"net/http"
	"strings"

	"github.com/dora-network/bond-trading-strategies/authctx"
)

// TenantIDHeader is the request header that carries the DORA tenant
// identifier. When present on an authed request, the value is forwarded
// to DORA on every outbound call. When absent, no tenant-id header is
// attached — DORA decides whether the call requires one. The middleware
// does not 401 on a missing tenant-id; the Authorization header alone is
// enough to authenticate the inbound request.
const TenantIDHeader = "tenant-id"

type doraUserIDContextKey struct{}

// requireAuth is an HTTP middleware that:
//  1. Validates the Authorization header (returns 401 if absent or unrecognised).
//  2. Reads the tenant-id header if present and forwards it on outbound
//     DORA calls via authctx. A missing tenant-id is not an auth failure.
//  3. Calls resolveUserID — which contacts DORA — to confirm the credentials
//     belong to a real user (returns 401 if DORA rejects them).
//  4. Stores the parsed credentials in the request context via authctx
//     so that downstream handlers and the liveDORAClient can read them
//     using the same key, regardless of whether the request was
//     authenticated here or upstream (e.g. the WS router).
//  5. Stores the verified DORA user ID in the request context so that
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

		authInfo := authctx.AuthInfo{
			TenantID: strings.TrimSpace(r.Header.Get(TenantIDHeader)),
		}
		switch {
		case strings.HasPrefix(authHeader, "ApiKey "):
			key := strings.TrimPrefix(authHeader, "ApiKey ")
			if key == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty API key")
				return
			}
			authInfo.APIKey = key
		case strings.HasPrefix(authHeader, "Bearer "):
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" {
				writeError(w, http.StatusUnauthorized, "invalid Authorization header: empty bearer token")
				return
			}
			authInfo.BearerToken = token
		default:
			writeError(w, http.StatusUnauthorized, "invalid Authorization header: unsupported scheme")
			return
		}

		ctx := authctx.WithAuthInfo(r.Context(), authInfo)

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
