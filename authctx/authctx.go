// Package authctx is a leaf package that owns the request-context
// types and helpers used to carry DORA credentials (API key or bearer
// token) across package boundaries. It exists as a separate package so
// that both strategy/http and notifications can read and write
// credentials on the same request context without forming an import
// cycle.
package authctx

import "context"

type contextKey struct{}

// AuthInfo holds the parsed Authorization header credentials extracted by
// requireAuth. Exactly one of APIKey or BearerToken will be non-empty.
type AuthInfo struct {
	// APIKey is populated when the Authorization header carries the "ApiKey" prefix.
	APIKey string
	// BearerToken is populated when the Authorization header carries the "Bearer" prefix.
	BearerToken string
}

// AuthInfoFromContext returns the AuthInfo stored in ctx and true when
// present, or (nil, false) when no credentials are present.
func AuthInfoFromContext(ctx context.Context) (*AuthInfo, bool) {
	info, ok := ctx.Value(contextKey{}).(AuthInfo)
	if !ok {
		return nil, false
	}
	return &info, true
}

// WithAPIKey returns a context that carries the given API key for DORA
// authentication. It is intended for server startup code that needs to
// make DORA API calls outside of an HTTP request context.
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, contextKey{}, AuthInfo{APIKey: apiKey})
}

// WithBearerToken returns a context that carries the given bearer token
// for DORA authentication. The token is forwarded as the DORA access
// token on subsequent API calls.
func WithBearerToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, contextKey{}, AuthInfo{BearerToken: token})
}
