package authctx_test

import (
	"context"
	"testing"

	"github.com/dora-network/bond-trading-strategies/authctx"
)

func TestWithAndReadAPIKey(t *testing.T) {
	ctx := authctx.WithAPIKey(context.Background(), "k1")
	info, ok := authctx.AuthInfoFromContext(ctx)
	if !ok {
		t.Fatal("expected auth info to be present")
	}
	if info.APIKey != "k1" {
		t.Errorf("got APIKey %q, want %q", info.APIKey, "k1")
	}
}

func TestWithAndReadBearerToken(t *testing.T) {
	ctx := authctx.WithBearerToken(context.Background(), "t1")
	info, ok := authctx.AuthInfoFromContext(ctx)
	if !ok {
		t.Fatal("expected auth info to be present")
	}
	if info.BearerToken != "t1" {
		t.Errorf("got BearerToken %q, want %q", info.BearerToken, "t1")
	}
}

func TestAbsentContextReturnsFalse(t *testing.T) {
	if _, ok := authctx.AuthInfoFromContext(context.Background()); ok {
		t.Error("expected no auth info on plain context")
	}
}
