package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type stubResolver struct {
	calls atomic.Int32
	user  string
	err   error
}

func (s *stubResolver) Resolve(ctx context.Context, apiKey string) (string, error) {
	s.calls.Add(1)
	if s.err != nil {
		return "", s.err
	}
	return s.user, nil
}

func TestAuthCache_HitWithinTTL(t *testing.T) {
	t.Parallel()
	r := &stubResolver{user: "u1"}
	c := newAuthCache(r, 5*time.Minute)

	u, err := c.Resolve(context.Background(), "key1")
	if err != nil || u != "u1" {
		t.Fatalf("first call: got (%q, %v)", u, err)
	}
	u, err = c.Resolve(context.Background(), "key1")
	if err != nil || u != "u1" {
		t.Fatalf("second call: got (%q, %v)", u, err)
	}
	if got := r.calls.Load(); got != 1 {
		t.Fatalf("expected resolver called once, got %d", got)
	}
}

func TestAuthCache_MissAfterTTL(t *testing.T) {
	t.Parallel()
	r := &stubResolver{user: "u1"}
	c := newAuthCache(r, 1*time.Millisecond)

	if _, err := c.Resolve(context.Background(), "key1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := c.Resolve(context.Background(), "key1"); err != nil {
		t.Fatal(err)
	}
	if got := r.calls.Load(); got != 2 {
		t.Fatalf("expected resolver called twice after TTL, got %d", got)
	}
}

func TestAuthCache_PropagatesError(t *testing.T) {
	t.Parallel()
	r := &stubResolver{err: errors.New("dora unreachable")}
	c := newAuthCache(r, 5*time.Minute)

	_, err := c.Resolve(context.Background(), "key1")
	if err == nil {
		t.Fatal("expected error from resolver to propagate")
	}
	if got := r.calls.Load(); got != 1 {
		t.Fatalf("expected resolver called once on error, got %d", got)
	}
}

func TestAuthCache_DifferentKeysAreIndependent(t *testing.T) {
	t.Parallel()
	r := &stubResolver{user: "u1"}
	c := newAuthCache(r, 5*time.Minute)

	if _, err := c.Resolve(context.Background(), "keyA"); err != nil {
		t.Fatal(err)
	}
	// Switch the resolver's user for the next key; cache must NOT serve
	// keyA's result for keyB.
	r.user = "u2"
	u, err := c.Resolve(context.Background(), "keyB")
	if err != nil {
		t.Fatal(err)
	}
	if u != "u2" {
		t.Fatalf("expected u2 for keyB, got %q (cache cross-pollution)", u)
	}
	if got := r.calls.Load(); got != 2 {
		t.Fatalf("expected resolver called twice (once per key), got %d", got)
	}
}

func TestAuthCache_ErrorIsNotCached(t *testing.T) {
	t.Parallel()
	r := &stubResolver{err: errors.New("transient")}
	c := newAuthCache(r, 5*time.Minute)

	_, err := c.Resolve(context.Background(), "key1")
	if err == nil {
		t.Fatal("expected error")
	}
	// Switch resolver to success; cache must NOT short-circuit on the
	// cached error.
	r.err = nil
	r.user = "u1"
	u, err := c.Resolve(context.Background(), "key1")
	if err != nil || u != "u1" {
		t.Fatalf("expected u1 after error resolves, got (%q, %v)", u, err)
	}
	if got := r.calls.Load(); got != 2 {
		t.Fatalf("expected resolver called twice (error then retry), got %d", got)
	}
}
