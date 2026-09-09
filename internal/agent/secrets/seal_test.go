package secrets

import (
	"bytes"
	"testing"
)

// testKey is a deterministic 32-byte key used by the tests in this
// package. The real key is plumbed in by the agent wiring at startup;
// tests bypass that plumbing.
var testKey = bytes.Repeat([]byte{0x42}, 32)

func newTestSealer() *Sealer { return NewSealer(testKey) }

func TestSealOpen_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newTestSealer()
	plaintext := []byte("a dora api key, base64 or similar")
	sealed, err := s.Seal(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(sealed, plaintext) {
		t.Fatal("sealed output equals plaintext")
	}
	opened, err := s.Open(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", opened, plaintext)
	}
}

func TestSeal_DifferentCiphertextEachTime(t *testing.T) {
	t.Parallel()
	s := newTestSealer()
	plaintext := []byte("same input, different ciphertexts")
	s1, err := s.Seal(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := s.Seal(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(s1, s2) {
		t.Fatal("two seals of the same plaintext produced the same ciphertext (nonce reuse?)")
	}
}

func TestSealOpen_EmptyPlaintext(t *testing.T) {
	t.Parallel()
	s := newTestSealer()
	sealed, err := s.Seal([]byte{})
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := s.Open(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(opened) != 0 {
		t.Fatalf("expected empty plaintext, got %d bytes", len(opened))
	}
}

func TestSealOpen_LargePlaintext(t *testing.T) {
	t.Parallel()
	s := newTestSealer()
	plaintext := bytes.Repeat([]byte("x"), 1024*1024) // 1 MiB
	sealed, err := s.Seal(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := s.Open(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("large round-trip mismatch")
	}
}

func TestNewSealer_CopiesKey(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x33}, 32)
	s := NewSealer(key)
	// Mutate the caller's buffer; the Sealer must not see the change.
	for i := range key {
		key[i] = 0x00
	}
	plaintext := []byte("verify")
	sealed, err := s.Seal(plaintext)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := s.Open(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("sealer must hold its own copy of the key, not borrow the caller's buffer")
	}
}
