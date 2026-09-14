// Package secrets provides Seal/Open helpers for the agent runtime's
// at-rest secrets (server_dora_credentials.api_key,
// provider_configs.api_key). The agent wiring constructs a *Sealer
// once at startup with the service-wide ENCRYPTION_KEY, and stores
// that pointer in the agent runtime. Code that needs to seal or
// open a secret takes the *Sealer as a constructor argument or
// pulls it from a context-scoped accessor.
package secrets

import (
	"github.com/dora-network/bond-trading-strategies/internal/secrets"
)

// Sealer encrypts and decrypts agent secrets with a fixed 32-byte
// AES-256 key (the service-wide ENCRYPTION_KEY). It is safe for
// concurrent use because it holds no mutable state.
type Sealer struct {
	key []byte
}

// NewSealer constructs a Sealer with the given key. The key is
// copied so the caller may overwrite its buffer immediately after.
func NewSealer(key []byte) *Sealer {
	return &Sealer{key: append([]byte(nil), key...)}
}

// Seal encrypts plaintext using the service-wide key. Returns a
// nonce-prefixed ciphertext (the format produced by internal/secrets.Encrypt).
func (s *Sealer) Seal(plaintext []byte) ([]byte, error) {
	return secrets.Encrypt(plaintext, s.key)
}

// Open decrypts a ciphertext produced by Seal.
func (s *Sealer) Open(sealed []byte) ([]byte, error) {
	return secrets.Decrypt(sealed, s.key)
}
