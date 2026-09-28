package secrets

import (
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes

	plaintext := []byte("dora.abc123.my-secret-api-key")
	ciphertext, err := Encrypt(plaintext, key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Ciphertext must differ from plaintext.
	if string(ciphertext) == string(plaintext) {
		t.Fatal("ciphertext should not equal plaintext")
	}

	decrypted, err := Decrypt(ciphertext, key)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Fatalf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptWithWrongKeyFails(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")
	wrongKey := []byte("fedcba9876543210fedcba9876543210")

	ciphertext, err := Encrypt([]byte("secret"), key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	_, err = Decrypt(ciphertext, wrongKey)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key, got nil")
	}
}

func TestDecryptTruncatedCiphertextFails(t *testing.T) {
	t.Parallel()
	key := []byte("0123456789abcdef0123456789abcdef")

	_, err := Decrypt([]byte("short"), key)
	if err == nil {
		t.Fatal("expected error for truncated ciphertext, got nil")
	}
}
