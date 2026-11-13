package ksink

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
)

// newTxnInstance creates an immutable per-transaction generation ID that
// matches the writer's single-component filename rules.
func newTxnInstance(id string) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate transaction instance: %w", err)
	}
	name := filepath.Base(filepath.Clean(id))
	if name == "." || name == ".." || name == string(filepath.Separator) {
		name = "_"
	}
	return name + "-" + hex.EncodeToString(nonce[:]), nil
}
