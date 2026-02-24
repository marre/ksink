package ksrv

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// randReader is the source of randomness, overridable in tests.
var randReader io.Reader = rand.Reader

func parseScramClientFirst(msg string) (username, nonce string, err error) {
	// Format: "n,,n=username,r=nonce"
	// The "n,," prefix is the GS2 header (no channel binding)
	parts := strings.SplitN(msg, ",", 4)
	if len(parts) < 4 {
		return "", "", fmt.Errorf("invalid client-first-message format: expected at least 4 comma-separated parts, got %d", len(parts))
	}

	for _, part := range parts[2:] {
		if strings.HasPrefix(part, "n=") {
			username = part[2:]
		} else if strings.HasPrefix(part, "r=") {
			nonce = part[2:]
		}
	}

	if username == "" || nonce == "" {
		return "", "", fmt.Errorf("missing username or nonce in client-first-message")
	}

	return username, nonce, nil
}

func parseScramClientFinal(msg string) (channelBinding, nonce, proof string, err error) {
	// Format: "c=biws,r=nonce,p=proof"
	parts := strings.Split(msg, ",")
	for _, part := range parts {
		if strings.HasPrefix(part, "c=") {
			channelBinding = part[2:]
		} else if strings.HasPrefix(part, "r=") {
			nonce = part[2:]
		} else if strings.HasPrefix(part, "p=") {
			proof = part[2:]
		}
	}

	if nonce == "" || proof == "" {
		return "", "", "", fmt.Errorf("missing nonce or proof in client-final-message")
	}

	return channelBinding, nonce, proof, nil
}

func extractClientFirstBare(msg string) string {
	// Remove GS2 header ("n,,") to get client-first-message-bare
	idx := strings.Index(msg, ",,")
	if idx >= 0 && idx+2 < len(msg) {
		return msg[idx+2:]
	}
	return msg
}

func extractClientFinalWithoutProof(msg string) string {
	// Remove ",p=..." from the end
	idx := strings.LastIndex(msg, ",p=")
	if idx >= 0 {
		return msg[:idx]
	}
	return msg
}

func generateNonce() string {
	b := make([]byte, 18)
	if _, err := io.ReadFull(randReader, b); err != nil {
		panic(fmt.Sprintf("failed to generate nonce: %v", err))
	}
	return base64.StdEncoding.EncodeToString(b)
}

func base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func base64Decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func computeSaltedPassword(hashName, password string, salt []byte, iterations int) []byte {
	h := hashFunc(hashName)
	return pbkdf2.Key([]byte(password), salt, iterations, h().Size(), h)
}

func computeHMAC(hashName string, key, data []byte) []byte {
	mac := hmac.New(hashFunc(hashName), key)
	mac.Write(data)
	return mac.Sum(nil)
}

func computeHash(hashName string, data []byte) []byte {
	h := hashFunc(hashName)()
	h.Write(data)
	return h.Sum(nil)
}

func hashFunc(name string) func() hash.Hash {
	switch name {
	case "SHA-256":
		return sha256.New
	case "SHA-512":
		return sha512.New
	default:
		return sha256.New
	}
}

func xorBytes(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

func bytesEqual(a, b []byte) bool {
	return hmac.Equal(a, b)
}
