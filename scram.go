package ksink

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/xdg-go/scram"
)

// computeScramCredentials pre-computes SCRAM StoredCredentials for a set of users.
func computeScramCredentials(hashGen scram.HashGeneratorFcn, users map[string]string) (map[string]scram.StoredCredentials, error) {
	creds := make(map[string]scram.StoredCredentials, len(users))
	for username, password := range users {
		client, err := hashGen.NewClient(username, password, "")
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM client for user %s: %w", username, err)
		}
		salt, err := randomSalt()
		if err != nil {
			return nil, fmt.Errorf("failed to generate salt for user %s: %w", username, err)
		}
		kf := scram.KeyFactors{
			Salt:  salt,
			Iters: 4096,
		}
		stored, err := client.GetStoredCredentialsWithError(kf)
		if err != nil {
			return nil, fmt.Errorf("failed to compute stored credentials for user %s: %w", username, err)
		}
		creds[username] = stored
	}
	return creds, nil
}

// randomSalt generates a random 16-byte salt, base64-encoded.
func randomSalt() (string, error) {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// newScramServer creates a *scram.Server with a credential lookup function.
func newScramServer(hashGen scram.HashGeneratorFcn, credMap map[string]scram.StoredCredentials) (*scram.Server, error) {
	return hashGen.NewServer(func(username string) (scram.StoredCredentials, error) {
		cred, ok := credMap[username]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("unknown user: %s", username)
		}
		return cred, nil
	})
}
