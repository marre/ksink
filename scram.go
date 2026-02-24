package ksrv

import (
	"crypto/sha256"
	"fmt"

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
		kf := scram.KeyFactors{
			Salt:  deterministicSalt(username, password),
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

// deterministicSalt derives a fixed salt from username and password.
func deterministicSalt(username, password string) string {
	h := sha256.Sum256([]byte(username + ":" + password))
	return string(h[:16])
}

// scramServer wraps a *scram.Server.
type scramServer struct {
	srv *scram.Server
}

// newScramServer creates a scramServer with a credential lookup function.
func newScramServer(hashGen scram.HashGeneratorFcn, credMap map[string]scram.StoredCredentials) (*scramServer, error) {
	srv, err := hashGen.NewServer(func(username string) (scram.StoredCredentials, error) {
		cred, ok := credMap[username]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("unknown user: %s", username)
		}
		return cred, nil
	})
	if err != nil {
		return nil, err
	}
	return &scramServer{srv: srv}, nil
}
