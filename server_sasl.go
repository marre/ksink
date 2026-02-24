package ksrv

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"fmt"
	"hash"

	"github.com/xdg-go/scram"
	"golang.org/x/crypto/pbkdf2"
)

// SCRAM authentication constants
const (
	scramSaltSize   = 16   // Size of random salt for SCRAM credentials
	scramIterations = 4096 // PBKDF2 iterations (standard for SCRAM)
)

// SASL mechanism constants
const (
	saslMechanismPlain       = "PLAIN"
	saslMechanismScramSha256 = "SCRAM-SHA-256"
	saslMechanismScramSha512 = "SCRAM-SHA-512"
)

// SCRAM key derivation labels (RFC 5802)
const (
	scramClientKeyLabel = "Client Key"
	scramServerKeyLabel = "Server Key"
)

// connectionState tracks per-connection authentication state.
type connectionState struct {
	authenticated        bool
	scramConversation    *scram.ServerConversation
	scramMechanism       string
	saslHandshakeVersion int16
	expectUnframedSASL   bool
}

// saslAuthStatus represents the result of a SASL authentication step.
type saslAuthStatus int

const (
	saslAuthInProgress saslAuthStatus = iota
	saslAuthSuccess
	saslAuthFailed
)

// PlainCredentials holds username/password pairs for PLAIN authentication.
type PlainCredentials map[string]string

// SCRAMCredentials holds pre-computed SCRAM credentials keyed by username.
type SCRAMCredentials map[string]scram.StoredCredentials

// generateSCRAMCredentials generates SCRAM stored credentials from a plaintext password.
func generateSCRAMCredentials(mechanism, password string) (scram.StoredCredentials, error) {
	salt := make([]byte, scramSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("failed to generate salt: %w", err)
	}

	var hashFunc func() hash.Hash
	var keyLen int
	switch mechanism {
	case saslMechanismScramSha256:
		hashFunc = sha256.New
		keyLen = sha256.Size
	case saslMechanismScramSha512:
		hashFunc = sha512.New
		keyLen = sha512.Size
	default:
		return scram.StoredCredentials{}, fmt.Errorf("unsupported mechanism: %s", mechanism)
	}

	saltedPassword := pbkdf2.Key([]byte(password), salt, scramIterations, keyLen, hashFunc)

	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte(scramClientKeyLabel))
	clientKey := clientKeyHMAC.Sum(nil)

	storedKeyHash := hashFunc()
	storedKeyHash.Write(clientKey)
	storedKey := storedKeyHash.Sum(nil)

	serverKeyHMAC := hmac.New(hashFunc, saltedPassword)
	serverKeyHMAC.Write([]byte(scramServerKeyLabel))
	serverKey := serverKeyHMAC.Sum(nil)

	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{
			Salt:  string(salt),
			Iters: scramIterations,
		},
		StoredKey: storedKey,
		ServerKey: serverKey,
	}, nil
}

// validatePlainCredentials validates PLAIN SASL credentials.
// PLAIN format: [authzid]\0username\0password
// Returns (username, valid).
func validatePlainCredentials(authBytes []byte, credentials PlainCredentials) (string, bool) {
	parts := bytes.Split(authBytes, []byte{0})
	if len(parts) < 3 {
		return "", false
	}
	username := string(parts[1])
	password := string(parts[2])

	expectedPassword, exists := credentials[username]
	if !exists {
		return username, false
	}

	if subtle.ConstantTimeCompare([]byte(expectedPassword), []byte(password)) != 1 {
		return username, false
	}
	return username, true
}

// addPlainCredentials adds a username/password pair to the PlainCredentials map.
func addPlainCredentials(creds PlainCredentials, username, password string) {
	creds[username] = password
}

// addSCRAMCredentials generates and inserts SCRAM stored credentials for the given username.
func addSCRAMCredentials(mechanism string, credsMap SCRAMCredentials, username, password string) error {
	creds, err := generateSCRAMCredentials(mechanism, password)
	if err != nil {
		return err
	}
	credsMap[username] = creds
	return nil
}

// newSCRAMServer creates a new SCRAM server for the specified mechanism.
func newSCRAMServer(mechanism string, credLookup scram.CredentialLookup) (*scram.Server, error) {
	switch mechanism {
	case saslMechanismScramSha256:
		return scram.SHA256.NewServer(credLookup)
	case saslMechanismScramSha512:
		return scram.SHA512.NewServer(credLookup)
	default:
		return nil, fmt.Errorf("unsupported SCRAM mechanism: %s", mechanism)
	}
}

// createSCRAMCredentialLookup returns a credential lookup function for the given mechanism.
func createSCRAMCredentialLookup(mechanism string, scram256Creds, scram512Creds SCRAMCredentials) scram.CredentialLookup {
	var credsMap SCRAMCredentials
	if mechanism == saslMechanismScramSha512 {
		credsMap = scram512Creds
	} else {
		credsMap = scram256Creds
	}
	return func(username string) (scram.StoredCredentials, error) {
		creds, ok := credsMap[username]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("user not found: %s", username)
		}
		return creds, nil
	}
}

// SCRAMAuthResult represents the result of a SCRAM authentication step.
type SCRAMAuthResult struct {
	ServerMessage string
	AuthStatus    saslAuthStatus
	Error         error
}

// Status returns the authentication status.
func (r SCRAMAuthResult) Status() saslAuthStatus {
	return r.AuthStatus
}

// processSCRAMStep advances the SCRAM conversation by one step.
// The state's scramConversation is initialised on the first call.
func processSCRAMStep(clientMessage string, state *connectionState, scram256Creds, scram512Creds SCRAMCredentials) SCRAMAuthResult {
	if state.scramConversation == nil {
		credLookup := createSCRAMCredentialLookup(state.scramMechanism, scram256Creds, scram512Creds)
		scramServer, err := newSCRAMServer(state.scramMechanism, credLookup)
		if err != nil {
			return SCRAMAuthResult{
				AuthStatus: saslAuthFailed,
				Error:      fmt.Errorf("failed to create SCRAM server: %w", err),
			}
		}
		state.scramConversation = scramServer.NewConversation()
	}

	serverMessage, err := state.scramConversation.Step(clientMessage)
	if err != nil {
		return SCRAMAuthResult{
			AuthStatus: saslAuthFailed,
			Error:      fmt.Errorf("authentication failed: %w", err),
		}
	}

	if state.scramConversation.Done() {
		if state.scramConversation.Valid() {
			return SCRAMAuthResult{ServerMessage: serverMessage, AuthStatus: saslAuthSuccess}
		}
		return SCRAMAuthResult{ServerMessage: serverMessage, AuthStatus: saslAuthFailed, Error: fmt.Errorf("invalid credentials")}
	}

	return SCRAMAuthResult{ServerMessage: serverMessage, AuthStatus: saslAuthInProgress}
}
