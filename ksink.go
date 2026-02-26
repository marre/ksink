// Package ksink provides a lightweight Kafka-protocol-compatible server
// that accepts produce requests from Kafka producers.
//
// It implements enough of the Kafka protocol to allow standard Kafka producers
// to connect and send messages, without requiring a full Kafka cluster.
package ksink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xdg-go/scram"
)

// ErrServerClosed is returned by ReadBatch when the server has been closed.
var ErrServerClosed = errors.New("server closed")

// Server configuration constants.
const (
	defaultAddress         = "0.0.0.0:9092"
	defaultTimeout         = 30 * time.Second
	defaultIdleTimeout     = 60 * time.Second
	defaultMaxMessageBytes = 1048576 // 1MB

	// shutdownGracePeriod is the maximum time to wait for connections to close during shutdown.
	shutdownGracePeriod = 5 * time.Second

	// protocolOverheadBytes is the estimated overhead for Kafka protocol headers and metadata.
	protocolOverheadBytes = 102400 // 100KB

	// requestSizeMultiplier is the multiplier applied to maxMessageBytes to account for protocol overhead.
	requestSizeMultiplier = 2

	// maxClientIDLength is the maximum allowed client ID length to prevent DoS attacks.
	maxClientIDLength = 10000
)

// Message represents a received Kafka message.
type Message struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key        []byte
	Value      []byte
	Headers    map[string]string
	Timestamp  time.Time
	Tombstone  bool
	ClientAddr string
}

// SASLCredential holds a SASL authentication credential.
type SASLCredential struct {
	Mechanism string // "PLAIN", "SCRAM-SHA-256", or "SCRAM-SHA-512"
	Username  string
	Password  string
}

// Config holds the configuration for a [Server].
type Config struct {
	// Address to listen on for Kafka protocol connections. Default: "0.0.0.0:9092".
	Address string

	// AdvertisedAddress is the address advertised to clients in metadata responses.
	// If empty, the listen address is used. Useful when the server is behind a NAT
	// or load balancer.
	AdvertisedAddress string

	// Topics is an optional list of topic names to accept. If empty, all topics are accepted.
	Topics []string

	// CertFile is the path to a server certificate file for enabling TLS.
	CertFile string

	// KeyFile is the path to a server key file for enabling TLS.
	KeyFile string

	// MTLSAuth sets the policy for mTLS client authentication.
	// Valid values: "", "none", "request", "require", "verify_if_given", "require_and_verify".
	MTLSAuth string

	// MTLSCAsFiles is a list of CA certificate files for client certificate verification.
	MTLSCAsFiles []string

	// SASL configures SASL credentials that clients must use to authenticate.
	SASL []SASLCredential

	// Timeout for read/write operations. Default: 30s.
	Timeout time.Duration

	// IdleTimeout for connections. Default: 60s.
	IdleTimeout time.Duration

	// MaxMessageBytes is the maximum size of a single message. Default: 1MB.
	MaxMessageBytes int

	// IdempotentWrite enables idempotent produce support.
	IdempotentWrite bool
}

func (c *Config) setDefaults() {
	if c.Address == "" {
		c.Address = defaultAddress
	}
	if c.Timeout == 0 {
		c.Timeout = defaultTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = defaultMaxMessageBytes
	}
}

// Logger is the interface used by the server for logging.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// AckFunc is called to acknowledge processing of a message batch.
// Pass nil to indicate success (success response sent to the producer).
// Pass an error to reject the batch (error response sent to the producer).
// Must be called exactly once per batch. Calling more than once is a no-op.
type AckFunc func(err error)

// pendingBatch is an internal type that pairs messages with an ack channel.
type pendingBatch struct {
	messages []*Message
	ackCh    chan error
}

// noopLogger discards all log messages.
type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

// Option configures the [Server].
type Option func(*Server)

// WithLogger sets the logger for the server.
func WithLogger(l Logger) Option {
	return func(s *Server) {
		s.logger = l
	}
}

// Server is a Kafka-protocol-compatible server that accepts produce requests.
// Use [ReadBatch] to receive message batches from connected producers.
type Server struct {
	cfg    Config
	logger Logger

	listener  net.Listener
	connWG    sync.WaitGroup
	connCount atomic.Uint64

	// Allowed topics
	allowedTopics map[string]struct{}

	// SASL credentials
	saslCredentials map[string]map[string]string // mechanism -> username -> password
	saslEnabled     bool
	scram256Server  *scram.Server
	scram512Server  *scram.Server

	// TLS
	tlsConfig *tls.Config

	// Idempotent producer tracking
	producerIDCounter atomic.Int64

	// Decompressor for parsing record batches
	decompressor kgo.Decompressor

	// Batch delivery channel for ReadBatch
	batchCh chan pendingBatch

	// Shutdown
	cancelFn     context.CancelFunc
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	shutdownDone atomic.Bool
}

// New creates a new [Server] with the given configuration and options.
func New(cfg Config, opts ...Option) (*Server, error) {
	cfg.setDefaults()

	s := &Server{
		cfg:             cfg,
		logger:          noopLogger{},
		allowedTopics:   make(map[string]struct{}),
		saslCredentials: make(map[string]map[string]string),
		shutdownCh:      make(chan struct{}),
		batchCh:         make(chan pendingBatch),
		decompressor:    kgo.DefaultDecompressor(),
	}

	for _, opt := range opts {
		opt(s)
	}

	// Set up allowed topics
	for _, t := range cfg.Topics {
		s.allowedTopics[t] = struct{}{}
	}

	// Set up SASL credentials
	for _, cred := range cfg.SASL {
		if _, ok := s.saslCredentials[cred.Mechanism]; !ok {
			s.saslCredentials[cred.Mechanism] = make(map[string]string)
		}
		s.saslCredentials[cred.Mechanism][cred.Username] = cred.Password
	}
	s.saslEnabled = len(s.saslCredentials) > 0

	// Pre-compute SCRAM servers
	if users, ok := s.saslCredentials["SCRAM-SHA-256"]; ok {
		credMap, err := computeScramCredentials(scram.SHA256, users)
		if err != nil {
			return nil, fmt.Errorf("failed to compute SCRAM-SHA-256 credentials: %w", err)
		}
		srv, err := newScramServer(scram.SHA256, credMap)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-256 server: %w", err)
		}
		s.scram256Server = srv
	}
	if users, ok := s.saslCredentials["SCRAM-SHA-512"]; ok {
		credMap, err := computeScramCredentials(scram.SHA512, users)
		if err != nil {
			return nil, fmt.Errorf("failed to compute SCRAM-SHA-512 credentials: %w", err)
		}
		srv, err := newScramServer(scram.SHA512, credMap)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-512 server: %w", err)
		}
		s.scram512Server = srv
	}

	// Set up TLS if configured
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig, err := s.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		s.tlsConfig = tlsConfig
	}

	return s, nil
}

// buildTLSConfig creates the TLS configuration for the server.
func (s *Server) buildTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(s.cfg.CertFile, s.cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Configure mTLS
	switch s.cfg.MTLSAuth {
	case "request":
		tlsConfig.ClientAuth = tls.RequestClientCert
	case "require":
		tlsConfig.ClientAuth = tls.RequireAnyClientCert
	case "verify_if_given":
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	case "require_and_verify":
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	default:
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	// Load client CAs if mTLS verification is needed
	if len(s.cfg.MTLSCAsFiles) > 0 {
		caCertPool := x509.NewCertPool()
		for _, caFile := range s.cfg.MTLSCAsFiles {
			caCert, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate %s: %w", caFile, err)
			}
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate %s", caFile)
			}
		}
		tlsConfig.ClientCAs = caCertPool
	}

	return tlsConfig, nil
}

// Start begins accepting connections.
func (s *Server) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancelFn = cancel

	var err error
	if s.tlsConfig != nil {
		s.listener, err = tls.Listen("tcp", s.cfg.Address, s.tlsConfig)
	} else {
		s.listener, err = net.Listen("tcp", s.cfg.Address)
	}
	if err != nil {
		cancel()
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.Address, err)
	}

	s.logger.Infof("Kafka server listening on %s", s.listener.Addr().String())

	go s.acceptLoop(ctx)

	return nil
}

// Addr returns the network address the server is listening on,
// or nil if the server has not been started.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// ReadBatch blocks until a batch of messages is available from a connected
// producer, or the context is cancelled, or the server is closed.
//
// Returns the messages, an [AckFunc] to acknowledge processing, and any error.
// The caller must call the AckFunc after processing the messages — pass nil to
// indicate success, or an error to reject the batch.
func (s *Server) ReadBatch(ctx context.Context) ([]*Message, AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.shutdownCh:
		return nil, nil, ErrServerClosed
	case batch := <-s.batchCh:
		ack := func(err error) {
			select {
			case batch.ackCh <- err:
			default:
			}
		}
		return batch.messages, ack, nil
	}
}

// Close gracefully shuts down the server.
func (s *Server) Close(ctx context.Context) error {
	if s.shutdownDone.Swap(true) {
		return nil // Already closed
	}

	s.shutdownOnce.Do(func() {
		close(s.shutdownCh)
	})

	if s.cancelFn != nil {
		s.cancelFn()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	// Wait for connections to finish with timeout
	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.logger.Warnf("Context cancelled waiting for connections to close")
	case <-time.After(shutdownGracePeriod):
		s.logger.Warnf("Timeout waiting for connections to close")
	}

	return nil
}


