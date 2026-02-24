// Package ksrv provides a lightweight Kafka-protocol-compatible server
// that accepts produce requests from Kafka producers.
//
// It implements enough of the Kafka protocol to allow standard Kafka producers
// to connect and send messages, without requiring a full Kafka cluster.
package ksrv

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xdg-go/scram"
)

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

// Handler is the callback for processing received message batches.
// It is called for each produce request with the batch of messages.
// Return nil to acknowledge the messages (success response sent to the producer).
// Return an error to reject the messages (error response sent to the producer).
type Handler func(ctx context.Context, msgs []*Message) error

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
type Server struct {
	cfg     Config
	handler Handler
	logger  Logger

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

	// Shutdown
	cancelFn     context.CancelFunc
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	shutdownDone atomic.Bool
}

// New creates a new [Server] with the given configuration, handler, and options.
func New(cfg Config, handler Handler, opts ...Option) (*Server, error) {
	cfg.setDefaults()

	s := &Server{
		cfg:             cfg,
		handler:         handler,
		logger:          noopLogger{},
		allowedTopics:   make(map[string]struct{}),
		saslCredentials: make(map[string]map[string]string),
		shutdownCh:      make(chan struct{}),
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


