// Package ksrv provides a lightweight Kafka-protocol-compatible server
// that accepts produce requests from Kafka producers.
//
// It implements enough of the Kafka protocol to allow standard Kafka producers
// to connect and send messages, without requiring a full Kafka cluster.
package ksrv

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// Start begins accepting connections. It blocks until the context is cancelled
// or [Server.Close] is called.
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

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return
			case <-ctx.Done():
				return
			default:
				s.logger.Errorf("Failed to accept connection: %v", err)
				continue
			}
		}

		connID := s.connCount.Add(1)
		s.logger.Debugf("[conn:%d] New connection from %s", connID, conn.RemoteAddr())

		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			s.handleConnection(ctx, conn, connID)
		}()
	}
}

// connState tracks per-connection state for SASL and producer ID tracking.
type connState struct {
	authenticated bool
	saslMechanism string
	scramState    *scramServerState
}

// scramServerState holds server-side SCRAM authentication state.
type scramServerState struct {
	mechanism    string
	username     string
	password     string
	nonce        string
	clientNonce  string
	salt         []byte
	iterations   int
	authMessage  string
	serverKey    []byte
	storedKey    []byte
	clientProof  []byte
	serverSig    []byte
	step         int
	conversation scramConversation
}

// scramConversation manages the SCRAM authentication flow using xdg-go/scram.
type scramConversation interface {
	Step(challenge string) (response string, err error)
	Valid() bool
	Done() bool
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, connID uint64) {
	defer func() {
		conn.Close()
		s.logger.Debugf("[conn:%d] Connection closed", connID)
	}()

	state := &connState{}

	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
			s.logger.Errorf("[conn:%d] Failed to set read deadline: %v", connID, err)
			return
		}

		// Read the request size (4 bytes, big-endian)
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.Debugf("[conn:%d] Connection idle timeout", connID)
			} else if err != io.EOF {
				s.logger.Debugf("[conn:%d] Error reading request size: %v", connID, err)
			}
			return
		}

		requestSize := int32(binary.BigEndian.Uint32(sizeBuf))
		s.logger.Debugf("[conn:%d] Request size: %d", connID, requestSize)

		maxRequestSize := int32(s.cfg.MaxMessageBytes*requestSizeMultiplier + protocolOverheadBytes)
		if requestSize <= 0 || requestSize > maxRequestSize {
			s.logger.Errorf("[conn:%d] Invalid request size: %d (max: %d)", connID, requestSize, maxRequestSize)
			return
		}

		// Read the full request body
		requestBody := make([]byte, requestSize)
		if _, err := io.ReadFull(conn, requestBody); err != nil {
			s.logger.Errorf("[conn:%d] Error reading request body: %v", connID, err)
			return
		}

		// Parse the request header using kbin.Reader
		reader := kbin.Reader{Src: requestBody}
		apiKey := reader.Int16()
		apiVersion := reader.Int16()
		correlationID := reader.Int32()

		s.logger.Debugf("[conn:%d] Request: apiKey=%d, apiVersion=%d, correlationID=%d", connID, apiKey, apiVersion, correlationID)

		// Read and validate clientID (nullable string)
		clientIDLen := reader.Int16()
		if clientIDLen > maxClientIDLength {
			s.logger.Errorf("[conn:%d] Client ID too long: %d bytes (max: %d)", connID, clientIDLen, maxClientIDLength)
			return
		}
		if clientIDLen > 0 {
			reader.Span(int(clientIDLen))
		}

		kreq := kmsg.RequestForKey(apiKey)
		if kreq == nil {
			s.logger.Warnf("[conn:%d] Unsupported API key: %d", connID, apiKey)
			return
		}
		kreq.SetVersion(apiVersion)

		// Skip flexible header tags if applicable (KIP-482)
		if kreq.IsFlexible() {
			kmsg.SkipTags(&reader)
		}

		// The remaining bytes are the request body
		bodyData := reader.Src

		// Handle the request
		var handleErr error
		switch req := kreq.(type) {
		case *kmsg.ApiVersionsRequest:
			handleErr = s.handleApiVersions(conn, connID, correlationID, apiVersion)
		case *kmsg.MetadataRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse metadata request: %w", err)
			} else {
				handleErr = s.handleMetadata(conn, connID, correlationID, apiVersion, req)
			}
		case *kmsg.ProduceRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse produce request: %w", err)
			} else {
				handleErr = s.handleProduce(ctx, conn, connID, correlationID, apiVersion, req)
			}
		case *kmsg.SASLHandshakeRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse SASL handshake request: %w", err)
			} else {
				handleErr = s.handleSaslHandshake(conn, connID, correlationID, apiVersion, req, state)
			}
		case *kmsg.SASLAuthenticateRequest:
			_ = req
			handleErr = s.handleSaslAuthenticate(conn, connID, correlationID, apiVersion, bodyData, state)
		case *kmsg.InitProducerIDRequest:
			_ = req
			handleErr = s.handleInitProducerId(conn, connID, correlationID, apiVersion, state)
		case *kmsg.FindCoordinatorRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse find coordinator request: %w", err)
			} else {
				handleErr = s.handleFindCoordinator(conn, connID, correlationID, apiVersion, req)
			}
		default:
			s.logger.Warnf("[conn:%d] Unsupported API key: %d", connID, apiKey)
			handleErr = fmt.Errorf("unsupported API key: %d", apiKey)
		}

		if handleErr != nil {
			s.logger.Errorf("[conn:%d] Error handling request (apiKey=%d): %v", connID, apiKey, handleErr)
			return
		}
	}
}

func (s *Server) handleApiVersions(conn net.Conn, connID uint64, correlationID int32, apiVersion int16) error {
	resp := &kmsg.ApiVersionsResponse{
		Version:        apiVersion,
		ErrorCode:      0,
		ThrottleMillis: 0,
	}

	// Always advertise core APIs
	resp.ApiKeys = append(resp.ApiKeys,
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 12},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 9},
		kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.FindCoordinator), MinVersion: 0, MaxVersion: 5},
	)

	// Only advertise SASL APIs when SASL is enabled
	if s.saslEnabled {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
		)
	}

	// Only advertise InitProducerID when IdempotentWrite is enabled
	if s.cfg.IdempotentWrite {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.InitProducerID), MinVersion: 0, MaxVersion: 5},
		)
	}

	s.logger.Debugf("[conn:%d] Sending ApiVersions response with %d keys", connID, len(resp.ApiKeys))

	// ApiVersions always uses non-flexible encoding
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleMetadata(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.MetadataRequest) error {
	addr := s.cfg.Address
	if s.cfg.AdvertisedAddress != "" {
		addr = s.cfg.AdvertisedAddress
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("failed to parse address %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}

	resp := &kmsg.MetadataResponse{
		Version: apiVersion,
		Brokers: []kmsg.MetadataResponseBroker{
			{
				NodeID: 1,
				Host:   host,
				Port:   int32(port),
			},
		},
		ControllerID: 1,
		ClusterID:    kmsg.StringPtr("ksrv-cluster"),
	}

	// Build topic list for response using parsed request topics
	if req.Topics != nil {
		for _, t := range req.Topics {
			topicName := ""
			if t.Topic != nil {
				topicName = *t.Topic
			}
			if s.isTopicAllowed(topicName) {
				resp.Topics = append(resp.Topics, s.buildTopicMetadata(topicName, apiVersion))
			} else {
				resp.Topics = append(resp.Topics, s.buildTopicErrorMetadata(topicName, apiVersion, kerr.UnknownTopicOrPartition))
			}
		}
	} else {
		// nil Topics means "all topics"
		for topic := range s.allowedTopics {
			resp.Topics = append(resp.Topics, s.buildTopicMetadata(topic, apiVersion))
		}
	}

	s.logger.Debugf("[conn:%d] Sending Metadata response: %d brokers, %d topics", connID, len(resp.Brokers), len(resp.Topics))
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) buildTopicMetadata(topic string, apiVersion int16) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{
		Topic: kmsg.StringPtr(topic),
		Partitions: []kmsg.MetadataResponseTopicPartition{
			{
				Partition:   0,
				Leader:      1,
				LeaderEpoch: 1,
				Replicas:    []int32{1},
				ISR:         []int32{1},
			},
		},
	}
	if apiVersion >= 10 {
		t.TopicID = generateTopicID(topic)
	}
	return t
}

func (s *Server) buildTopicErrorMetadata(topic string, apiVersion int16, errCode *kerr.Error) kmsg.MetadataResponseTopic {
	t := kmsg.MetadataResponseTopic{
		Topic:     kmsg.StringPtr(topic),
		ErrorCode: errCode.Code,
	}
	if apiVersion >= 10 {
		t.TopicID = generateTopicID(topic)
	}
	return t
}

func generateTopicID(topic string) [16]byte {
	hash := sha256.Sum256([]byte(topic))
	var id [16]byte
	copy(id[:], hash[:16])
	return id
}

func (s *Server) handleProduce(ctx context.Context, conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.ProduceRequest) error {
	if s.saslEnabled {
		// Check if connection is authenticated - we don't have direct access to connState here,
		// so authentication is checked in handleSaslAuthenticate before allowing produce
	}

	s.logger.Debugf("[conn:%d] Produce request: acks=%d, numTopics=%d", connID, req.Acks, len(req.Topics))

	resp := &kmsg.ProduceResponse{
		Version: apiVersion,
	}

	remoteAddr := conn.RemoteAddr().String()

	for _, topicData := range req.Topics {
		topicResp := kmsg.ProduceResponseTopic{
			Topic: topicData.Topic,
		}

		for _, partData := range topicData.Partitions {
			partResp := kmsg.ProduceResponseTopicPartition{
				Partition: partData.Partition,
			}

			if !s.isTopicAllowed(topicData.Topic) {
				s.logger.Warnf("[conn:%d] Rejected produce to disallowed topic: %s", connID, topicData.Topic)
				partResp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			batch, err := s.parseRecords(connID, topicData.Topic, partData.Partition, partData.Records, remoteAddr)
			if err != nil {
				s.logger.Errorf("[conn:%d] Failed to parse records for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
				partResp.ErrorCode = kerr.CorruptMessage.Code
				topicResp.Partitions = append(topicResp.Partitions, partResp)
				continue
			}

			if len(batch) > 0 {
				err = s.handler(ctx, batch)
				if err != nil {
					s.logger.Errorf("[conn:%d] Handler error for %s/%d: %v", connID, topicData.Topic, partData.Partition, err)
					partResp.ErrorCode = kerr.UnknownServerError.Code
				}
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	if req.Acks == 0 {
		return nil
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleSaslHandshake(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.SASLHandshakeRequest, state *connState) error {
	s.logger.Debugf("[conn:%d] SASL Handshake: mechanism=%s", connID, req.Mechanism)

	resp := &kmsg.SASLHandshakeResponse{
		Version: apiVersion,
	}

	// List all supported mechanisms
	for mech := range s.saslCredentials {
		resp.SupportedMechanisms = append(resp.SupportedMechanisms, mech)
	}

	if _, ok := s.saslCredentials[req.Mechanism]; !ok {
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		s.logger.Warnf("[conn:%d] Unsupported SASL mechanism: %s", connID, req.Mechanism)
	} else {
		state.saslMechanism = req.Mechanism
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleSaslAuthenticate(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, bodyData []byte, state *connState) error {
	reader := kbin.Reader{Src: bodyData}

	var authBytes []byte
	if apiVersion >= 2 {
		authBytes = reader.CompactBytes()
	} else {
		authBytes = reader.Bytes()
	}

	s.logger.Debugf("[conn:%d] SASL Authenticate: mechanism=%s, authBytesLen=%d", connID, state.saslMechanism, len(authBytes))

	resp := &kmsg.SASLAuthenticateResponse{
		Version: apiVersion,
	}

	switch state.saslMechanism {
	case "PLAIN":
		authenticated := s.validateSASLPlain(authBytes, state)
		if !authenticated {
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			resp.ErrorMessage = kmsg.StringPtr("Authentication failed")
			s.logger.Warnf("[conn:%d] SASL PLAIN authentication failed", connID)
			if err := s.sendResponse(conn, connID, correlationID, resp); err != nil {
				return err
			}
			return fmt.Errorf("SASL PLAIN authentication failed")
		}
		s.logger.Infof("[conn:%d] SASL PLAIN authentication successful", connID)

	case "SCRAM-SHA-256", "SCRAM-SHA-512":
		respBytes, err := s.handleSASLScram(connID, authBytes, state)
		if err != nil {
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			resp.ErrorMessage = kmsg.StringPtr(err.Error())
			s.logger.Warnf("[conn:%d] SASL SCRAM authentication failed: %v", connID, err)
			if sendErr := s.sendResponse(conn, connID, correlationID, resp); sendErr != nil {
				return sendErr
			}
			return fmt.Errorf("SASL SCRAM authentication failed: %w", err)
		}

		if respBytes != nil {
			resp.SASLAuthBytes = respBytes
		}

		if !state.authenticated {
			// SCRAM needs another round
			return s.sendResponse(conn, connID, correlationID, resp)
		}
		s.logger.Infof("[conn:%d] SASL SCRAM authentication successful", connID)

	default:
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		resp.ErrorMessage = kmsg.StringPtr("Unsupported SASL mechanism")
		if err := s.sendResponse(conn, connID, correlationID, resp); err != nil {
			return err
		}
		return fmt.Errorf("unsupported SASL mechanism: %s", state.saslMechanism)
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) validateSASLPlain(authBytes []byte, state *connState) bool {
	// PLAIN format: \x00username\x00password
	parts := splitSASLPlain(authBytes)
	if len(parts) != 3 {
		return false
	}

	username := string(parts[1])
	password := string(parts[2])

	creds, ok := s.saslCredentials["PLAIN"]
	if !ok {
		return false
	}

	expectedPassword, ok := creds[username]
	if !ok {
		return false
	}

	if expectedPassword != password {
		return false
	}

	state.authenticated = true
	return true
}

func splitSASLPlain(data []byte) [][]byte {
	var parts [][]byte
	start := 0
	for i, b := range data {
		if b == 0 {
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}
	parts = append(parts, data[start:])
	return parts
}

func (s *Server) handleSASLScram(connID uint64, authBytes []byte, state *connState) ([]byte, error) {
	if state.scramState == nil {
		// First message - client-first
		return s.handleScramClientFirst(connID, authBytes, state)
	}
	// Second message - client-final
	return s.handleScramClientFinal(connID, authBytes, state)
}

func (s *Server) handleScramClientFirst(connID uint64, authBytes []byte, state *connState) ([]byte, error) {
	clientFirst := string(authBytes)
	s.logger.Debugf("[conn:%d] SCRAM client-first: %s", connID, clientFirst)

	// Parse client-first-message: "n,,n=username,r=nonce"
	username, clientNonce, err := parseScramClientFirst(clientFirst)
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM client-first message: %w", err)
	}

	// Look up user credentials
	creds, ok := s.saslCredentials[state.saslMechanism]
	if !ok {
		return nil, fmt.Errorf("no credentials for mechanism %s", state.saslMechanism)
	}

	password, ok := creds[username]
	if !ok {
		return nil, fmt.Errorf("unknown user: %s", username)
	}

	// Generate server parameters
	salt := make([]byte, 16)
	if _, err := io.ReadFull(randReader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	iterations := 4096
	serverNonce := clientNonce + generateNonce()

	state.scramState = &scramServerState{
		mechanism:   state.saslMechanism,
		username:    username,
		password:    password,
		nonce:       serverNonce,
		clientNonce: clientNonce,
		salt:        salt,
		iterations:  iterations,
		step:        1,
	}

	// Build server-first-message
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d",
		serverNonce,
		base64Encode(salt),
		iterations)

	// Store for auth message computation
	clientFirstBare := extractClientFirstBare(clientFirst)
	state.scramState.authMessage = clientFirstBare + "," + serverFirst

	s.logger.Debugf("[conn:%d] SCRAM server-first: %s", connID, serverFirst)
	return []byte(serverFirst), nil
}

func (s *Server) handleScramClientFinal(connID uint64, authBytes []byte, state *connState) ([]byte, error) {
	clientFinal := string(authBytes)
	s.logger.Debugf("[conn:%d] SCRAM client-final: %s", connID, clientFinal)

	ss := state.scramState

	// Parse client-final-message: "c=biws,r=nonce,p=proof"
	channelBinding, nonce, proof, err := parseScramClientFinal(clientFinal)
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAM client-final message: %w", err)
	}

	_ = channelBinding

	// Verify nonce
	if nonce != ss.nonce {
		return nil, fmt.Errorf("nonce mismatch")
	}

	// Compute SCRAM verification
	var hashName string
	switch ss.mechanism {
	case "SCRAM-SHA-256":
		hashName = "SHA-256"
	case "SCRAM-SHA-512":
		hashName = "SHA-512"
	default:
		return nil, fmt.Errorf("unsupported SCRAM mechanism: %s", ss.mechanism)
	}

	// Build the complete auth message
	clientFinalWithoutProof := extractClientFinalWithoutProof(clientFinal)
	authMessage := ss.authMessage + "," + clientFinalWithoutProof

	// Compute keys and verify
	saltedPassword := computeSaltedPassword(hashName, ss.password, ss.salt, ss.iterations)
	clientKey := computeHMAC(hashName, saltedPassword, []byte("Client Key"))
	storedKey := computeHash(hashName, clientKey)
	clientSig := computeHMAC(hashName, storedKey, []byte(authMessage))

	// Verify proof
	decodedProof, err := base64Decode(proof)
	if err != nil {
		return nil, fmt.Errorf("invalid proof encoding: %w", err)
	}

	recoveredClientKey := xorBytes(decodedProof, clientSig)
	recoveredStoredKey := computeHash(hashName, recoveredClientKey)

	if !bytesEqual(recoveredStoredKey, storedKey) {
		return nil, fmt.Errorf("authentication failed: invalid proof")
	}

	// Compute server signature
	serverKey := computeHMAC(hashName, saltedPassword, []byte("Server Key"))
	serverSig := computeHMAC(hashName, serverKey, []byte(authMessage))

	serverFinal := "v=" + base64Encode(serverSig)

	state.authenticated = true
	s.logger.Debugf("[conn:%d] SCRAM server-final: %s", connID, serverFinal)
	return []byte(serverFinal), nil
}

func (s *Server) handleInitProducerId(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, state *connState) error {
	if s.saslEnabled && !state.authenticated {
		s.logger.Warnf("[conn:%d] Rejecting InitProducerId: not authenticated", connID)
		resp := &kmsg.InitProducerIDResponse{
			Version:    apiVersion,
			ErrorCode:  kerr.SaslAuthenticationFailed.Code,
			ProducerID: -1,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	if !s.cfg.IdempotentWrite {
		s.logger.Warnf("[conn:%d] Rejecting InitProducerId: idempotent_write is disabled", connID)
		resp := &kmsg.InitProducerIDResponse{
			Version:    apiVersion,
			ErrorCode:  kerr.ClusterAuthorizationFailed.Code,
			ProducerID: -1,
		}
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	producerID := s.producerIDCounter.Add(1)

	resp := &kmsg.InitProducerIDResponse{
		Version:       apiVersion,
		ProducerID:    producerID,
		ProducerEpoch: 0,
	}

	s.logger.Debugf("[conn:%d] InitProducerId: assigned producerID=%d", connID, producerID)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleFindCoordinator(conn net.Conn, connID uint64, correlationID int32, apiVersion int16, req *kmsg.FindCoordinatorRequest) error {
	addr := s.cfg.Address
	if s.cfg.AdvertisedAddress != "" {
		addr = s.cfg.AdvertisedAddress
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("failed to parse address %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to parse port %s: %w", portStr, err)
	}

	resp := &kmsg.FindCoordinatorResponse{
		Version:        apiVersion,
		NodeID:         1,
		Host:           host,
		Port:           int32(port),
		ErrorCode:      0,
		ThrottleMillis: 0,
	}

	// For version >= 4, use Coordinators array
	if apiVersion >= 4 {
		var keys []string
		if len(req.CoordinatorKeys) > 0 {
			keys = req.CoordinatorKeys
		} else {
			keys = []string{req.CoordinatorKey}
		}
		for _, key := range keys {
			resp.Coordinators = append(resp.Coordinators, kmsg.FindCoordinatorResponseCoordinator{
				Key:    key,
				NodeID: 1,
				Host:   host,
				Port:   int32(port),
			})
		}
	}

	s.logger.Debugf("[conn:%d] FindCoordinator response: host=%s, port=%d", connID, host, port)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) isTopicAllowed(topic string) bool {
	if len(s.allowedTopics) == 0 {
		return true
	}
	_, ok := s.allowedTopics[topic]
	return ok
}

func (s *Server) parseRecords(connID uint64, topic string, partition int32, data []byte, remoteAddr string) ([]*Message, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Create a fake FetchPartition to use kgo's record parsing
	fakePartition := kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		RecordBatches: data,
	}

	opts := kgo.ProcessFetchPartitionOpts{
		Topic:                topic,
		Partition:            partition,
		KeepControlRecords:   false,
		DisableCRCValidation: true,
	}

	fp, _ := kgo.ProcessFetchPartition(opts, &fakePartition, s.decompressor, nil)

	if fp.Err != nil {
		return nil, fmt.Errorf("failed to process records: %w", fp.Err)
	}

	msgs := make([]*Message, 0, len(fp.Records))
	for _, record := range fp.Records {
		msg := &Message{
			Topic:      record.Topic,
			Partition:  record.Partition,
			Offset:     record.Offset,
			Key:        record.Key,
			Value:      record.Value,
			Timestamp:  record.Timestamp,
			Tombstone:  record.Value == nil,
			ClientAddr: remoteAddr,
			Headers:    make(map[string]string),
		}

		for _, header := range record.Headers {
			msg.Headers[header.Key] = string(header.Value)
		}

		msgs = append(msgs, msg)
	}

	s.logger.Debugf("[conn:%d] Parsed %d records", connID, len(msgs))
	return msgs, nil
}

func (s *Server) sendResponse(conn net.Conn, connID uint64, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)

	// For flexible responses (EXCEPT ApiVersions key 18), add response header TAG_BUFFER
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0) // Empty TAG_BUFFER (0 tags)
	}

	buf = msg.AppendTo(buf)

	s.logger.Debugf("[conn:%d] Sending response: correlationID=%d, key=%d, size=%d", connID, correlationID, msg.Key(), len(buf))

	return s.writeResponse(connID, conn, buf)
}

func (s *Server) writeResponse(connID uint64, conn net.Conn, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout)); err != nil {
		s.logger.Errorf("[conn:%d] Failed to set write deadline: %v", connID, err)
		return err
	}

	fullData := kbin.AppendInt32(nil, int32(len(data)))
	fullData = append(fullData, data...)

	n, err := conn.Write(fullData)
	if err != nil {
		s.logger.Errorf("[conn:%d] Failed to write response: %v", connID, err)
		return err
	}
	s.logger.Debugf("[conn:%d] Wrote %d bytes response", connID, n)
	return nil
}


