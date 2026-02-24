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

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Server configuration constants
const (
	defaultMessageChanBuffer = 10
	shutdownGracePeriod      = 5 * time.Second
	protocolOverheadBytes    = 102400 // 100 KB
	requestSizeMultiplier    = 2
	maxClientIDLength        = 10000
)

// SASLCredential is a single server-side SASL credential.
type SASLCredential struct {
	// Mechanism is one of "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512".
	Mechanism string
	Username  string
	Password  string
}

// Options configures a Server.
type Options struct {
	// Address is the TCP address to listen on (default "0.0.0.0:9092").
	Address string

	// AdvertisedAddress is the address sent to clients in metadata responses.
	// When empty the listen address is used.
	AdvertisedAddress string

	// AllowedTopics is an optional whitelist of topic names.
	// When empty all topics are accepted.
	AllowedTopics []string

	// TLSConfig enables TLS when non-nil.
	TLSConfig *tls.Config

	// SASL holds the credentials clients may use to authenticate.
	// When empty, authentication is disabled.
	SASL []SASLCredential

	// Timeout is the maximum time to wait for pipeline acknowledgement and for
	// write operations (default 5 s).
	Timeout time.Duration

	// IdleTimeout is the maximum idle time per connection before it is closed.
	// Zero disables the idle timeout.
	IdleTimeout time.Duration

	// MaxMessageBytes is the maximum allowed size of a single message payload
	// (default 1 MiB).
	MaxMessageBytes int

	// IdempotentWrite enables support for idempotent Kafka producers.
	IdempotentWrite bool

	// Logger is used for structured logging.  When nil all output is discarded.
	Logger Logger
}

// TLSOptions holds paths to TLS certificate material used by NewTLSConfig.
type TLSOptions struct {
	CertFile   string
	KeyFile    string
	MTLSAuth   tls.ClientAuthType
	CACertFiles []string
}

// NewTLSConfig builds a *tls.Config from file paths.
func NewTLSConfig(opts TLSOptions) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   opts.MTLSAuth,
	}
	if len(opts.CACertFiles) > 0 {
		cfg.ClientCAs = x509.NewCertPool()
		for _, f := range opts.CACertFiles {
			pem, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file %s: %w", f, err)
			}
			if !cfg.ClientCAs.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("failed to parse CA certificates from %s", f)
			}
		}
	}
	return cfg, nil
}

// messageBatch is the internal representation sent over msgChan.
type messageBatch struct {
	batch   MessageBatch
	ackFn   AckFunc
	resChan chan error
}

// Server is a Kafka-protocol-compatible server that accepts produce requests
// from Kafka producers and exposes them via ReadBatch.
//
// The Connect/ReadBatch/Close interface mirrors Bento's service.BatchInput so
// that a thin Bento adapter can delegate to this type directly.
type Server struct {
	address           string
	advertisedAddress string
	allowedTopics     map[string]struct{}
	tlsConfig         *tls.Config
	saslEnabled       bool
	saslMechanisms    []string
	saslPlain         PlainCredentials
	saslSCRAM256      SCRAMCredentials
	saslSCRAM512      SCRAMCredentials
	timeout           time.Duration
	idleTimeout       time.Duration
	maxMessageBytes   int
	idempotentWrite   bool
	logger            Logger

	producerIDCounter atomic.Int64

	listener   net.Listener
	shutdownCh chan struct{}

	shutSig *shutdown.Signaller
	connWG  sync.WaitGroup

	shutdownOnce sync.Once
	shutdownDone atomic.Bool
	msgChanValue atomic.Value
	connectMu    sync.Mutex

	connCounter  atomic.Uint64
	decompressor kgo.Decompressor
}

// New creates a new Server with the given Options.
// Default values are applied for fields that are zero.
func New(opts Options) (*Server, error) {
	if opts.Address == "" {
		opts.Address = "0.0.0.0:9092"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.MaxMessageBytes == 0 {
		opts.MaxMessageBytes = 1048576
	}
	if opts.Logger == nil {
		opts.Logger = nopLogger{}
	}

	s := &Server{
		address:           opts.Address,
		advertisedAddress: opts.AdvertisedAddress,
		tlsConfig:         opts.TLSConfig,
		timeout:           opts.Timeout,
		idleTimeout:       opts.IdleTimeout,
		maxMessageBytes:   opts.MaxMessageBytes,
		idempotentWrite:   opts.IdempotentWrite,
		logger:            opts.Logger,
		shutdownCh:        make(chan struct{}),
		shutSig:           shutdown.NewSignaller(),
		decompressor:      kgo.DefaultDecompressor(),
	}

	if len(opts.AllowedTopics) > 0 {
		s.allowedTopics = make(map[string]struct{}, len(opts.AllowedTopics))
		for _, t := range opts.AllowedTopics {
			s.allowedTopics[t] = struct{}{}
		}
	}

	if len(opts.SASL) > 0 {
		s.saslPlain = make(PlainCredentials)
		s.saslSCRAM256 = make(SCRAMCredentials)
		s.saslSCRAM512 = make(SCRAMCredentials)
		mechanismSet := make(map[string]bool)

		for i, cred := range opts.SASL {
			if cred.Username == "" {
				return nil, fmt.Errorf("SASL credential %d: username cannot be empty", i)
			}
			switch cred.Mechanism {
			case saslMechanismPlain:
				addPlainCredentials(s.saslPlain, cred.Username, cred.Password)
			case saslMechanismScramSha256:
				if err := addSCRAMCredentials(saslMechanismScramSha256, s.saslSCRAM256, cred.Username, cred.Password); err != nil {
					return nil, fmt.Errorf("SASL credential %d: %w", i, err)
				}
			case saslMechanismScramSha512:
				if err := addSCRAMCredentials(saslMechanismScramSha512, s.saslSCRAM512, cred.Username, cred.Password); err != nil {
					return nil, fmt.Errorf("SASL credential %d: %w", i, err)
				}
			default:
				return nil, fmt.Errorf("SASL credential %d: unsupported mechanism %q", i, cred.Mechanism)
			}
			mechanismSet[cred.Mechanism] = true
		}

		priorityOrder := []string{saslMechanismScramSha512, saslMechanismScramSha256, saslMechanismPlain}
		for _, m := range priorityOrder {
			if mechanismSet[m] {
				s.saslMechanisms = append(s.saslMechanisms, m)
			}
		}
		s.saslEnabled = len(s.saslMechanisms) > 0
	}

	return s, nil
}

// Connect starts the TCP listener and begins accepting Kafka connections.
// It is safe to call Connect concurrently; subsequent calls are no-ops when
// the server is already listening.
func (s *Server) Connect(ctx context.Context) error {
	s.connectMu.Lock()
	defer s.connectMu.Unlock()

	if s.listener != nil {
		return nil
	}
	if s.shutSig.IsSoftStopSignalled() {
		return ErrEndOfInput
	}

	var listener net.Listener
	var err error
	if s.tlsConfig != nil {
		listener, err = tls.Listen("tcp", s.address, s.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to start TLS listener: %w", err)
		}
		s.logger.Infof("Kafka server listening on %s (TLS enabled)", s.address)
	} else {
		listener, err = net.Listen("tcp", s.address)
		if err != nil {
			return fmt.Errorf("failed to start listener: %w", err)
		}
		s.logger.Infof("Kafka server listening on %s", s.address)
	}

	s.listener = listener
	s.setMsgChan(make(chan messageBatch, defaultMessageChanBuffer))
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	defer func() {
		msgChan := s.getMsgChan()
		if msgChan != nil {
			close(msgChan)
			s.setMsgChan(nil)
		}
		s.shutSig.TriggerHasStopped()
	}()

	connChan := make(chan net.Conn)
	errChan := make(chan error, 1)

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case errChan <- err:
				case <-s.shutdownCh:
				}
				return
			}
			select {
			case connChan <- conn:
			case <-s.shutdownCh:
				conn.Close()
				return
			}
		}
	}()

	for {
		select {
		case <-s.shutdownCh:
			return
		case conn := <-connChan:
			s.connWG.Add(1)
			go s.handleConnection(conn)
		case err := <-errChan:
			if !s.shutSig.IsSoftStopSignalled() {
				s.logger.Debugf("Accept loop ended: %v", err)
			}
			return
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	connID := s.connCounter.Add(1)
	remoteAddr := conn.RemoteAddr().String()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			s.logger.Warnf("[conn:%d] Failed to set TCP_NODELAY: %v", connID, err)
		}
	}

	defer func() {
		conn.Close()
		s.connWG.Done()
		s.logger.Debugf("[conn:%d] Connection closed from %s", connID, remoteAddr)
	}()

	s.logger.Debugf("[conn:%d] Accepted connection from %s", connID, remoteAddr)

	connState := &connectionState{
		authenticated: !s.saslEnabled,
	}

	for {
		select {
		case <-s.shutdownCh:
			s.logger.Debugf("[conn:%d] Shutdown signaled, closing connection", connID)
			return
		default:
		}

		if s.idleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
				s.logger.Errorf("[conn:%d] Failed to set read deadline: %v", connID, err)
				return
			}
		} else {
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				s.logger.Errorf("[conn:%d] Failed to clear read deadline: %v", connID, err)
				return
			}
		}

		peekBuf := make([]byte, 1)
		_, peekErr := conn.Read(peekBuf)
		if peekErr != nil {
			if peekErr == io.EOF {
				s.logger.Debugf("[conn:%d] Client closed connection (EOF)", connID)
			} else if netErr, ok := peekErr.(net.Error); ok && netErr.Timeout() {
				s.logger.Debugf("[conn:%d] Read timeout waiting for first byte", connID)
			} else {
				s.logger.Debugf("[conn:%d] Failed to read first byte: %v", connID, peekErr)
			}
			return
		}

		sizeBytes := make([]byte, 4)
		sizeBytes[0] = peekBuf[0]
		if _, err := io.ReadFull(conn, sizeBytes[1:4]); err != nil {
			s.logger.Errorf("[conn:%d] Failed to read remaining size bytes: %v", connID, err)
			return
		}

		size := int32(binary.BigEndian.Uint32(sizeBytes))

		if connState.expectUnframedSASL {
			s.logger.Debugf("[conn:%d] Handling legacy unframed SASL %s (size=%d)", connID, connState.scramMechanism, size)

			if size <= 0 || size > 10000 {
				s.logger.Errorf("[conn:%d] Invalid unframed SASL size: %d", connID, size)
				return
			}

			saslData := make([]byte, size)
			if _, err := io.ReadFull(conn, saslData); err != nil {
				s.logger.Errorf("[conn:%d] Failed to read unframed SASL data: %v", connID, err)
				return
			}

			var authenticated bool
			var err error
			switch connState.scramMechanism {
			case saslMechanismPlain:
				connState.expectUnframedSASL = false
				authenticated, err = s.handleUnframedSaslPlain(conn, connID, saslData, connState)
			case saslMechanismScramSha256, saslMechanismScramSha512:
				authenticated, err = s.handleUnframedSaslScram(conn, connID, saslData, connState)
			default:
				s.logger.Errorf("[conn:%d] Unknown mechanism for unframed SASL: %s", connID, connState.scramMechanism)
				return
			}

			if err != nil {
				s.logger.Errorf("[conn:%d] Legacy SASL %s authentication error: %v", connID, connState.scramMechanism, err)
				return
			}
			if authenticated {
				connState.authenticated = true
				connState.expectUnframedSASL = false
				s.logger.Debugf("[conn:%d] Client authenticated successfully (legacy SASL %s)", connID, connState.scramMechanism)
			}
			continue
		}

		maxRequestSize := int32(s.maxMessageBytes)*requestSizeMultiplier + protocolOverheadBytes
		if size <= 0 || size > maxRequestSize {
			s.logger.Errorf("[conn:%d] Invalid request size: %d (max: %d)", connID, size, maxRequestSize)
			return
		}

		requestData := make([]byte, size)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			s.logger.Errorf("[conn:%d] Failed to read request data: %v", connID, err)
			return
		}

		authUpdated, err := s.handleRequest(conn, connID, remoteAddr, requestData, connState)
		if err != nil {
			s.logger.Errorf("[conn:%d] Failed to handle request: %v", connID, err)
			return
		}

		if authUpdated && connState.authenticated {
			s.logger.Debugf("[conn:%d] Client authenticated successfully", connID)
		}
	}
}

func (s *Server) handleRequest(conn net.Conn, connID uint64, remoteAddr string, data []byte, connState *connectionState) (bool, error) {
	if len(data) < 8 {
		return false, fmt.Errorf("request too small: %d bytes", len(data))
	}

	reader := kbin.Reader{Src: data}
	apiKey := reader.Int16()
	apiVersion := reader.Int16()
	correlationID := reader.Int32()

	clientIDLen := reader.Int16()
	if clientIDLen > maxClientIDLength {
		return false, fmt.Errorf("client ID too large: %d bytes", clientIDLen)
	}
	if clientIDLen > 0 {
		reader.Span(int(clientIDLen))
	}

	s.logger.Debugf("[conn:%d] Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d", connID, apiKey, apiVersion, correlationID, len(data))

	if s.saslEnabled && !connState.authenticated {
		switch kmsg.Key(apiKey) {
		case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
			// allowed before authentication
		default:
			s.logger.Warnf("[conn:%d] Rejecting unauthenticated request for API key %d", connID, apiKey)
			return false, fmt.Errorf("authentication required")
		}
	}

	kreq := kmsg.RequestForKey(apiKey)
	if kreq == nil {
		s.logger.Warnf("[conn:%d] Unsupported API key: %d, closing connection", connID, apiKey)
		return false, fmt.Errorf("unsupported API key: %d", apiKey)
	}
	kreq.SetVersion(apiVersion)

	if kreq.IsFlexible() {
		kmsg.SkipTags(&reader)
	}

	if err := reader.Complete(); err != nil {
		return false, fmt.Errorf("failed to parse request header: %w", err)
	}

	bodyData := reader.Src

	var handleErr error
	authUpdated := false

	switch req := kreq.(type) {
	case *kmsg.ApiVersionsRequest:
		resp := kmsg.NewApiVersionsResponse()
		resp.SetVersion(apiVersion)
		handleErr = s.handleApiVersionsReq(conn, connID, correlationID, req, &resp)

	case *kmsg.SASLHandshakeRequest:
		if err := req.ReadFrom(bodyData); err != nil {
			resp := kmsg.NewSASLHandshakeResponse()
			resp.SetVersion(apiVersion)
			resp.ErrorCode = kerr.InvalidRequest.Code
			resp.SupportedMechanisms = append([]string{}, s.saslMechanisms...)
			handleErr = s.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = s.handleSaslHandshakeReq(conn, connID, correlationID, req, connState)
		}

	case *kmsg.SASLAuthenticateRequest:
		var authStatus saslAuthStatus
		authStatus, handleErr = s.handleSaslAuthenticate(conn, connID, correlationID, bodyData, apiVersion, connState)
		if handleErr == nil && authStatus == saslAuthSuccess {
			connState.authenticated = true
			authUpdated = true
		}
		if authStatus == saslAuthFailed && handleErr == nil {
			handleErr = fmt.Errorf("SASL authentication failed")
		}

	case *kmsg.MetadataRequest:
		resp := kmsg.NewMetadataResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			handleErr = s.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = s.handleMetadataReq(conn, connID, correlationID, req, &resp)
		}

	case *kmsg.ProduceRequest:
		resp := kmsg.NewProduceResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			handleErr = s.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = s.handleProduceReq(conn, connID, remoteAddr, correlationID, req, &resp)
		}

	case *kmsg.InitProducerIDRequest:
		resp := kmsg.NewInitProducerIDResponse()
		resp.SetVersion(apiVersion)
		if err := req.ReadFrom(bodyData); err != nil {
			resp.ErrorCode = kerr.InvalidRequest.Code
			handleErr = s.sendResponse(conn, connID, correlationID, &resp)
		} else {
			handleErr = s.handleInitProducerIDReq(conn, connID, correlationID, req, &resp)
		}

	default:
		s.logger.Warnf("[conn:%d] Unsupported API key: %d (%T), closing connection", connID, apiKey, kreq)
		return false, fmt.Errorf("unsupported API key: %d", apiKey)
	}

	return authUpdated, handleErr
}

func (s *Server) handleApiVersionsReq(conn net.Conn, connID uint64, correlationID int32, _ *kmsg.ApiVersionsRequest, resp *kmsg.ApiVersionsResponse) error {
	resp.ErrorCode = 0
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3},
		{ApiKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 12},
		{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 9},
	}
	if s.saslEnabled {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
		)
	}
	if s.idempotentWrite {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.InitProducerID), MinVersion: 0, MaxVersion: 4},
		)
	}
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleSaslHandshakeReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.SASLHandshakeRequest, connState *connectionState) error {
	resp := kmsg.NewSASLHandshakeResponse()
	resp.SetVersion(req.Version)

	supported := false
	for _, mech := range s.saslMechanisms {
		if mech == req.Mechanism {
			supported = true
			break
		}
	}

	if supported {
		resp.ErrorCode = 0
		connState.scramMechanism = req.Mechanism
		connState.saslHandshakeVersion = req.Version
		if req.Version == 0 {
			connState.expectUnframedSASL = true
		}
	} else {
		s.logger.Warnf("[conn:%d] Unsupported SASL mechanism requested: %s", connID, req.Mechanism)
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
	}

	resp.SupportedMechanisms = append([]string{}, s.saslMechanisms...)
	return s.sendResponse(conn, connID, correlationID, &resp)
}

func (s *Server) handleSaslAuthenticate(conn net.Conn, connID uint64, correlationID int32, data []byte, apiVersion int16, connState *connectionState) (saslAuthStatus, error) {
	req := kmsg.NewSASLAuthenticateRequest()
	req.SetVersion(apiVersion)
	resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)

	b := kbin.Reader{Src: data}
	var authBytes []byte
	if apiVersion >= 2 {
		authBytes = b.CompactBytes()
	} else {
		authBytes = b.Bytes()
	}

	status := saslAuthInProgress

	if connState.scramMechanism == "" {
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "SASL handshake required before authentication"
		resp.ErrorMessage = &errMsg
		return saslAuthFailed, s.sendResponse(conn, connID, correlationID, resp)
	}

	switch connState.scramMechanism {
	case saslMechanismPlain:
		username, valid := validatePlainCredentials(authBytes, s.saslPlain)
		if valid {
			resp.ErrorCode = 0
			status = saslAuthSuccess
			s.logger.Debugf("[conn:%d] SASL PLAIN authentication succeeded for user: %s", connID, username)
		} else {
			s.logger.Warnf("[conn:%d] SASL PLAIN authentication failed", connID)
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := "Authentication failed"
			resp.ErrorMessage = &errMsg
			status = saslAuthFailed
		}

	case saslMechanismScramSha256, saslMechanismScramSha512:
		result := processSCRAMStep(string(authBytes), connState, s.saslSCRAM256, s.saslSCRAM512)
		if result.Error != nil {
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := result.Error.Error()
			resp.ErrorMessage = &errMsg
			status = saslAuthFailed
		} else {
			resp.SASLAuthBytes = []byte(result.ServerMessage)
			resp.ErrorCode = 0
			status = result.Status()
		}

	default:
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "Unknown SASL mechanism"
		resp.ErrorMessage = &errMsg
		status = saslAuthFailed
	}

	return status, s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleUnframedSaslPlain(conn net.Conn, connID uint64, saslData []byte, _ *connectionState) (bool, error) {
	username, authenticated := validatePlainCredentials(saslData, s.saslPlain)
	if !authenticated {
		s.logger.Warnf("[conn:%d] Legacy SASL PLAIN authentication failed", connID)
		return false, fmt.Errorf("authentication failed")
	}
	s.logger.Debugf("[conn:%d] Legacy SASL PLAIN authentication succeeded for user: %s", connID, username)

	if err := conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return false, fmt.Errorf("failed to set write deadline: %w", err)
	}
	if _, err := conn.Write([]byte{0x00, 0x00, 0x00, 0x00}); err != nil {
		return false, fmt.Errorf("failed to write SASL success response: %w", err)
	}
	return true, nil
}

func (s *Server) handleUnframedSaslScram(conn net.Conn, connID uint64, saslData []byte, connState *connectionState) (bool, error) {
	result := processSCRAMStep(string(saslData), connState, s.saslSCRAM256, s.saslSCRAM512)
	if result.Error != nil {
		return false, fmt.Errorf("authentication failed: %w", result.Error)
	}

	if err := conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return false, fmt.Errorf("failed to set write deadline: %w", err)
	}

	respBytes := []byte(result.ServerMessage)
	if err := binary.Write(conn, binary.BigEndian, int32(len(respBytes))); err != nil {
		return false, fmt.Errorf("failed to write SCRAM response size: %w", err)
	}
	if _, err := conn.Write(respBytes); err != nil {
		return false, fmt.Errorf("failed to write SCRAM response: %w", err)
	}

	switch result.AuthStatus {
	case saslAuthSuccess:
		return true, nil
	case saslAuthFailed:
		return false, fmt.Errorf("invalid credentials")
	default:
		return false, nil
	}
}

func (s *Server) handleInitProducerIDReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.InitProducerIDRequest, resp *kmsg.InitProducerIDResponse) error {
	if !s.idempotentWrite {
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	}
	if req.TransactionalID != nil && *req.TransactionalID != "" {
		resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	newProducerID := s.producerIDCounter.Add(1)
	resp.ErrorCode = 0
	resp.ProducerID = newProducerID
	resp.ProducerEpoch = 0
	s.logger.Debugf("[conn:%d] Allocated producer ID %d for idempotent producer", connID, newProducerID)
	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) handleMetadataReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {
	var requestedTopics []string
	for _, t := range req.Topics {
		if t.Topic != nil {
			requestedTopics = append(requestedTopics, *t.Topic)
		}
	}

	addressToUse := s.address
	if s.advertisedAddress != "" {
		addressToUse = s.advertisedAddress
	}

	host, portStr, err := net.SplitHostPort(addressToUse)
	if err != nil {
		host = "127.0.0.1"
		portStr = "9092"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 9092
	}

	if s.advertisedAddress == "" && (host == "" || host == "0.0.0.0" || host == "::") {
		if localAddr := conn.LocalAddr(); localAddr != nil {
			if localHost, _, err := net.SplitHostPort(localAddr.String()); err == nil && localHost != "" {
				host = localHost
			}
		}
	}

	resp.Brokers = []kmsg.MetadataResponseBroker{
		{NodeID: 1, Host: host, Port: int32(port)},
	}
	clusterID := "ksrv-cluster"
	resp.ClusterID = &clusterID
	resp.ControllerID = 1

	var topicsToReturn []string
	if len(requestedTopics) > 0 {
		topicsToReturn = requestedTopics
	} else if s.allowedTopics != nil {
		for topic := range s.allowedTopics {
			topicsToReturn = append(topicsToReturn, topic)
		}
	}

	for _, topic := range topicsToReturn {
		if s.allowedTopics != nil {
			if _, ok := s.allowedTopics[topic]; !ok {
				resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
					Topic:     kmsg.StringPtr(topic),
					TopicID:   createTopicID(topic),
					ErrorCode: kerr.UnknownTopicOrPartition.Code,
				})
				continue
			}
		}
		resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
			Topic:      kmsg.StringPtr(topic),
			TopicID:    createTopicID(topic),
			ErrorCode:  0,
			IsInternal: false,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 0,
					Replicas:    []int32{1},
					ISR:         []int32{1},
					ErrorCode:   0,
				},
			},
		})
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

type partitionErrorKey struct {
	topic     string
	partition int32
}

func buildProduceResponseTopics(topics []kmsg.ProduceRequestTopic, defaultErrorCode int16, partitionErrors map[partitionErrorKey]int16) []kmsg.ProduceResponseTopic {
	result := make([]kmsg.ProduceResponseTopic, 0, len(topics))
	for _, topic := range topics {
		respTopic := kmsg.ProduceResponseTopic{
			Topic:      topic.Topic,
			Partitions: make([]kmsg.ProduceResponseTopicPartition, 0, len(topic.Partitions)),
		}
		for _, partition := range topic.Partitions {
			partResp := kmsg.NewProduceResponseTopicPartition()
			partResp.Partition = partition.Partition
			partResp.BaseOffset = 0
			partResp.LogAppendTime = -1
			partResp.LogStartOffset = 0
			if errCode, ok := partitionErrors[partitionErrorKey{topic.Topic, partition.Partition}]; ok {
				partResp.ErrorCode = errCode
			} else {
				partResp.ErrorCode = defaultErrorCode
			}
			respTopic.Partitions = append(respTopic.Partitions, partResp)
		}
		result = append(result, respTopic)
	}
	return result
}

func (s *Server) handleProduceReq(conn net.Conn, connID uint64, remoteAddr string, correlationID int32, req *kmsg.ProduceRequest, resp *kmsg.ProduceResponse) error {
	switch req.Acks {
	case -1, 0, 1:
	default:
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.InvalidRequiredAcks.Code, nil)
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var batch MessageBatch
	partErrors := make(map[partitionErrorKey]int16)

	for _, topic := range req.Topics {
		topicName := topic.Topic

		if s.allowedTopics != nil {
			if _, ok := s.allowedTopics[topicName]; !ok {
				s.logger.Warnf("[conn:%d] Rejecting produce to disallowed topic: %s", connID, topicName)
				for _, partition := range topic.Partitions {
					partErrors[partitionErrorKey{topicName, partition.Partition}] = kerr.UnknownTopicOrPartition.Code
				}
				continue
			}
		}

		for i, partition := range topic.Partitions {
			if len(partition.Records) == 0 {
				continue
			}
			if s.maxMessageBytes > 0 && len(partition.Records) > s.maxMessageBytes {
				s.logger.Warnf("[conn:%d] Record batch too large for topic=%s partition=%d", connID, topicName, partition.Partition)
				partErrors[partitionErrorKey{topicName, partition.Partition}] = kerr.MessageTooLarge.Code
				continue
			}
			messages, err := s.parseRecordBatch(connID, partition.Records, topicName, partition.Partition, remoteAddr)
			if err != nil {
				s.logger.Errorf("[conn:%d] Failed to parse record batch for topic=%s partition=%d: %v", connID, topicName, i, err)
				continue
			}
			batch = append(batch, messages...)
		}
	}

	if len(batch) == 0 {
		resp.Topics = buildProduceResponseTopics(req.Topics, 0, partErrors)
		return s.sendResponse(conn, connID, correlationID, resp)
	}

	resChan := make(chan error, 1)
	msgChan := s.getMsgChan()
	if msgChan == nil {
		return fmt.Errorf("server not connected")
	}

	select {
	case msgChan <- messageBatch{
		batch: batch,
		ackFn: func(ackCtx context.Context, err error) error {
			select {
			case resChan <- err:
			default:
			}
			return nil
		},
		resChan: resChan,
	}:
	case <-ctx.Done():
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code, partErrors)
		return s.sendResponse(conn, connID, correlationID, resp)
	case <-s.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	select {
	case ackErr := <-resChan:
		if ackErr != nil {
			resp.Topics = buildProduceResponseTopics(req.Topics, kerr.UnknownServerError.Code, partErrors)
		} else {
			resp.Topics = buildProduceResponseTopics(req.Topics, 0, partErrors)
		}
	case <-ctx.Done():
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code, partErrors)
	case <-s.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	return s.sendResponse(conn, connID, correlationID, resp)
}

func (s *Server) parseRecordBatch(connID uint64, data []byte, topic string, partition int32, remoteAddr string) (MessageBatch, error) {
	if len(data) == 0 {
		return nil, nil
	}

	fakePartition := &kmsg.FetchResponseTopicPartition{
		Partition:     partition,
		RecordBatches: data,
	}

	opts := kgo.ProcessFetchPartitionOpts{
		Topic:                topic,
		Partition:            partition,
		KeepControlRecords:   false,
		DisableCRCValidation: true,
	}

	fp, _ := kgo.ProcessFetchPartition(opts, fakePartition, s.decompressor, nil)

	if fp.Err != nil {
		return nil, fmt.Errorf("failed to process records: %w", fp.Err)
	}

	batch := make(MessageBatch, 0, len(fp.Records))
	for _, record := range fp.Records {
		msg := &Message{
			Value:         record.Value,
			Topic:         record.Topic,
			Partition:     record.Partition,
			Offset:        record.Offset,
			Tombstone:     record.Value == nil,
			ClientAddress: remoteAddr,
		}
		if record.Key != nil {
			msg.Key = record.Key
		}
		if !record.Timestamp.IsZero() {
			msg.Timestamp = record.Timestamp
		}
		if len(record.Headers) > 0 {
			msg.Headers = make(map[string]string, len(record.Headers))
			for _, h := range record.Headers {
				msg.Headers[h.Key] = string(h.Value)
			}
		}
		batch = append(batch, msg)
	}

	s.logger.Debugf("[conn:%d] Parsed %d records", connID, len(batch))
	return batch, nil
}

func (s *Server) sendResponse(conn net.Conn, connID uint64, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0)
	}
	buf = msg.AppendTo(buf)
	return s.writeResponse(connID, conn, buf)
}

func (s *Server) writeResponse(connID uint64, conn net.Conn, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
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

func (s *Server) getMsgChan() chan messageBatch {
	c, _ := s.msgChanValue.Load().(chan messageBatch)
	return c
}

func (s *Server) setMsgChan(ch chan messageBatch) {
	s.msgChanValue.Store(ch)
}

// ReadBatch blocks until a message batch is available, the context is cancelled,
// or the server is closed.  It returns ErrNotConnected when the server has not
// been started and ErrEndOfInput after Close has been called.
func (s *Server) ReadBatch(ctx context.Context) (MessageBatch, AckFunc, error) {
	msgChan := s.getMsgChan()
	if msgChan == nil {
		return nil, nil, ErrNotConnected
	}

	select {
	case mb, open := <-msgChan:
		if !open {
			return nil, nil, ErrNotConnected
		}
		return mb.batch, mb.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.shutdownCh:
		return nil, nil, ErrEndOfInput
	}
}

// Close gracefully shuts down the server, waiting up to shutdownGracePeriod for
// in-flight connections to finish.
func (s *Server) Close(ctx context.Context) error {
	if s.shutdownDone.Swap(true) {
		return nil
	}

	s.shutSig.TriggerSoftStop()

	s.shutdownOnce.Do(func() {
		close(s.shutdownCh)
	})

	if s.listener != nil {
		s.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownGracePeriod):
		s.logger.Warn("Timeout waiting for connections to close")
	}

	s.shutSig.TriggerHasStopped()
	return nil
}

// createTopicID returns a deterministic UUID derived from the topic name.
func createTopicID(topic string) [16]byte {
	hash := sha256.Sum256([]byte(topic))
	var id [16]byte
	copy(id[:], hash[:16])
	id[6] = (id[6] & 0x0f) | 0x40 // version 4
	id[8] = (id[8] & 0x3f) | 0x80 // variant 1
	return id
}
