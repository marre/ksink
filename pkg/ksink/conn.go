package ksink

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/xdg-go/scram"
)

// connState tracks per-connection state for SASL and producer ID tracking.
type connState struct {
	authenticated    bool
	saslMechanism    string
	scramConv        *scram.ServerConversation
	awaitingRawSASL  bool // true after a v0 SASLHandshake to expect raw SASL bytes
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

func (s *Server) handleConnection(ctx context.Context, conn net.Conn, connID uint64) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Debugf("[conn:%d] Error closing connection: %v", connID, err)
		}
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

		requestSize := (&kbin.Reader{Src: sizeBuf}).Int32()
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

		// After a v0 SASL Handshake, the next frame is raw SASL auth bytes
		// (not wrapped in a Kafka request envelope).
		if state.awaitingRawSASL {
			state.awaitingRawSASL = false
			if err := s.handleRawSASLAuthenticate(conn, connID, requestBody, state); err != nil {
				s.logger.Errorf("[conn:%d] Raw SASL authentication error: %v", connID, err)
				return
			}
			continue
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
		case *kmsg.AddPartitionsToTxnRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse add partitions to txn request: %w", err)
			} else {
				handleErr = s.handleAddPartitionsToTxn(conn, connID, correlationID, apiVersion, req, state)
			}
		case *kmsg.EndTxnRequest:
			if err := req.ReadFrom(bodyData); err != nil {
				handleErr = fmt.Errorf("failed to parse end txn request: %w", err)
			} else {
				handleErr = s.handleEndTxn(conn, connID, correlationID, apiVersion, req, state)
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
