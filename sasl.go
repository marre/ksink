package ksrv

import (
	"fmt"
	"net"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/xdg-go/scram"
)

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
	clientMsg := string(authBytes)

	if state.scramConv == nil {
		// First message - create a new conversation
		var srv *scram.Server
		switch state.saslMechanism {
		case "SCRAM-SHA-256":
			srv = s.scram256Server
		case "SCRAM-SHA-512":
			srv = s.scram512Server
		}
		if srv == nil {
			return nil, fmt.Errorf("no SCRAM server for mechanism %s", state.saslMechanism)
		}
		state.scramConv = srv.NewConversation()
	}

	response, err := state.scramConv.Step(clientMsg)
	if err != nil {
		return nil, fmt.Errorf("SCRAM step failed: %w", err)
	}

	if state.scramConv.Done() && state.scramConv.Valid() {
		state.authenticated = true
	}

	return []byte(response), nil
}
