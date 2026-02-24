package ksrv

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// --- Unit tests ---

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()

	assert.Equal(t, defaultAddress, cfg.Address)
	assert.Equal(t, defaultTimeout, cfg.Timeout)
	assert.Equal(t, defaultIdleTimeout, cfg.IdleTimeout)
	assert.Equal(t, defaultMaxMessageBytes, cfg.MaxMessageBytes)
}

func TestConfigDoesNotOverrideSet(t *testing.T) {
	cfg := Config{
		Address:         "127.0.0.1:1234",
		Timeout:         10 * time.Second,
		IdleTimeout:     120 * time.Second,
		MaxMessageBytes: 2048,
	}
	cfg.setDefaults()

	assert.Equal(t, "127.0.0.1:1234", cfg.Address)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 120*time.Second, cfg.IdleTimeout)
	assert.Equal(t, 2048, cfg.MaxMessageBytes)
}

func TestServerConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid minimal config",
			cfg: Config{
				Address: "127.0.0.1:0",
			},
		},
		{
			name: "valid config with topics",
			cfg: Config{
				Address: "127.0.0.1:0",
				Topics:  []string{"test-topic", "events"},
			},
		},
		{
			name: "valid config with timeout",
			cfg: Config{
				Address:         "127.0.0.1:0",
				Timeout:         10 * time.Second,
				MaxMessageBytes: 2097152,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, func(context.Context, []*Message) error { return nil })
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSplitSASLPlain(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected [][]byte
	}{
		{
			name:     "standard format",
			input:    []byte("\x00user\x00pass"),
			expected: [][]byte{[]byte(""), []byte("user"), []byte("pass")},
		},
		{
			name:     "with authzID",
			input:    []byte("authz\x00user\x00pass"),
			expected: [][]byte{[]byte("authz"), []byte("user"), []byte("pass")},
		},
		{
			name:     "empty password",
			input:    []byte("\x00user\x00"),
			expected: [][]byte{[]byte(""), []byte("user"), []byte("")},
		},
		{
			name:     "no separators",
			input:    []byte("noNulls"),
			expected: [][]byte{[]byte("noNulls")},
		},
		{
			name:     "empty input",
			input:    []byte{},
			expected: [][]byte{[]byte("")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := splitSASLPlain(tt.input)
			require.Len(t, parts, len(tt.expected))
			for i, part := range parts {
				assert.Equal(t, tt.expected[i], part)
			}
		})
	}
}

func TestIsTopicAllowed(t *testing.T) {
	tests := []struct {
		name    string
		topics  []string
		check   string
		allowed bool
	}{
		{
			name:    "empty topics allows all",
			topics:  nil,
			check:   "any-topic",
			allowed: true,
		},
		{
			name:    "allowed topic",
			topics:  []string{"events", "logs"},
			check:   "events",
			allowed: true,
		},
		{
			name:    "disallowed topic",
			topics:  []string{"events", "logs"},
			check:   "other",
			allowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Address: "127.0.0.1:0", Topics: tt.topics}
			srv, err := New(cfg, func(context.Context, []*Message) error { return nil })
			require.NoError(t, err)
			assert.Equal(t, tt.allowed, srv.isTopicAllowed(tt.check))
		})
	}
}

func TestGenerateTopicID(t *testing.T) {
	// Same topic should produce same ID
	id1 := generateTopicID("test-topic")
	id2 := generateTopicID("test-topic")
	assert.Equal(t, id1, id2)

	// Different topics should produce different IDs
	id3 := generateTopicID("other-topic")
	assert.NotEqual(t, id1, id3)

	// Should not be all zeros
	assert.NotEqual(t, [16]byte{}, id1)
}

func TestServerSASLSetup(t *testing.T) {
	cfg := Config{
		Address: "127.0.0.1:0",
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
			{Mechanism: "PLAIN", Username: "user2", Password: "pass2"},
			{Mechanism: "SCRAM-SHA-256", Username: "suser", Password: "spass"},
		},
	}

	srv, err := New(cfg, func(context.Context, []*Message) error { return nil })
	require.NoError(t, err)

	assert.True(t, srv.saslEnabled)
	assert.Len(t, srv.saslCredentials["PLAIN"], 2)
	assert.Equal(t, "pass1", srv.saslCredentials["PLAIN"]["user1"])
	assert.NotNil(t, srv.scram256Server)
	assert.Nil(t, srv.scram512Server)
}

func TestServerAddr(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0"}, func(context.Context, []*Message) error { return nil })
	require.NoError(t, err)

	// Before Start, Addr should be nil
	assert.Nil(t, srv.Addr())

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	// After Start, Addr should not be nil
	assert.NotNil(t, srv.Addr())
}

func TestServerAdvertisedAddress(t *testing.T) {
	capture := &messageCapture{}
	port := getFreePort(t)

	cfg := Config{
		Address:           fmt.Sprintf("127.0.0.1:%d", port),
		AdvertisedAddress: fmt.Sprintf("myhost.local:%d", port),
		Timeout:           5 * time.Second,
	}

	srv, err := New(cfg, captureHandler(capture), WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background())

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	// Connect and verify via raw protocol that the advertised address is returned
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Send ApiVersions request
	req := &kmsg.ApiVersionsRequest{Version: 0}
	reqBuf := req.AppendTo(nil)

	header := make([]byte, 0)
	header = kbin.AppendInt16(header, int16(kmsg.ApiVersions))
	header = kbin.AppendInt16(header, 0)
	header = kbin.AppendInt32(header, 1)
	header = kbin.AppendInt16(header, -1)
	fullReq := append(header, reqBuf...)
	sizeBuf := kbin.AppendInt32(nil, int32(len(fullReq)))
	_, err = conn.Write(append(sizeBuf, fullReq...))
	require.NoError(t, err)

	// Read ApiVersions response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)
	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)

	// Send Metadata request
	metadataReq := &kmsg.MetadataRequest{Version: 1}
	metadataReqBuf := metadataReq.AppendTo(nil)

	header2 := make([]byte, 0)
	header2 = kbin.AppendInt16(header2, int16(kmsg.Metadata))
	header2 = kbin.AppendInt16(header2, 1) // version 1
	header2 = kbin.AppendInt32(header2, 2) // correlationID
	header2 = kbin.AppendInt16(header2, -1) // no clientID
	fullReq2 := append(header2, metadataReqBuf...)
	sizeBuf2 := kbin.AppendInt32(nil, int32(len(fullReq2)))
	_, err = conn.Write(append(sizeBuf2, fullReq2...))
	require.NoError(t, err)

	// Read Metadata response
	respSizeBuf2 := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf2)
	require.NoError(t, err)
	respSize2 := int32(binary.BigEndian.Uint32(respSizeBuf2))
	respBuf2 := make([]byte, respSize2)
	_, err = io.ReadFull(conn, respBuf2)
	require.NoError(t, err)

	// Parse the metadata response - skip correlationID (4 bytes)
	metadataResp := &kmsg.MetadataResponse{Version: 1}
	err = metadataResp.ReadFrom(respBuf2[4:])
	require.NoError(t, err)

	// Verify the advertised address is returned
	require.Len(t, metadataResp.Brokers, 1)
	assert.Equal(t, "myhost.local", metadataResp.Brokers[0].Host)
	assert.Equal(t, int32(port), metadataResp.Brokers[0].Port)
}

func TestProtocolApiVersions(t *testing.T) {
	_, addr := startTestServer(t, Config{
		Timeout: 5 * time.Second,
	}, func(context.Context, []*Message) error { return nil })

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	// Send ApiVersions request (apiKey=18, version=0)
	req := &kmsg.ApiVersionsRequest{Version: 0}
	reqBuf := req.AppendTo(nil)

	// Build request: apiKey(2) + apiVersion(2) + correlationID(4) + clientID(2 + len)
	header := make([]byte, 0)
	header = kbin.AppendInt16(header, int16(kmsg.ApiVersions))
	header = kbin.AppendInt16(header, 0) // version
	header = kbin.AppendInt32(header, 1) // correlationID
	header = kbin.AppendInt16(header, -1) // no clientID

	fullReq := append(header, reqBuf...)

	// Write size prefix + request
	sizeBuf := kbin.AppendInt32(nil, int32(len(fullReq)))
	_, err = conn.Write(append(sizeBuf, fullReq...))
	require.NoError(t, err)

	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)

	respSize := int32(binary.BigEndian.Uint32(respSizeBuf))
	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)

	// Parse: correlationID(4) + response body
	require.True(t, len(respBuf) >= 4)
	corrID := int32(binary.BigEndian.Uint32(respBuf[:4]))
	assert.Equal(t, int32(1), corrID)
}

func TestValidateSASLPlain(t *testing.T) {
	srv, err := New(Config{
		Address: "127.0.0.1:0",
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user", Password: "pass"},
		},
	}, func(context.Context, []*Message) error { return nil })
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{
			name: "valid credentials",
			data: []byte("\x00user\x00pass"),
			want: true,
		},
		{
			name: "wrong password",
			data: []byte("\x00user\x00wrong"),
			want: false,
		},
		{
			name: "unknown user",
			data: []byte("\x00nobody\x00pass"),
			want: false,
		},
		{
			name: "missing separator",
			data: []byte("userpass"),
			want: false,
		},
		{
			name: "empty",
			data: []byte{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &connState{}
			got := srv.validateSASLPlain(tt.data, state)
			assert.Equal(t, tt.want, got)
			if tt.want {
				assert.True(t, state.authenticated)
			}
		})
	}
}

