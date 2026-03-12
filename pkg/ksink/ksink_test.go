package ksink

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

func TestConfigTransactionalWriteEnablesIdempotent(t *testing.T) {
	cfg := Config{TransactionalWrite: true}
	assert.False(t, cfg.IdempotentWrite)

	cfg.setDefaults()

	assert.True(t, cfg.IdempotentWrite, "setDefaults must enable IdempotentWrite when TransactionalWrite is true")
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
			srv, err := New(cfg)
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

	srv, err := New(cfg)
	require.NoError(t, err)

	assert.True(t, srv.saslEnabled)
	assert.Len(t, srv.saslCredentials["PLAIN"], 2)
	assert.Equal(t, "pass1", srv.saslCredentials["PLAIN"]["user1"])
	assert.NotNil(t, srv.scram256Server)
	assert.Nil(t, srv.scram512Server)
}

func TestServerAddr(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	// Before Start, Addr should be nil
	assert.Nil(t, srv.Addr())

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background()) //nolint:errcheck

	// After Start, Addr should not be nil
	assert.NotNil(t, srv.Addr())
}

func TestServerAdvertisedAddress(t *testing.T) {
	port := getFreePort(t)

	cfg := Config{
		Address:           fmt.Sprintf("127.0.0.1:%d", port),
		AdvertisedAddress: fmt.Sprintf("myhost.local:%d", port),
		Timeout:           5 * time.Second,
	}

	srv, err := New(cfg, WithLogger(&testLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)
	defer srv.Close(context.Background()) //nolint:errcheck

	// Start a read loop so produce requests don't block
	startReadLoop(t, srv)

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	// Connect and verify via raw protocol that the advertised address is returned
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

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
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)
	respSize := (&kbin.Reader{Src: respSizeBuf}).Int32()
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
	respSize2 := (&kbin.Reader{Src: respSizeBuf2}).Int32()
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

func TestValidateSASLPlain(t *testing.T) {
	srv, err := New(Config{
		Address: "127.0.0.1:0",
		SASL: []SASLCredential{
			{Mechanism: "PLAIN", Username: "user", Password: "pass"},
		},
	})
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

func TestReadBatchContextCancelled(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	msgs, ack, err := srv.ReadBatch(ctx)
	assert.Nil(t, msgs)
	assert.Nil(t, ack)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReadBatchServerClosed(t *testing.T) {
	srv, err := New(Config{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)

	// Close the server
	require.NoError(t, srv.Close(context.Background()))

	msgs, ack, err := srv.ReadBatch(context.Background())
	assert.Nil(t, msgs)
	assert.Nil(t, ack)
	assert.ErrorIs(t, err, ErrServerClosed)
}

// sendRawRequest writes a Kafka-protocol request to conn and returns the
// response body (after the 4-byte length prefix and 4-byte correlationID).
func sendRawRequest(t *testing.T, conn net.Conn, apiKey int16, apiVersion int16, correlationID int32, body []byte) []byte {
	t.Helper()

	header := make([]byte, 0)
	header = kbin.AppendInt16(header, apiKey)
	header = kbin.AppendInt16(header, apiVersion)
	header = kbin.AppendInt32(header, correlationID)
	header = kbin.AppendInt16(header, -1) // no clientID
	fullReq := append(header, body...)
	sizeBuf := kbin.AppendInt32(nil, int32(len(fullReq)))
	_, err := conn.Write(append(sizeBuf, fullReq...))
	require.NoError(t, err)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	respSizeBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, respSizeBuf)
	require.NoError(t, err)
	respSize := (&kbin.Reader{Src: respSizeBuf}).Int32()
	respBuf := make([]byte, respSize)
	_, err = io.ReadFull(conn, respBuf)
	require.NoError(t, err)
	// Skip correlationID (4 bytes)
	return respBuf[4:]
}

// sendInitProducerID sends an InitProducerID request (version 0) and returns
// the parsed response.
func sendInitProducerID(t *testing.T, conn net.Conn, correlationID int32, txnID *string, timeoutMs int32) *kmsg.InitProducerIDResponse {
	t.Helper()

	req := kmsg.NewInitProducerIDRequest()
	req.Version = 0
	req.TransactionalID = txnID
	req.TransactionTimeoutMillis = timeoutMs
	body := req.AppendTo(nil)

	respBuf := sendRawRequest(t, conn, int16(kmsg.InitProducerID), 0, correlationID, body)

	resp := &kmsg.InitProducerIDResponse{Version: 0}
	err := resp.ReadFrom(respBuf)
	require.NoError(t, err)
	return resp
}

// sendAddPartitionsToTxn sends an AddPartitionsToTxn request (version 0) to
// mark a transaction as active on the server.
func sendAddPartitionsToTxn(t *testing.T, conn net.Conn, correlationID int32, txnID string, producerID int64, epoch int16, topic string) {
	t.Helper()

	req := kmsg.NewAddPartitionsToTxnRequest()
	req.Version = 0
	req.TransactionalID = txnID
	req.ProducerID = producerID
	req.ProducerEpoch = epoch
	topicReq := kmsg.NewAddPartitionsToTxnRequestTopic()
	topicReq.Topic = topic
	topicReq.Partitions = []int32{0}
	req.Topics = append(req.Topics, topicReq)
	body := req.AppendTo(nil)

	_ = sendRawRequest(t, conn, int16(kmsg.AddPartitionsToTxn), 0, correlationID, body)
}

func TestTxnStateZombieFencing(t *testing.T) {
	// Verify that zombie fencing aborts an in-flight transaction and bumps
	// the epoch when InitProducerID is called with an existing txnID.
	var (
		aborted []string
		mu      sync.Mutex
	)
	srv, err := New(Config{
		Address:            "127.0.0.1:0",
		TransactionalWrite: true,
		Timeout:            5 * time.Second,
	}, WithLogger(&testLogger{t}), WithTxnEndFunc(func(txnID string, commit bool) { //nolint:staticcheck // testing backward compat
		mu.Lock()
		defer mu.Unlock()
		if !commit {
			aborted = append(aborted, txnID)
		}
	}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	defer srv.Close(context.Background()) //nolint:errcheck
	startReadLoop(t, srv)

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	// First, send ApiVersions (required handshake).
	apiReq := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 0, apiReq.AppendTo(nil))

	// First InitProducerID — creates a new txn state.
	txnID := "txn-fence"
	resp1 := sendInitProducerID(t, conn, 1, &txnID, 30000)
	assert.Equal(t, int16(0), resp1.ErrorCode)
	assert.Equal(t, int16(0), resp1.ProducerEpoch)
	pid := resp1.ProducerID

	// Mark the transaction as active via AddPartitionsToTxn.
	sendAddPartitionsToTxn(t, conn, 2, txnID, pid, 0, "test-topic")

	// Second InitProducerID with the same txnID — should fence zombie.
	resp2 := sendInitProducerID(t, conn, 3, &txnID, 30000)
	assert.Equal(t, int16(0), resp2.ErrorCode)
	assert.Equal(t, pid, resp2.ProducerID, "should reuse the same producer ID")
	assert.Equal(t, int16(1), resp2.ProducerEpoch, "epoch should be bumped")
	mu.Lock()
	assert.Equal(t, []string{"txn-fence"}, aborted, "in-flight txn should have been aborted")
	mu.Unlock()
}

func TestTxnStateEpochBumpNoActiveTransaction(t *testing.T) {
	// When InitProducerID is called for an existing txnID that does not
	// have an active transaction, the epoch is bumped but no abort is fired.
	var (
		aborted []string
		mu      sync.Mutex
	)
	srv, err := New(Config{
		Address:            "127.0.0.1:0",
		TransactionalWrite: true,
		Timeout:            5 * time.Second,
	}, WithLogger(&testLogger{t}), WithTxnEndFunc(func(txnID string, commit bool) { //nolint:staticcheck // testing backward compat
		mu.Lock()
		defer mu.Unlock()
		if !commit {
			aborted = append(aborted, txnID)
		}
	}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	defer srv.Close(context.Background()) //nolint:errcheck
	startReadLoop(t, srv)

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	apiReq := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 0, apiReq.AppendTo(nil))

	// First InitProducerID — creates a new txn state (not active yet).
	txnID := "txn-no-active"
	resp1 := sendInitProducerID(t, conn, 1, &txnID, 30000)
	assert.Equal(t, int16(0), resp1.ErrorCode)
	assert.Equal(t, int16(0), resp1.ProducerEpoch)

	// Second InitProducerID — no active txn so epoch bumps but no abort.
	resp2 := sendInitProducerID(t, conn, 2, &txnID, 30000)
	assert.Equal(t, int16(0), resp2.ErrorCode)
	assert.Equal(t, resp1.ProducerID, resp2.ProducerID, "should reuse the same producer ID")
	assert.Equal(t, int16(1), resp2.ProducerEpoch, "epoch should be bumped")
	mu.Lock()
	assert.Empty(t, aborted, "no abort should be fired when txn is not active")
	mu.Unlock()
}

func TestTxnStateInitialized(t *testing.T) {
	srv, err := New(Config{
		Address:            "127.0.0.1:0",
		TransactionalWrite: true,
	})
	require.NoError(t, err)
	assert.NotNil(t, srv.txnStates, "txnStates map should be initialized")
	assert.Empty(t, srv.txnStates, "txnStates map should be empty initially")
}

func TestTxnStatesMaxSize(t *testing.T) {
	// Verify that the server rejects new transactional IDs when the max
	// number of tracked IDs is reached.
	srv, err := New(Config{
		Address:            "127.0.0.1:0",
		TransactionalWrite: true,
		Timeout:            5 * time.Second,
	}, WithLogger(&testLogger{t}))
	require.NoError(t, err)
	require.NoError(t, srv.Start(context.Background()))
	defer srv.Close(context.Background()) //nolint:errcheck
	startReadLoop(t, srv)

	// Pre-fill txnStates to the max.
	srv.txnMu.Lock()
	for i := 0; i < maxTxnStates; i++ {
		srv.txnStates[fmt.Sprintf("fill-%d", i)] = &txnState{}
	}
	srv.txnMu.Unlock()

	addr := srv.Addr().String()
	waitForTCPReady(t, addr, 5*time.Second)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	apiReq := &kmsg.ApiVersionsRequest{Version: 0}
	sendRawRequest(t, conn, int16(kmsg.ApiVersions), 0, 0, apiReq.AppendTo(nil))

	txnID := "one-too-many"
	resp := sendInitProducerID(t, conn, 1, &txnID, 30000)
	assert.NotEqual(t, int16(0), resp.ErrorCode, "should reject when txnStates is full")
}

