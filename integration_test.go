package ksrv_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/marre/ksrv"
)

// --- Shared utilities ---

// getHostAddress returns the address to use from inside a Docker container to
// reach the host.
func getHostAddress() string {
	return "host.docker.internal"
}

func getFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// waitForTCPReady polls until the given TCP address accepts connections.
func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// --- Message capture ---

// receivedMessage holds a captured message from the server.
type receivedMessage struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// messageCapture accumulates messages received by the server.
type messageCapture struct {
	messages []receivedMessage
	mu       sync.Mutex
}

func (mc *messageCapture) add(msg receivedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.messages = append(mc.messages, msg)
}

func (mc *messageCapture) get() []receivedMessage {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	result := make([]receivedMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

func (mc *messageCapture) waitForMessages(count int, timeout time.Duration) []receivedMessage {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := mc.get()
		if len(msgs) >= count {
			return msgs
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mc.get()
}

func (mc *messageCapture) waitForCount(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mc.mu.Lock()
		n := len(mc.messages)
		mc.mu.Unlock()
		if n >= count {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// --- runKafkaServerTestWithCapture ---

// runKafkaServerTestWithCapture starts a ksrv.Server, drains messages into
// capture, runs testFn, then shuts the server down.
func runKafkaServerTestWithCapture(t *testing.T, port int, opts ksrv.Options, testFn func(ctx context.Context, capture *messageCapture)) {
	t.Helper()

	srv, err := ksrv.New(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, srv.Connect(ctx))

	capture := &messageCapture{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			batch, ackFn, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			for _, msg := range batch {
				rm := receivedMessage{
					Topic:   msg.Topic,
					Value:   string(msg.Value),
					Headers: msg.Headers,
				}
				if msg.Key != nil {
					rm.Key = string(msg.Key)
				}
				capture.add(rm)
			}
			_ = ackFn(ctx, nil)
		}
	}()

	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	testFn(ctx, capture)

	cancel()
	_ = srv.Close(context.Background())
	wg.Wait()
}

// --- kafkaProducer interface ---

// kafkaProducer is an interface that both kafkaDockerClient and kcatClient
// implement, allowing reuse of common integration test logic.
type kafkaProducer interface {
	Close()
	produceMessage(brokerAddr, topic, value string) error
	produceMessageWithKey(brokerAddr, topic, key, value string) error
	produceMultipleMessages(brokerAddr, topic string, messages []string) error
	produceMessageIdempotent(brokerAddr, topic, value string) error
	produceWithSASLPlain(brokerAddr, topic, value, username, password string) error
	produceWithSASLPlainExpectFailure(brokerAddr, topic, value, username, password string) error
	produceWithSASLScram(brokerAddr, topic, value, username, password, mechanism string) error
	produceWithSASLScramExpectFailure(brokerAddr, topic, value, username, password, mechanism string) error
}

// --- Docker container helpers ---

// newDockerPool creates a dockertest pool, skipping the test if Docker is
// unavailable.
func newDockerPool(t *testing.T) *dockertest.Pool {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to Docker: %v", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Could not ping Docker: %v", err)
	}
	pool.MaxWait = time.Minute
	return pool
}

// dockerExecClient wraps a running Docker container and provides exec helpers.
type dockerExecClient struct {
	pool     *dockertest.Pool
	resource *dockertest.Resource
	t        *testing.T
}

// dockerContainerOpts configures a container for testing.
type dockerContainerOpts struct {
	repository string
	tag        string
	cmd        []string
	readyCmd   []string
}

// newDockerExecClient starts a container and waits until the readyCmd succeeds.
func newDockerExecClient(t *testing.T, pool *dockertest.Pool, opts dockerContainerOpts) dockerExecClient {
	t.Helper()

	runOpts := &dockertest.RunOptions{
		Repository: opts.repository,
		Tag:        opts.tag,
		Cmd:        opts.cmd,
	}
	if runtime.GOOS == "linux" {
		runOpts.ExtraHosts = []string{"host.docker.internal:host-gateway"}
	}

	resource, err := pool.RunWithOptions(runOpts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	client := dockerExecClient{pool: pool, resource: resource, t: t}

	err = pool.Retry(func() error {
		_, _, err := client.exec(opts.readyCmd)
		return err
	})
	require.NoError(t, err)

	return client
}

func (c *dockerExecClient) exec(cmd []string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	exitCode, err := c.resource.Exec(cmd, dockertest.ExecOptions{
		StdOut: &stdout,
		StdErr: &stderr,
	})
	if err != nil {
		return stdout.String(), stderr.String(), fmt.Errorf("exec %v: %w (stdout=%q, stderr=%q)", cmd, err, stdout.String(), stderr.String())
	}
	if exitCode != 0 {
		return stdout.String(), stderr.String(), fmt.Errorf("exec %v exited with code %d (stdout=%q, stderr=%q)", cmd, exitCode, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String(), nil
}

func (c *dockerExecClient) Close() {
	if err := c.pool.Purge(c.resource); err != nil {
		c.t.Logf("Failed to purge container: %v", err)
	}
}

func (c *dockerExecClient) runProduce(desc string, cmd []string) error {
	stdout, stderr, err := c.exec(cmd)
	if err != nil {
		return fmt.Errorf("produce %s failed: %w (stdout=%q, stderr=%q)", desc, err, stdout, stderr)
	}
	c.t.Logf("Produced %s: stdout=%q, stderr=%q", desc, stdout, stderr)
	return nil
}

func (c *dockerExecClient) runProduceExpectFailure(desc string, cmd []string, failurePatterns []string) error {
	stdout, stderr, _ := c.exec(cmd)
	combined := stdout + stderr
	for _, pattern := range failurePatterns {
		if strings.Contains(combined, pattern) {
			return fmt.Errorf("authentication failure confirmed: %s", pattern)
		}
	}
	return fmt.Errorf("expected auth failure for %s but got: stdout=%q stderr=%q", desc, stdout, stderr)
}

// --- Shared test functions ---

func testKafkaServerBasicNoAuth(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessage(hostAddr, "test-topic", `{"test": "message"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		require.Equal(t, "test-topic", msgs[0].Topic)
		require.Equal(t, `{"test": "message"}`, msgs[0].Value)
		t.Logf("Basic message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerMultipleMessages(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		messages := []string{`{"id": 1}`, `{"id": 2}`, `{"id": 3}`}
		err := client.produceMultipleMessages(hostAddr, "multi-topic", messages)
		require.NoError(t, err)

		msgs := capture.waitForMessages(3, 10*time.Second)
		require.GreaterOrEqual(t, len(msgs), 1, "Expected at least 1 message")
		t.Logf("Received %d messages", len(msgs))
	})
}

func testKafkaServerSASLPlain(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlain(hostAddr, "auth-topic", `{"auth": "plain"}`, "user1", "pass1")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		require.Equal(t, "auth-topic", msgs[0].Topic)
		t.Logf("SASL PLAIN message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLPlainWrongPassword(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlainExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "user1", "wrongpassword")
		require.Error(t, err)
		t.Logf("SASL PLAIN wrong password correctly rejected: %v", err)

		msgs := capture.waitForMessages(1, 2*time.Second)
		require.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerSASLScram256(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram-topic", `{"auth": "scram256"}`, "scramuser", "scrampass", "SCRAM-SHA-256")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		require.Equal(t, "scram-topic", msgs[0].Topic)
		t.Logf("SASL SCRAM-SHA-256 message received: topic=%s", msgs[0].Topic)
	})
}

func testKafkaServerSASLScram512(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "SCRAM-SHA-512", Username: "scramuser", Password: "scrampass"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram-topic", `{"auth": "scram512"}`, "scramuser", "scrampass", "SCRAM-SHA-512")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		require.Equal(t, "scram-topic", msgs[0].Topic)
		t.Logf("SASL SCRAM-SHA-512 message received: topic=%s", msgs[0].Topic)
	})
}

func testKafkaServerSASLScramWrongPassword(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "SCRAM-SHA-256", Username: "scramuser", Password: "scrampass"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScramExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "scramuser", "wrongpassword", "SCRAM-SHA-256")
		require.Error(t, err)
		t.Logf("SASL SCRAM wrong password correctly rejected: %v", err)

		msgs := capture.waitForMessages(1, 2*time.Second)
		require.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerMessageWithKey(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessageWithKey(hostAddr, "key-topic", "my-key", `{"test": "with_key"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		require.Equal(t, "key-topic", msgs[0].Topic)
		require.Equal(t, "my-key", msgs[0].Key)
		require.Equal(t, `{"test": "with_key"}`, msgs[0].Value)
		t.Logf("Message with key received: topic=%s, key=%s", msgs[0].Topic, msgs[0].Key)
	})
}

func testKafkaServerIdempotentProducer(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		IdempotentWrite:   true,
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessageIdempotent(hostAddr, "idempotent-topic", `{"message": "hello from idempotent producer"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1)
		require.Equal(t, "idempotent-topic", msgs[0].Topic)
		t.Logf("Idempotent producer message received: topic=%s", msgs[0].Topic)
	})
}

func testKafkaServerMultipleUsers(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	opts := ksrv.Options{
		Address:           fmt.Sprintf("0.0.0.0:%d", port),
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksrv.SASLCredential{
			{Mechanism: "PLAIN", Username: "user1", Password: "pass1"},
			{Mechanism: "PLAIN", Username: "user2", Password: "pass2"},
		},
	}

	runKafkaServerTestWithCapture(t, port, opts, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlain(hostAddr, "user1-topic", `{"user": "user1"}`, "user1", "pass1")
		require.NoError(t, err)

		err = client.produceWithSASLPlain(hostAddr, "user2-topic", `{"user": "user2"}`, "user2", "pass2")
		require.NoError(t, err)

		msgs := capture.waitForMessages(2, 10*time.Second)
		require.Len(t, msgs, 2)
		t.Logf("Both users' messages received successfully")
	})
}

// --- Shared test dispatch ---

// runKafkaServerSubtests dispatches the standard set of kafka server integration
// subtests.  If scramSkipReason is non-empty, SCRAM tests are skipped.
func runKafkaServerSubtests(t *testing.T, client kafkaProducer, scramSkipReason string) {
	t.Run("basic_no_auth", func(t *testing.T) {
		t.Parallel()
		testKafkaServerBasicNoAuth(t, client)
	})

	t.Run("multiple_messages", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMultipleMessages(t, client)
	})

	t.Run("sasl_plain", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlain(t, client)
	})

	t.Run("sasl_plain_wrong_password", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlainWrongPassword(t, client)
	})

	t.Run("sasl_scram_sha256", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScram256(t, client)
	})

	t.Run("sasl_scram_sha512", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScram512(t, client)
	})

	t.Run("sasl_scram_wrong_password", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScramWrongPassword(t, client)
	})

	t.Run("message_with_key", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMessageWithKey(t, client)
	})

	t.Run("idempotent_producer", func(t *testing.T) {
		t.Parallel()
		testKafkaServerIdempotentProducer(t, client)
	})
}
