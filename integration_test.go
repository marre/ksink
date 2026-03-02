package ksink_test

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/marre/ksink"
)

// --- Shared integration test utilities ---

// checkDockerIntegration skips the test if Docker is not available.
func checkDockerIntegration(t *testing.T) {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to Docker: %v", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Could not ping Docker: %v", err)
	}
}

// getHostAddress returns the address to use from inside a Docker container to reach the host.
func getHostAddress() string {
	return "host.docker.internal"
}

func getIntegrationFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func integrationWaitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
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

// --- Integration message capture ---

type integrationReceivedMessage struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

type integrationMessageCapture struct {
	messages []integrationReceivedMessage
	mu       sync.Mutex
}

func (mc *integrationMessageCapture) add(msg integrationReceivedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.messages = append(mc.messages, msg)
}

func (mc *integrationMessageCapture) get() []integrationReceivedMessage {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	result := make([]integrationReceivedMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

func (mc *integrationMessageCapture) waitForMessages(count int, timeout time.Duration) []integrationReceivedMessage {
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

func (mc *integrationMessageCapture) waitForCount(count int, timeout time.Duration) bool {
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

func integrationStartReadLoop(t *testing.T, srv *ksink.Server) *integrationMessageCapture {
	t.Helper()
	capture := &integrationMessageCapture{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		for {
			msgs, ack, err := srv.ReadBatch(ctx)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				rm := integrationReceivedMessage{
					Topic:   msg.Topic,
					Value:   string(msg.Value),
					Headers: make(map[string]string),
				}
				if msg.Key != nil {
					rm.Key = string(msg.Key)
				}
				for k, v := range msg.Headers {
					rm.Headers[k] = v
				}
				capture.add(rm)
			}
			ack(nil)
		}
	}()

	return capture
}

// --- Integration test logger ---

type integrationLogger struct {
	t *testing.T
}

func (l *integrationLogger) Debugf(format string, args ...any) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *integrationLogger) Infof(format string, args ...any)  { l.t.Logf("[INFO] "+format, args...) }
func (l *integrationLogger) Warnf(format string, args ...any)  { l.t.Logf("[WARN] "+format, args...) }
func (l *integrationLogger) Errorf(format string, args ...any) {
	l.t.Logf("[ERROR] "+format, args...)
}

// --- Integration test server helper ---

func startIntegrationServer(t *testing.T, port int, cfg ksink.Config) *integrationMessageCapture {
	t.Helper()

	cfg.Address = fmt.Sprintf("0.0.0.0:%d", port)

	srv, err := ksink.New(cfg, ksink.WithLogger(&integrationLogger{t}))
	require.NoError(t, err)

	err = srv.Start(context.Background())
	require.NoError(t, err)

	integrationWaitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	t.Cleanup(func() {
		srv.Close(context.Background())
	})

	return integrationStartReadLoop(t, srv)
}

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

type dockerExecClient struct {
	pool     *dockertest.Pool
	resource *dockertest.Resource
	t        *testing.T
}

type dockerContainerOpts struct {
	repository string
	tag        string
	cmd        []string
	readyCmd   []string
}

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

	// Wait for container to be ready
	if len(opts.readyCmd) > 0 {
		err = pool.Retry(func() error {
			_, err := execInContainer(pool, resource, opts.readyCmd)
			return err
		})
		require.NoError(t, err, "Container failed to become ready")
	}

	t.Logf("Docker container started: %s:%s (ID: %s)", opts.repository, opts.tag, resource.Container.ID[:12])

	return dockerExecClient{
		pool:     pool,
		resource: resource,
		t:        t,
	}
}

func (c *dockerExecClient) Close() {
	if c.resource != nil {
		_ = c.pool.Purge(c.resource)
	}
}

func execInContainer(pool *dockertest.Pool, resource *dockertest.Resource, cmd []string) (string, error) {
	var stdout, stderr bytes.Buffer

	exitCode, err := resource.Exec(cmd, dockertest.ExecOptions{
		StdOut: &stdout,
		StdErr: &stderr,
	})
	if err != nil {
		return "", fmt.Errorf("exec failed: %w", err)
	}
	if exitCode != 0 {
		return stdout.String(), fmt.Errorf("exec returned code %d: %s", exitCode, stderr.String())
	}
	return stdout.String(), nil
}

func (c *dockerExecClient) exec(cmd []string) (string, string, error) {
	var stdout, stderr bytes.Buffer

	exitCode, err := c.resource.Exec(cmd, dockertest.ExecOptions{
		StdOut: &stdout,
		StdErr: &stderr,
	})
	if err != nil {
		return "", "", fmt.Errorf("exec failed: %w", err)
	}
	if exitCode != 0 {
		return stdout.String(), stderr.String(), fmt.Errorf("exec returned code %d: stdout=%s, stderr=%s", exitCode, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String(), nil
}

func (c *dockerExecClient) runProduce(desc string, cmd []string) error {
	stdout, stderr, err := c.exec(cmd)
	if err != nil {
		return fmt.Errorf("%s failed: %w\nstdout: %s\nstderr: %s", desc, err, stdout, stderr)
	}

	combined := stdout + stderr
	failurePatterns := []string{
		"ERROR",
		"WARN",
		"Exception",
		"TimeoutException",
		"TIMEOUT_OR_ERROR",
	}

	for _, pattern := range failurePatterns {
		if strings.Contains(combined, pattern) {
			return fmt.Errorf("%s had warnings/errors in output: %s", desc, combined)
		}
	}

	c.t.Logf("Produce succeeded: %s", desc)
	return nil
}

func (c *dockerExecClient) runProduceExpectFailure(desc string, cmd []string, patterns []string) error {
	stdout, stderr, _ := c.exec(cmd)
	combined := stdout + stderr

	for _, pattern := range patterns {
		if strings.Contains(combined, pattern) {
			return fmt.Errorf("expected auth failure detected: %s", pattern)
		}
	}

	return nil // No failure pattern found - the failure was expected but we got success
}

// --- kafkaProducer interface ---

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

// --- Integration test server helper ---

// (Defined above: startIntegrationServer)

// --- Shared integration test cases ---

func testBasicNoAuth(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	})

	err := client.produceMessage(hostAddr, "test-topic", `{"hello": "world"}`)
	require.NoError(t, err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "test-topic", msgs[0].Topic)
	assert.Equal(t, `{"hello": "world"}`, msgs[0].Value)
}

func testMultipleMessages(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	})

	messages := []string{
		`{"msg": 1}`,
		`{"msg": 2}`,
		`{"msg": 3}`,
	}
	err := client.produceMultipleMessages(hostAddr, "multi-topic", messages)
	require.NoError(t, err)

	msgs := capture.waitForMessages(3, 10*time.Second)
	require.Len(t, msgs, 3)
}

func testSASLPlain(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksink.SASLCredential{
			{Mechanism: "PLAIN", Username: "testuser", Password: "testpass"},
		},
	})

	err := client.produceWithSASLPlain(hostAddr, "sasl-topic", `{"auth": "plain"}`, "testuser", "testpass")
	require.NoError(t, err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"auth": "plain"}`, msgs[0].Value)
}

func testSASLPlainWrongPassword(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		SASL: []ksink.SASLCredential{
			{Mechanism: "PLAIN", Username: "testuser", Password: "testpass"},
		},
	})

	err := client.produceWithSASLPlainExpectFailure(hostAddr, "fail-topic", `{"should": "fail"}`, "testuser", "wrongpass")
	assert.Error(t, err)

	msgs := capture.waitForMessages(1, 2*time.Second)
	assert.Len(t, msgs, 0)
}

func testMessageWithKey(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
	})

	err := client.produceMessageWithKey(hostAddr, "key-topic", "my-key", `{"test": "with_key"}`)
	require.NoError(t, err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "key-topic", msgs[0].Topic)
	assert.Equal(t, "my-key", msgs[0].Key)
	assert.Equal(t, `{"test": "with_key"}`, msgs[0].Value)
}

func testIdempotentProducer(t *testing.T, client kafkaProducer) {
	port := getIntegrationFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := startIntegrationServer(t, port, ksink.Config{
		AdvertisedAddress: hostAddr,
		Timeout:           10 * time.Second,
		IdempotentWrite:   true,
	})

	err := client.produceMessageIdempotent(hostAddr, "idempotent-topic", `{"idempotent": true}`)
	require.NoError(t, err)

	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, `{"idempotent": true}`, msgs[0].Value)
}

// runIntegrationSubtests dispatches the standard set of integration subtests.
func runIntegrationSubtests(t *testing.T, client kafkaProducer, scramSkipReason string) {
	t.Run("basic_no_auth", func(t *testing.T) {
		t.Parallel()
		testBasicNoAuth(t, client)
	})

	t.Run("multiple_messages", func(t *testing.T) {
		t.Parallel()
		testMultipleMessages(t, client)
	})

	t.Run("sasl_plain", func(t *testing.T) {
		t.Parallel()
		testSASLPlain(t, client)
	})

	t.Run("sasl_plain_wrong_password", func(t *testing.T) {
		t.Parallel()
		testSASLPlainWrongPassword(t, client)
	})

	t.Run("message_with_key", func(t *testing.T) {
		t.Parallel()
		testMessageWithKey(t, client)
	})

	t.Run("idempotent_producer", func(t *testing.T) {
		t.Parallel()
		testIdempotentProducer(t, client)
	})
}
