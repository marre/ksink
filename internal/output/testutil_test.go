package output_test

import (
    "context"
    "encoding/json"
    "fmt"
    "net"
    "strings"
    "testing"
    "time"

    "github.com/marre/ksink/pkg/ksink"
    "github.com/marre/ksink/internal/output"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "github.com/twmb/franz-go/pkg/kgo"
)

// messageRecord mirrors the structure written by the main command; tests
// duplicate it here so they remain self-contained and don't depend on the
// cmd/ksink package.
type messageRecord struct {
    Topic      string            `json:"topic"`
    Partition  int32             `json:"partition"`
    Offset     int64             `json:"offset"`
    Key        string            `json:"key,omitempty"`
    Value      string            `json:"value"`
    Headers    map[string]string `json:"headers,omitempty"`
    Timestamp  string            `json:"timestamp,omitempty"`
    ClientAddr string            `json:"client_addr"`
}

func getFreePort(t *testing.T) int {
    t.Helper()
    l, err := net.Listen("tcp", "127.0.0.1:0")
    require.NoError(t, err)
    port := l.Addr().(*net.TCPAddr).Port
    require.NoError(t, l.Close())
    return port
}

func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
        if err == nil {
            conn.Close() //nolint:errcheck
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// startKsinkServer starts a ksink server and returns it with its address.
func startKsinkServer(t *testing.T, ctx context.Context) (*ksink.Server, string) {
    t.Helper()
    port := getFreePort(t)
    addr := fmt.Sprintf("127.0.0.1:%d", port)

    srv, err := ksink.New(ksink.Config{
        Address: addr,
        Timeout: 5 * time.Second,
    })
    require.NoError(t, err)
    require.NoError(t, srv.Start(ctx))
    t.Cleanup(func() { srv.Close(context.Background()) }) //nolint:errcheck
    waitForTCPReady(t, addr, 5*time.Second)
    return srv, addr
}

// startReadWriteLoop runs the message forwarding loop (same as the main run loop).
func startReadWriteLoop(t *testing.T, srv *ksink.Server, w output.Writer) {
    t.Helper()
    ctx, cancel := context.WithCancel(context.Background())
    t.Cleanup(cancel)

    go func() {
        for {
            event, ack, err := srv.Read(ctx)
            if err != nil {
                return
            }
            var writeErr error
            if e, ok := event.(*ksink.MessagesEvent); ok {
                for _, msg := range e.Messages {
                    rec := messageRecord{
                        Topic:      msg.Topic,
                        Partition:  msg.Partition,
                        Offset:     msg.Offset,
                        Value:      string(msg.Value),
                        Headers:    msg.Headers,
                        ClientAddr: msg.ClientAddr,
                    }
                    if msg.Key != nil {
                        rec.Key = string(msg.Key)
                    }
                    if !msg.Timestamp.IsZero() {
                        rec.Timestamp = msg.Timestamp.String()
                    }
                    data, jerr := json.Marshal(rec)
                    if jerr != nil {
                        writeErr = jerr
                        break
                    }
                    data = append(data, '\n')
                    if werr := w.Write(data, msg); werr != nil {
                        writeErr = werr
                        break
                    }
                }
            }
            ack(writeErr)
        }
    }()
}

// produceMessages sends count messages via a franz-go client.
func produceMessages(t *testing.T, ctx context.Context, kafkaAddr string, count int) {
    t.Helper()
    client, err := kgo.NewClient(
        kgo.SeedBrokers(kafkaAddr),
        kgo.RequestTimeoutOverhead(5*time.Second),
        kgo.DisableIdempotentWrite(),
    )
    require.NoError(t, err)
    defer client.Close()

    for i := 0; i < count; i++ {
        record := &kgo.Record{
            Topic: "test-topic",
            Key:   []byte(fmt.Sprintf("key-%d", i)),
            Value: []byte(fmt.Sprintf("value-%d", i)),
            Headers: []kgo.RecordHeader{
                {Key: "header-key", Value: []byte(fmt.Sprintf("hval-%d", i))},
            },
        }
        results := client.ProduceSync(ctx, record)
        require.NoError(t, results[0].Err, "produce message %d", i)
    }
}

func splitJSONLines(data []byte) []string {
    var lines []string
    for _, line := range strings.Split(string(data), "\n") {
        if strings.TrimSpace(line) != "" {
            lines = append(lines, line)
        }
    }
    return lines
}

func verifyMessages(t *testing.T, lines []string, count int) {
    t.Helper()
    require.Len(t, lines, count)
    for i, line := range lines {
        var rec messageRecord
        require.NoError(t, json.Unmarshal([]byte(line), &rec), "unmarshal line %d", i)
        assert.Equal(t, "test-topic", rec.Topic)
        assert.Equal(t, fmt.Sprintf("key-%d", i), rec.Key)
        assert.Equal(t, fmt.Sprintf("value-%d", i), rec.Value)
        assert.Equal(t, fmt.Sprintf("hval-%d", i), rec.Headers["header-key"])
        assert.NotEmpty(t, rec.ClientAddr)
    }
}
