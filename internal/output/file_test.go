package output_test

import (
    "context"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/marre/ksink/internal/output"
    "github.com/stretchr/testify/require"
)

func TestOutputFile(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    tmpDir := t.TempDir()
    outputFile := filepath.Join(tmpDir, "output.jsonl")

    w, err := output.Open(outputFile, nil)
    require.NoError(t, err)
    t.Cleanup(func() { w.Close() }) //nolint:errcheck

    srv, kafkaAddr := startKsinkServer(t, ctx)
    startReadWriteLoop(t, srv, w)
    produceMessages(t, ctx, kafkaAddr, 3)

    // Poll until the file has the expected number of lines
    var lines []string
    deadline := time.Now().Add(5 * time.Second)
    for time.Now().Before(deadline) {
        data, err := os.ReadFile(outputFile)
        if err == nil {
            lines = splitJSONLines(data)
            if len(lines) >= 3 {
                break
            }
        }
        time.Sleep(100 * time.Millisecond)
    }

    verifyMessages(t, lines, 3)
}
