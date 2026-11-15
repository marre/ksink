package ksink

import (
    "bufio"
    "context"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
)

// The child process is intentionally killed without running deferred cleanup.
// This exercises the kernel lock-release semantics rather than an orderly
// TransactionLog.Close, and detects accidental concurrent WAL ownership.
func TestWALFaultExclusiveProcessOwnership(t *testing.T) {
    if os.Getenv("KSINK_TEST_WAL_OWNER_CHILD") == "1" {
        wal, err := OpenTransactionLog(os.Getenv("KSINK_TEST_WAL_OWNER_DIR"))
        if err != nil {
            _, _ = fmt.Fprintln(os.Stderr, err)
            os.Exit(2)
        }
        _, _ = fmt.Fprintln(os.Stdout, "wal-owner-ready")
        var buf [1]byte
        _, _ = os.Stdin.Read(buf[:]) // parent will SIGKILL, not close the WAL
        _ = wal.Close()
        os.Exit(0)
    }

    stateDir := filepath.Join(t.TempDir(), "wal")
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALFaultExclusiveProcessOwnership$")
    child.Env = append(os.Environ(),
        "KSINK_TEST_WAL_OWNER_CHILD=1", "KSINK_TEST_WAL_OWNER_DIR="+stateDir)
    child.Stderr = os.Stderr
    pipe, err := child.StdoutPipe()
    require.NoError(t, err)
    stdin, err := child.StdinPipe() // keep open: child blocks holding its WAL lock
    require.NoError(t, err)
    defer func() { _ = stdin.Close() }()
    require.NoError(t, child.Start())
    t.Cleanup(func() {
        if child.ProcessState == nil {
            _ = child.Process.Kill()
            _ = child.Wait()
        }
    })

    ready, err := bufio.NewReader(pipe).ReadString('\n')
    require.NoError(t, err)
    require.Equal(t, "wal-owner-ready", strings.TrimSpace(ready))

    _, err = OpenTransactionLog(stateDir)
    require.Error(t, err, "two independent processes must not write one WAL")
    require.NoError(t, child.Process.Kill())
    require.Error(t, child.Wait(), "SIGKILL should terminate the owning process")

    recovered, err := OpenTransactionLog(stateDir)
    require.NoError(t, err, "the OS must release the WAL lock on process death")
    require.NoError(t, recovered.Close())
}
