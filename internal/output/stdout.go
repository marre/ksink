package output

import (
	"os"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// stdoutWriter writes messages to standard output.
type stdoutWriter struct {
	mu sync.Mutex
}

func newStdoutWriter() *stdoutWriter {
	return &stdoutWriter{}
}

func (w *stdoutWriter) Write(data []byte, _ *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := os.Stdout.Write(data)
	return err
}

func (w *stdoutWriter) Close() error { return nil }
