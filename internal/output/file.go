package output

import (
	"fmt"
	"os"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// fileWriter writes JSON lines to a file.
type fileWriter struct {
	f  *os.File
	mu sync.Mutex
}

func newFileWriter(path string) (*fileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}
	return &fileWriter{f: f}, nil
}

func (w *fileWriter) Write(data []byte, _ *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.f.Write(data)
	return err
}

func (w *fileWriter) Close() error { return w.f.Close() }
