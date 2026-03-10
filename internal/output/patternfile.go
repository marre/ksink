package output

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/marre/ksink/pkg/ksink"
)

// patternFileWriter writes messages to files whose paths are derived by
// replacing placeholders (e.g. {topic}) in a pattern string. Messages
// that resolve to different paths are written to separate files, opened
// lazily on first write.
type patternFileWriter struct {
	pattern string
	mu      sync.Mutex
	files   map[string]*os.File // resolved path → open file
}

func newPatternFileWriter(pattern string) *patternFileWriter {
	return &patternFileWriter{
		pattern: pattern,
		files:   make(map[string]*os.File),
	}
}

func (w *patternFileWriter) Write(data []byte, msg *ksink.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	topic := ""
	if msg != nil {
		topic = msg.Topic
	}

	path := w.resolvePath(topic)

	f, ok := w.files[path]
	if !ok {
		var err error
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}
		w.files[path] = f
	}

	_, err := f.Write(data)
	return err
}

func (w *patternFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	for _, f := range w.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	w.files = nil
	return firstErr
}

func (w *patternFileWriter) resolvePath(topic string) string {
	return strings.ReplaceAll(w.pattern, TopicPlaceholder, SanitizePathSegment(topic))
}
