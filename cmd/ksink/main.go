// Command ksink starts a Kafka-protocol-compatible server and forwards all
// received messages to an output sink (file, TCP socket, or nanomsg).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/marre/ksink"
	"github.com/spf13/cobra"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/push"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

type stdLogger struct{}

func (stdLogger) Debugf(format string, args ...any) {}
func (stdLogger) Infof(format string, args ...any)  { log.Printf("[INFO] "+format, args...) }
func (stdLogger) Warnf(format string, args ...any)  { log.Printf("[WARN] "+format, args...) }
func (stdLogger) Errorf(format string, args ...any) { log.Printf("[ERROR] "+format, args...) }

// messageRecord is the JSON structure written to outputs.
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

// writer is the interface for message output backends.
type writer interface {
	io.Closer
	Write(data []byte) error
}

// fileWriter writes JSON lines to a file.
type fileWriter struct {
	f  *os.File
	mu sync.Mutex
}

func newFileWriter(path string) (*fileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open output file: %w", err)
	}
	return &fileWriter{f: f}, nil
}

func (w *fileWriter) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.f.Write(data)
	return err
}

func (w *fileWriter) Close() error { return w.f.Close() }

// tcpWriter connects as a TCP client and writes JSON lines.
type tcpWriter struct {
	conn net.Conn
	mu   sync.Mutex
}

func newTCPWriter(addr string) (*tcpWriter, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	return &tcpWriter{conn: conn}, nil
}

func (w *tcpWriter) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.conn.Write(data)
	return err
}

func (w *tcpWriter) Close() error { return w.conn.Close() }

// nanomsgWriter sends messages over a nanomsg PUSH socket.
type nanomsgWriter struct {
	sock mangos.Socket
}

func newNanomsgWriter(url string) (*nanomsgWriter, error) {
	sock, err := push.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("failed to create nanomsg PUSH socket: %w", err)
	}
	if err := sock.Dial(url); err != nil {
		sock.Close()
		return nil, fmt.Errorf("failed to dial nanomsg %s: %w", url, err)
	}
	return &nanomsgWriter{sock: sock}, nil
}

func (w *nanomsgWriter) Write(data []byte) error {
	return w.sock.Send(data)
}

func (w *nanomsgWriter) Close() error { return w.sock.Close() }

// openWriter creates a writer based on the output string.
// Supported schemes: tcp://<host:port>, nanomsg://<url>, or a file path.
func openWriter(output string) (writer, error) {
	switch {
	case strings.HasPrefix(output, "tcp://"):
		addr := strings.TrimPrefix(output, "tcp://")
		return newTCPWriter(addr)
	case strings.HasPrefix(output, "nanomsg://"):
		url := strings.TrimPrefix(output, "nanomsg://")
		return newNanomsgWriter(url)
	default:
		return newFileWriter(output)
	}
}

func main() {
	var addr string
	var output string

	rootCmd := &cobra.Command{
		Use:   "ksink",
		Short: "A lightweight Kafka-protocol-compatible message sink",
		Long: `ksink accepts produce requests from Kafka producers and forwards
received messages to an output sink.

Output formats:
  messages.jsonl            Write JSON lines to a file (default)
  tcp://host:port           Connect as a TCP client and send JSON lines
  nanomsg://tcp://host:port Send messages over a nanomsg PUSH socket`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(addr, output)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", ":9092", "Address to listen on")
	rootCmd.Flags().StringVar(&output, "output", "messages.jsonl", "Output destination (file path, tcp://host:port, or nanomsg://url)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr, output string) error {
	w, err := openWriter(output)
	if err != nil {
		return err
	}
	defer w.Close()

	srv, err := ksink.New(ksink.Config{
		Address: addr,
	}, ksink.WithLogger(stdLogger{}))
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	log.Printf("Kafka server listening on %s, output=%s", srv.Addr(), output)

	// Start read loop
	go func() {
		for {
			msgs, ack, readErr := srv.ReadBatch(ctx)
			if readErr != nil {
				return
			}

			var writeErr error
			for _, msg := range msgs {
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

				data, jsonErr := json.Marshal(rec)
				if jsonErr != nil {
					writeErr = fmt.Errorf("failed to marshal message: %w", jsonErr)
					break
				}
				data = append(data, '\n')

				if err := w.Write(data); err != nil {
					writeErr = fmt.Errorf("failed to write message: %w", err)
					break
				}
			}

			ack(writeErr)
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	srv.Close(context.Background())
	return nil
}
