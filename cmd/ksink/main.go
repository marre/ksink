// Command ksink starts a Kafka-protocol-compatible server and forwards all
// received messages to an output sink (file, TCP socket, or nanomsg).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

// tlsOpts holds TLS configuration options for output connections.
type tlsOpts struct {
	certFile    string
	keyFile     string
	caFile      string
	skipVerify  bool
}

// buildTLSConfig creates a *tls.Config from the TLS options.
// Returns nil if no TLS options are set.
func (o *tlsOpts) buildTLSConfig() (*tls.Config, error) {
	if o.certFile == "" && o.keyFile == "" && o.caFile == "" && !o.skipVerify {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.skipVerify, //nolint:gosec // User-requested for testing
	}

	// Client certificate for mTLS
	if o.certFile != "" && o.keyFile != "" {
		cert, err := tls.LoadX509KeyPair(o.certFile, o.keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// CA certificate for server verification
	if o.caFile != "" {
		caCert, err := os.ReadFile(o.caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", o.caFile)
		}
		cfg.RootCAs = pool
	}

	return cfg, nil
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
// Supports both plain TCP and TLS connections.
type tcpWriter struct {
	conn net.Conn
	mu   sync.Mutex
}

func newTCPWriter(addr string, tlsCfg *tls.Config) (*tcpWriter, error) {
	var conn net.Conn
	var err error
	if tlsCfg != nil {
		conn, err = tls.Dial("tcp", addr, tlsCfg)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
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
// Supports both plain TCP and TLS (tls+tcp://) transports.
type nanomsgWriter struct {
	sock mangos.Socket
}

func newNanomsgWriter(url string, tlsCfg *tls.Config) (*nanomsgWriter, error) {
	sock, err := push.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("failed to create nanomsg PUSH socket: %w", err)
	}
	if tlsCfg != nil {
		opts := map[string]interface{}{mangos.OptionTLSConfig: tlsCfg}
		if err := sock.DialOptions(url, opts); err != nil {
			sock.Close()
			return nil, fmt.Errorf("failed to dial nanomsg %s: %w", url, err)
		}
	} else {
		if err := sock.Dial(url); err != nil {
			sock.Close()
			return nil, fmt.Errorf("failed to dial nanomsg %s: %w", url, err)
		}
	}
	return &nanomsgWriter{sock: sock}, nil
}

func (w *nanomsgWriter) Write(data []byte) error {
	return w.sock.Send(data)
}

func (w *nanomsgWriter) Close() error { return w.sock.Close() }

// openWriter creates a writer based on the output string.
//
// Supported schemes:
//   - tcp://host:port             Plain TCP
//   - tls://host:port             TLS-wrapped TCP
//   - nanomsg://tcp://host:port   Nanomsg over plain TCP
//   - nanomsg://tls+tcp://host:port  Nanomsg over TLS
//   - <path>                      File output
func openWriter(output string, tlsCfg *tls.Config) (writer, error) {
	switch {
	case strings.HasPrefix(output, "tls://"):
		addr := strings.TrimPrefix(output, "tls://")
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return newTCPWriter(addr, tlsCfg)
	case strings.HasPrefix(output, "tcp://"):
		addr := strings.TrimPrefix(output, "tcp://")
		return newTCPWriter(addr, tlsCfg)
	case strings.HasPrefix(output, "nanomsg://"):
		url := strings.TrimPrefix(output, "nanomsg://")
		return newNanomsgWriter(url, tlsCfg)
	default:
		return newFileWriter(output)
	}
}

func main() {
	var (
		addr   string
		output string
		tOpts  tlsOpts
	)

	rootCmd := &cobra.Command{
		Use:   "ksink",
		Short: "A lightweight Kafka-protocol-compatible message sink",
		Long: `ksink accepts produce requests from Kafka producers and forwards
received messages to an output sink.

Output formats:
  messages.jsonl                    Write JSON lines to a file (default)
  tcp://host:port                   Connect as a TCP client and send JSON lines
  tls://host:port                   Connect over TLS and send JSON lines
  nanomsg://tcp://host:port         Send messages over a nanomsg PUSH socket
  nanomsg://tls+tcp://host:port     Send messages over a nanomsg PUSH socket with TLS

Use --output-tls-* flags to configure client certificates (mTLS) and
CA certificates for server verification on tcp, tls, and nanomsg outputs.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(addr, output, tOpts)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", ":9092", "Address to listen on")
	rootCmd.Flags().StringVar(&output, "output", "messages.jsonl",
		"Output destination (file path, tcp://, tls://, or nanomsg:// URL)")
	rootCmd.Flags().StringVar(&tOpts.certFile, "output-tls-cert", "",
		"Client certificate file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.keyFile, "output-tls-key", "",
		"Client private key file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.caFile, "output-tls-ca", "",
		"CA certificate file for verifying the output server")
	rootCmd.Flags().BoolVar(&tOpts.skipVerify, "output-tls-skip-verify", false,
		"Skip TLS certificate verification for output connections")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr, output string, tOpts tlsOpts) error {
	tlsCfg, err := tOpts.buildTLSConfig()
	if err != nil {
		return err
	}

	w, err := openWriter(output, tlsCfg)
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

				data, err := json.Marshal(rec)
				if err != nil {
					writeErr = fmt.Errorf("failed to marshal message: %w", err)
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
