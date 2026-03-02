// Command ksink starts a Kafka-protocol-compatible server and forwards all
// received messages to an output sink (file, TCP socket, or nanomsg).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/marre/ksink"
	"github.com/spf13/cobra"
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
