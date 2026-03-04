// Command ksink starts a Kafka-protocol-compatible server and forwards all
// received messages to an output sink (file, TCP socket, or nanomsg).
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/marre/ksink/internal/format"
	"github.com/marre/ksink/internal/output"
	"github.com/marre/ksink/pkg/ksink"
	"github.com/spf13/cobra"
)

// Set via ldflags at build time by goreleaser.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type stdLogger struct{}

func (stdLogger) Debugf(format string, args ...any) {}
func (stdLogger) Infof(format string, args ...any)  { log.Printf("[INFO] "+format, args...) }
func (stdLogger) Warnf(format string, args ...any)  { log.Printf("[WARN] "+format, args...) }
func (stdLogger) Errorf(format string, args ...any) { log.Printf("[ERROR] "+format, args...) }

func main() {
	var (
		addr      string
		dst       string
		fmtName   string
		fmtStr    string
		separator string
		sepHex    string
		tOpts     output.TLSOpts
	)

	rootCmd := &cobra.Command{
		Use:     "ksink",
		Short:   "A lightweight Kafka-protocol-compatible message sink",
		Version: fmt.Sprintf("%s (commit=%s, date=%s)", version, commit, date),
		Long: `ksink accepts produce requests from Kafka producers and forwards
received messages to an output sink.

Output formats:
  messages.jsonl                    Write JSON lines to a file (default)
  tcp://host:port                   Connect as a TCP client and send JSON lines
  tls://host:port                   Connect over TLS and send JSON lines
  nanomsg://tcp://host:port         Send messages over a nanomsg PUSH socket
  nanomsg://tls+tcp://host:port     Send messages over a nanomsg PUSH socket with TLS

Use --output-tls-* flags to configure client certificates (mTLS) and
CA certificates for server verification on tcp, tls, and nanomsg outputs.

Message formats (--output-format):
  json         JSON lines with key/value as UTF-8 strings (default)
  json-base64  JSON lines with key/value base64-encoded (for binary data)
  text         Raw message value followed by the separator
  binary       Raw message value bytes with no separator by default
  kcat         kcat-compatible format string (requires --output-format-string)

kcat format specifiers (--output-format-string):
  %t  topic       %k  key         %s  value (payload)
  %p  partition   %o  offset      %T  timestamp (Unix ms)
  %K  key length  %S  value length
  %%  literal %%   \n  newline     \t  tab     \\  backslash`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(addr, dst, fmtName, fmtStr, separator, sepHex, tOpts)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", ":9092", "Address to listen on")
	rootCmd.Flags().StringVar(&dst, "output", "messages.jsonl",
		"Output destination (file path, tcp://, tls://, or nanomsg:// URL)")
	rootCmd.Flags().StringVar(&fmtName, "output-format", "json",
		"Message format: json, json-base64, text, binary, kcat")
	rootCmd.Flags().StringVar(&fmtStr, "output-format-string", "",
		`kcat-compatible format string (e.g. "%t %k %s\n"). Required when --output-format=kcat.`)
	rootCmd.Flags().StringVar(&separator, "output-separator", "\n",
		`Separator appended after each message. Escape sequences \n, \r, \t and \0 are interpreted.`)
	rootCmd.Flags().StringVar(&sepHex, "output-separator-hex", "",
		"Separator as hex-encoded bytes (e.g. \"0a\" for newline, \"00\" for null). Overrides --output-separator.")
	rootCmd.Flags().StringVar(&tOpts.CertFile, "output-tls-cert", "",
		"Client certificate file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.KeyFile, "output-tls-key", "",
		"Client private key file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.CAFile, "output-tls-ca", "",
		"CA certificate file for verifying the output server")
	rootCmd.Flags().BoolVar(&tOpts.SkipVerify, "output-tls-skip-verify", false,
		"Skip TLS certificate verification for output connections")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr, dst, fmtName, fmtStr, separator, sepHex string, tOpts output.TLSOpts) error {
	sep, err := buildSeparator(separator, sepHex)
	if err != nil {
		return err
	}
	fmtr, err := format.New(fmtName, sep, fmtStr)
	if err != nil {
		return err
	}

	tlsCfg, err := tOpts.BuildTLSConfig()
	if err != nil {
		return err
	}

	w, err := output.Open(dst, tlsCfg)
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

	log.Printf("Kafka server listening on %s, output=%s", srv.Addr(), dst)

	// Start read loop
	go func() {
		for {
			msgs, ack, readErr := srv.ReadBatch(ctx)
			if readErr != nil {
				return
			}

			var writeErr error
			for _, msg := range msgs {
				data, err := fmtr.Format(msg)
				if err != nil {
					writeErr = fmt.Errorf("failed to format message: %w", err)
					break
				}

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

// buildSeparator returns the separator bytes. If hexStr is non-empty it is
// decoded as hex; otherwise the text separator is parsed for escape sequences.
func buildSeparator(text, hexStr string) ([]byte, error) {
	if hexStr != "" {
		b, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid --output-separator-hex value: %w", err)
		}
		return b, nil
	}
	return parseSeparator(text), nil
}

// parseSeparator interprets common escape sequences in the separator string.
// Supported: \n (newline), \r (carriage return), \t (tab), \0 (null), \\ (literal backslash).
func parseSeparator(s string) []byte {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i++
			case 'r':
				b.WriteByte('\r')
				i++
			case 't':
				b.WriteByte('\t')
				i++
			case '0':
				b.WriteByte(0)
				i++
			case '\\':
				b.WriteByte('\\')
				i++
			default:
				b.WriteByte(s[i])
			}
		} else {
			b.WriteByte(s[i])
		}
	}
	return []byte(b.String())
}
