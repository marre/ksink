// Command ksink starts a Kafka-protocol-compatible server and forwards all
// received messages to an output sink (file, HTTP, or stdout).
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
	"time"

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
		addr          string
		dst           string
		fmtName       string
		fmtStr        string
		jsonB64Key    bool
		jsonB64Val    bool
		separator     string
		sepHex        string
		noSeparator   bool
		transactional bool
		tOpts         output.TLSOpts
		httpOpts      output.HTTPOpts
	)

	rootCmd := &cobra.Command{
		Use:     "ksink",
		Short:   "A lightweight Kafka-protocol-compatible message sink",
		Version: fmt.Sprintf("%s (commit=%s, date=%s)", version, commit, date),
		Long: `ksink accepts produce requests from Kafka producers and forwards
received messages to an output sink.

Output destinations (--output):
  -                                 Write to stdout (default)
  messages.jsonl                    Write to a file
  http://host:port/path             POST messages via HTTP
  https://host:port/path            POST messages via HTTPS

Use --output-tls-* flags to configure client certificates (mTLS) and
CA certificates for server verification on https outputs.

HTTP output sends one message at a time and waits for a 200 OK response
before proceeding to the next message. Use --output-http-* flags to
configure retries and a dead-letter queue for failed messages.

Message formats (--output-format):
  binary       Raw message value bytes (default, newline-delimited)
  jsonl        JSON lines with configurable UTF-8/base64 encoding for key/value
  kcat         kcat-compatible format string (requires --output-format-string)

JSON format options:
	--output-json-base64-key      Encode the JSON key field as base64
	--output-json-base64-value    Encode the JSON value field as base64

Separator options (binary format only):
  --output-separator            Separator appended after each message (default: \n)
  --output-separator-hex        Separator as hex-encoded bytes
  --no-separator                Clear the separator entirely

kcat format specifiers (--output-format-string):
  %t  topic       %k  key         %s  value (payload)
  %p  partition   %o  offset      %T  timestamp (Unix ms)
  %K  key length  %S  value length
  %%  literal %%   \n  newline     \t  tab     \\  backslash`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if fmtName != "binary" {
				if cmd.Flags().Changed("output-separator") || cmd.Flags().Changed("output-separator-hex") || cmd.Flags().Changed("no-separator") {
					return fmt.Errorf("--output-separator, --output-separator-hex, and --no-separator are only supported with --output-format=binary")
				}
			}
			return run(addr, dst, fmtName, fmtStr, jsonB64Key, jsonB64Val, separator, sepHex, noSeparator, transactional, tOpts, httpOpts)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", ":9092", "Address to listen on")
	rootCmd.Flags().BoolVar(&transactional, "transactional", false,
		"Enable transactional produce support (per-transaction files with rename-as-commit/delete-as-abort). Requires {txnID} in --output.")
	rootCmd.Flags().StringVar(&dst, "output", "-",
		`Output destination: "-" for stdout (default), file path, or http(s):// URL.
With --transactional, must be a pattern containing {txnID}
(e.g. "messages-{txnID}.jsonl" produces "messages-my-txn.jsonl").`)
	rootCmd.Flags().StringVar(&fmtName, "output-format", "binary",
		"Message format: binary, jsonl, kcat")
	rootCmd.Flags().StringVar(&fmtStr, "output-format-string", "",
		`kcat-compatible format string (e.g. "%t %k %s\n"). Required when --output-format=kcat.`)
	rootCmd.Flags().BoolVar(&jsonB64Key, "output-json-base64-key", false,
		"Encode the JSON key field as base64 when --output-format=jsonl")
	rootCmd.Flags().BoolVar(&jsonB64Val, "output-json-base64-value", false,
		"Encode the JSON value field as base64 when --output-format=jsonl")
	rootCmd.Flags().StringVar(&separator, "output-separator", "\n",
		`Separator appended after each binary message. Escape sequences \n, \r, \t and \0 are interpreted.`)
	rootCmd.Flags().StringVar(&sepHex, "output-separator-hex", "",
		"Separator as hex-encoded bytes (e.g. \"0a\" for newline, \"00\" for null). Overrides --output-separator. Binary format only.")
	rootCmd.Flags().BoolVar(&noSeparator, "no-separator", false,
		"Clear the separator entirely (no delimiter between messages). Binary format only.")
	rootCmd.Flags().StringVar(&tOpts.CertFile, "output-tls-cert", "",
		"Client certificate file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.KeyFile, "output-tls-key", "",
		"Client private key file for output TLS/mTLS connections")
	rootCmd.Flags().StringVar(&tOpts.CAFile, "output-tls-ca", "",
		"CA certificate file for verifying the output server")
	rootCmd.Flags().BoolVar(&tOpts.SkipVerify, "output-tls-skip-verify", false,
		"Skip TLS certificate verification for output connections")
	rootCmd.Flags().IntVar(&httpOpts.MaxRetries, "output-http-retries", 0,
		"Number of retry attempts for failed HTTP requests (0 = no retries)")
	rootCmd.Flags().DurationVar(&httpOpts.RetryDelay, "output-http-retry-delay", time.Second,
		"Delay between HTTP retry attempts")
	rootCmd.Flags().StringVar(&httpOpts.DLQPath, "output-http-dlq", "",
		"File path for dead-letter queue; failed HTTP messages are appended here instead of stopping the process")
	rootCmd.Flags().DurationVar(&httpOpts.Timeout, "output-http-timeout", 30*time.Second,
		"HTTP request timeout per message")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "json-schema",
		Short: "Print the JSON schema for the jsonl output format to stdout",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(format.JSONSchema)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr, dst, fmtName, fmtStr string, jsonB64Key, jsonB64Val bool, separator, sepHex string, noSeparator, transactional bool, tOpts output.TLSOpts, httpOpts output.HTTPOpts) error {
	var sep []byte
	if fmtName == "binary" {
		var err error
		sep, err = buildSeparator(separator, sepHex, noSeparator)
		if err != nil {
			return err
		}
	}

	// Validate format-specific flag combinations before constructing anything.
	if fmtStr != "" && fmtName != "kcat" {
		return fmt.Errorf("--output-format-string is only supported with --output-format=kcat")
	}
	if (jsonB64Key || jsonB64Val) && fmtName != "jsonl" {
		return fmt.Errorf("--output-json-base64-key and --output-json-base64-value require --output-format=jsonl")
	}

	options := make([]format.Option, 0, 3)
	if fmtStr != "" {
		options = append(options, format.WithKcatFormatString(fmtStr))
	}
	if jsonB64Key {
		options = append(options, format.WithJSONBase64Key())
	}
	if jsonB64Val {
		options = append(options, format.WithJSONBase64Value())
	}
	fmtr, err := format.New(fmtName, sep, options...)
	if err != nil {
		return err
	}

	tlsCfg, err := tOpts.BuildTLSConfig()
	if err != nil {
		return err
	}

	var w output.Writer
	isHTTP := strings.HasPrefix(dst, "http://") || strings.HasPrefix(dst, "https://")
	if isHTTP {
		if transactional {
			return fmt.Errorf("--transactional is not supported with HTTP output")
		}
		w, err = output.NewHTTPWriter(dst, httpOpts, tlsCfg)
	} else if transactional && dst != "-" {
		w, err = output.NewTxnFileWriter(dst)
	} else {
		if transactional && dst == "-" {
			return fmt.Errorf("--transactional is not supported with stdout output")
		}
		w, err = output.Open(dst, tlsCfg)
	}
	if err != nil {
		return err
	}
	defer w.Close()

	serverOpts := []ksink.Option{ksink.WithLogger(stdLogger{})}
	if transactional {
		if tw, ok := w.(output.TransactionalWriter); ok {
			serverOpts = append(serverOpts, ksink.WithTxnEndFunc(func(txnID string, commit bool) {
				if commit {
					if err := tw.CommitTxn(txnID); err != nil {
						log.Printf("[ERROR] commit txn %s: %v", txnID, err)
					}
				} else {
					if err := tw.AbortTxn(txnID); err != nil {
						log.Printf("[ERROR] abort txn %s: %v", txnID, err)
					}
				}
			}))
		}
	}

	srv, err := ksink.New(ksink.Config{
		Address:            addr,
		TransactionalWrite: transactional,
	}, serverOpts...)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	outputLabel := dst
	if dst == "-" {
		outputLabel = "stdout"
	}
	log.Printf("Kafka server listening on %s, output=%s", srv.Addr(), outputLabel)

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

				if err := w.Write(data, msg); err != nil {
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

// buildSeparator returns the separator bytes. If noSep is true the separator
// is cleared. If hexStr is non-empty it is decoded as hex; otherwise the text
// separator is parsed for escape sequences.
func buildSeparator(text, hexStr string, noSep bool) ([]byte, error) {
	if noSep {
		return nil, nil
	}
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
