// Command ksink starts a Kafka-protocol-compatible server and writes all
// received messages to a file (one JSON line per message).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/marre/ksink"
	"github.com/spf13/cobra"
)

type stdLogger struct{}

func (stdLogger) Debugf(string, ...any) {}
func (stdLogger) Infof(format string, args ...any)  { log.Printf("[INFO] "+format, args...) }
func (stdLogger) Warnf(format string, args ...any)  { log.Printf("[WARN] "+format, args...) }
func (stdLogger) Errorf(format string, args ...any) { log.Printf("[ERROR] "+format, args...) }

// messageRecord is the JSON structure written to the output file.
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
	var addr string
	var output string

	rootCmd := &cobra.Command{
		Use:   "ksink",
		Short: "A lightweight Kafka-protocol-compatible message sink",
		Long:  "ksink accepts produce requests from Kafka producers and writes received messages to a file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(addr, output)
		},
	}

	rootCmd.Flags().StringVar(&addr, "addr", ":9092", "Address to listen on")
	rootCmd.Flags().StringVar(&output, "output", "messages.jsonl", "Output file path")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(addr, output string) error {
	f, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	var mu sync.Mutex

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

	log.Printf("Kafka server listening on %s, writing to %s", srv.Addr(), output)

	// Start read loop
	go func() {
		for {
			msgs, ack, readErr := srv.ReadBatch(ctx)
			if readErr != nil {
				return
			}

			mu.Lock()
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
				if err := enc.Encode(rec); err != nil {
					writeErr = fmt.Errorf("failed to write message: %w", err)
					break
				}
			}
			mu.Unlock()

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
