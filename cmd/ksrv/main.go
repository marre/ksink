// ksrv is a lightweight Kafka-protocol-compatible server that writes all
// received messages to a local file (one JSON object per line).
//
// Usage:
//
//	ksrv [flags]
//
// Flags:
//
//	-addr string       listen address (default "0.0.0.0:9092")
//	-output string     output file path (default "messages.jsonl")
//	-timeout duration  request timeout (default 5s)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marre/ksrv"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9092", "listen address")
	output := flag.String("output", "messages.jsonl", "output file path")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	f, err := os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file %s: %v", *output, err)
	}
	defer f.Close()

	srv, err := ksrv.New(ksrv.Options{
		Address: *addr,
		Timeout: *timeout,
		Logger:  newStdLogger(),
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Connect(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("Kafka server listening on %s, writing messages to %s", *addr, *output)

	// Handle OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	enc := json.NewEncoder(f)

	for {
		batch, ackFn, err := srv.ReadBatch(ctx)
		if err != nil {
			break
		}

		for _, msg := range batch {
			record := buildRecord(msg)
			if encErr := enc.Encode(record); encErr != nil {
				log.Printf("Failed to write message: %v", encErr)
			}
		}

		if err := ackFn(ctx, nil); err != nil {
			log.Printf("Failed to acknowledge batch: %v", err)
		}
	}

	if err := srv.Close(context.Background()); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
	log.Println("Server stopped")
}

// record is the JSON representation of a single Kafka message written to the
// output file.
type record struct {
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Offset        int64             `json:"offset"`
	Key           string            `json:"key,omitempty"`
	Value         string            `json:"value"`
	Timestamp     string            `json:"timestamp,omitempty"`
	ClientAddress string            `json:"client_address,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
}

func buildRecord(msg *ksrv.Message) record {
	r := record{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Value:         string(msg.Value),
		ClientAddress: msg.ClientAddress,
		Headers:       msg.Headers,
	}
	if msg.Key != nil {
		r.Key = string(msg.Key)
	}
	if !msg.Timestamp.IsZero() {
		r.Timestamp = msg.Timestamp.Format(time.RFC3339)
	}
	return r
}

// stdLogger wraps the standard log package to satisfy ksrv.Logger.
type stdLogger struct{}

func newStdLogger() ksrv.Logger { return stdLogger{} }

func (stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}
func (stdLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO]  "+format, args...)
}
func (stdLogger) Warnf(format string, args ...interface{}) {
	log.Printf("[WARN]  "+format, args...)
}
func (stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
func (stdLogger) Warn(msg string) {
	log.Printf("[WARN]  %s", msg)
}
