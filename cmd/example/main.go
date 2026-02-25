// Command example starts a Kafka-protocol-compatible server and writes all
// received messages to a file (one JSON line per message).
//
// Usage:
//
//	go run ./cmd/example -addr :9092 -output messages.jsonl
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/marre/ksink"
)

type stdLogger struct{}

func (stdLogger) Debugf(format string, args ...any) {}
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
	addr := flag.String("addr", ":9092", "Address to listen on")
	output := flag.String("output", "messages.jsonl", "Output file path")
	flag.Parse()

	f, err := os.OpenFile(*output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file: %v", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	var mu sync.Mutex

	handler := func(_ context.Context, msgs []*ksink.Message) error {
		mu.Lock()
		defer mu.Unlock()

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
				return fmt.Errorf("failed to write message: %w", err)
			}
		}
		return nil
	}

	srv, err := ksink.New(ksink.Config{
		Address: *addr,
	}, handler, ksink.WithLogger(stdLogger{}))
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	log.Printf("Kafka server listening on %s, writing to %s", srv.Addr(), *output)
	log.Printf("Use any Kafka producer to send messages, e.g.:")
	log.Printf("  echo 'hello world' | kafka-console-producer.sh --bootstrap-server localhost%s --topic test", *addr)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	srv.Close(context.Background())
}
