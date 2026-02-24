# ksrv

A lightweight Kafka-protocol-compatible server library for Go. It accepts produce requests from standard Kafka producers without requiring a full Kafka cluster.

## Features

- Accepts produce requests from any Kafka producer (kafka-console-producer, librdkafka, franz-go, etc.)
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- TLS and mutual TLS (mTLS)
- Topic filtering
- Idempotent producer support
- Callback-based message handling

## Installation

```bash
go get github.com/marre/ksrv
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/marre/ksrv"
)

func main() {
    handler := func(ctx context.Context, msgs []*ksrv.Message) error {
        for _, msg := range msgs {
            fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
        }
        return nil
    }

    srv, err := ksrv.New(ksrv.Config{
        Address: ":9092",
    }, handler)
    if err != nil {
        log.Fatal(err)
    }

    if err := srv.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer srv.Close(context.Background())

    log.Printf("Listening on %s", srv.Addr())
    select {} // Block forever
}
```

Then send messages using any Kafka producer:

```bash
echo "hello world" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

## Configuration

```go
cfg := ksrv.Config{
    Address:           ":9092",           // Listen address
    AdvertisedAddress: "myhost:9092",     // Address advertised to clients
    Topics:            []string{"events"},// Restrict to specific topics
    CertFile:          "server.pem",      // TLS certificate
    KeyFile:           "server-key.pem",  // TLS private key
    MTLSAuth:          "require_and_verify",
    MTLSCAsFiles:      []string{"ca.pem"},
    SASL: []ksrv.SASLCredential{
        {Mechanism: "PLAIN", Username: "user", Password: "pass"},
    },
    Timeout:         30 * time.Second,
    IdleTimeout:     60 * time.Second,
    MaxMessageBytes: 1048576,
    IdempotentWrite: false,
}
```

## Example

See [cmd/example](cmd/example) for a complete example that writes all received messages to a JSONL file.

```bash
go run ./cmd/example -addr :9092 -output messages.jsonl
```
