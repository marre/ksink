# ksink

A lightweight Kafka-protocol-compatible server library and tool for Go. It accepts produce requests from standard Kafka producers without requiring a full Kafka cluster.

## Features

- Accepts produce requests from any Kafka producer (kafka-console-producer, librdkafka, franz-go, etc.)
- Pull-based API: call `ReadBatch` to receive messages, then acknowledge
- SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- TLS and mutual TLS (mTLS)
- Topic filtering
- Idempotent producer support

## Installation

```bash
go get github.com/marre/ksink
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/marre/ksink"
)

func main() {
    srv, err := ksink.New(ksink.Config{
        Address: ":9092",
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := srv.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer srv.Close(ctx)

    log.Printf("Listening on %s", srv.Addr())

    for {
        msgs, ack, err := srv.ReadBatch(ctx)
        if err != nil {
            log.Fatal(err)
        }

        for _, msg := range msgs {
            fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
        }

        ack(nil) // acknowledge successful processing
    }
}
```

Then send messages using any Kafka producer:

```bash
echo "hello world" | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

## Configuration

```go
cfg := ksink.Config{
    Address:           ":9092",           // Listen address
    AdvertisedAddress: "myhost:9092",     // Address advertised to clients
    Topics:            []string{"events"},// Restrict to specific topics
    CertFile:          "server.pem",      // TLS certificate
    KeyFile:           "server-key.pem",  // TLS private key
    MTLSAuth:          "require_and_verify",
    MTLSCAsFiles:      []string{"ca.pem"},
    SASL: []ksink.SASLCredential{
        {Mechanism: "PLAIN", Username: "user", Password: "pass"},
    },
    Timeout:         30 * time.Second,
    IdleTimeout:     60 * time.Second,
    MaxMessageBytes: 1048576,
    IdempotentWrite: false,
}
```

## ksink Tool

The `cmd/ksink` tool forwards received messages to an output sink:

```bash
# Write JSON lines to a file (default)
go run ./cmd/ksink --addr :9092 --output messages.jsonl

# Send JSON lines over a TCP client socket
go run ./cmd/ksink --addr :9092 --output tcp://host:port

# Send JSON lines over a TLS-encrypted TCP connection
go run ./cmd/ksink --addr :9092 --output tls://host:port --output-tls-ca ca.pem

# TLS with mTLS client authentication
go run ./cmd/ksink --addr :9092 --output tls://host:port \
  --output-tls-ca ca.pem --output-tls-cert client.pem --output-tls-key client-key.pem

# Send messages over a nanomsg PUSH socket
go run ./cmd/ksink --addr :9092 --output nanomsg://tcp://host:port

# Send messages over a nanomsg PUSH socket with TLS
go run ./cmd/ksink --addr :9092 --output nanomsg://tls+tcp://host:port --output-tls-ca ca.pem
```
