package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
)

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
