package output

import (
	"crypto/tls"
	"fmt"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/push"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

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
