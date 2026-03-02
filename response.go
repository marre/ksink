package ksink

import (
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (s *Server) sendResponse(conn net.Conn, connID uint64, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)

	// For flexible responses (EXCEPT ApiVersions key 18), add response header TAG_BUFFER
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0) // Empty TAG_BUFFER (0 tags)
	}

	buf = msg.AppendTo(buf)

	s.logger.Debugf("[conn:%d] Sending response: correlationID=%d, key=%d, size=%d", connID, correlationID, msg.Key(), len(buf))

	return s.writeResponse(connID, conn, buf)
}

func (s *Server) writeResponse(connID uint64, conn net.Conn, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout)); err != nil {
		s.logger.Errorf("[conn:%d] Failed to set write deadline: %v", connID, err)
		return err
	}

	fullData := kbin.AppendInt32(nil, int32(len(data)))
	fullData = append(fullData, data...)

	n, err := conn.Write(fullData)
	if err != nil {
		s.logger.Errorf("[conn:%d] Failed to write response: %v", connID, err)
		return err
	}
	s.logger.Debugf("[conn:%d] Wrote %d bytes response", connID, n)
	return nil
}
