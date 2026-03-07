package format

import (
	"encoding/base64"
	"encoding/json"

	"github.com/marre/ksink/pkg/ksink"
)

// jsonRecord is the JSON structure written by the json formatter. The
// key/value encoding fields indicate how raw bytes were represented.
type jsonRecord struct {
	Topic         string            `json:"topic"`
	Partition     int32             `json:"partition"`
	Offset        int64             `json:"offset"`
	Key           *string           `json:"key,omitempty"`
	KeyEncoding   string            `json:"key_encoding,omitempty"`
	Value         string            `json:"value"`
	ValueEncoding string            `json:"value_encoding"`
	Headers       map[string]string `json:"headers,omitempty"`
	Timestamp     string            `json:"timestamp,omitempty"`
	ClientAddr    string            `json:"client_addr"`
}

type jsonFormatter struct {
	encodeKeyBase64   bool
	encodeValueBase64 bool
}

func (f *jsonFormatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonRecord{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Value:         encodeJSONField(msg.Value, f.encodeValueBase64),
		ValueEncoding: jsonEncodingName(f.encodeValueBase64),
		Headers:       msg.Headers,
		ClientAddr:    msg.ClientAddr,
	}
	if msg.Key != nil {
		k := encodeJSONField(msg.Key, f.encodeKeyBase64)
		rec.Key = &k
		rec.KeyEncoding = jsonEncodingName(f.encodeKeyBase64)
	}
	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp.String()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func encodeJSONField(data []byte, useBase64 bool) string {
	if useBase64 {
		return base64.StdEncoding.EncodeToString(data)
	}
	return string(data)
}

func jsonEncodingName(useBase64 bool) string {
	if useBase64 {
		return "base64"
	}
	return "utf-8"
}
