package format

import (
	"encoding/base64"
	"encoding/json"

	"github.com/marre/ksink/pkg/ksink"
)

// jsonBase64Record is the JSON structure written by the json-base64 formatter.
type jsonBase64Record struct {
	Topic      string            `json:"topic"`
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Key        string            `json:"key,omitempty"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Timestamp  string            `json:"timestamp,omitempty"`
	ClientAddr string            `json:"client_addr"`
	Encoding   string            `json:"encoding"`
}

type jsonBase64Formatter struct {
	separator []byte
}

func (f *jsonBase64Formatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonBase64Record{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Value:      base64.StdEncoding.EncodeToString(msg.Value),
		Headers:    msg.Headers,
		ClientAddr: msg.ClientAddr,
		Encoding:   "base64",
	}
	if msg.Key != nil {
		rec.Key = base64.StdEncoding.EncodeToString(msg.Key)
	}
	if !msg.Timestamp.IsZero() {
		rec.Timestamp = msg.Timestamp.String()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return append(data, f.separator...), nil
}
