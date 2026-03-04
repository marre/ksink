package format

import (
	"encoding/json"

	"github.com/marre/ksink/pkg/ksink"
)

// jsonRecord is the JSON structure written by the json formatter.
type jsonRecord struct {
	Topic      string            `json:"topic"`
	Partition  int32             `json:"partition"`
	Offset     int64             `json:"offset"`
	Key        *string           `json:"key,omitempty"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Timestamp  string            `json:"timestamp,omitempty"`
	ClientAddr string            `json:"client_addr"`
}

type jsonFormatter struct {
	separator []byte
}

func (f *jsonFormatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonRecord{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Value:      string(msg.Value),
		Headers:    msg.Headers,
		ClientAddr: msg.ClientAddr,
	}
	if msg.Key != nil {
		k := string(msg.Key)
		rec.Key = &k
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
