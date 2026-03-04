package format

import (
	"encoding/base64"
	"encoding/json"

	"github.com/marre/ksink/pkg/ksink"
)

type jsonBase64Formatter struct {
	separator []byte
}

func (f *jsonBase64Formatter) Format(msg *ksink.Message) ([]byte, error) {
	rec := jsonRecord{
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		Value:      base64.StdEncoding.EncodeToString(msg.Value),
		Headers:    msg.Headers,
		ClientAddr: msg.ClientAddr,
		Encoding:   "base64",
	}
	if msg.Key != nil {
		k := base64.StdEncoding.EncodeToString(msg.Key)
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
