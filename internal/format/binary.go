package format

import "github.com/marre/ksink/pkg/ksink"

// binaryFormatter writes the raw message value bytes.
type binaryFormatter struct {
	separator []byte
}

func (f *binaryFormatter) Format(msg *ksink.Message) ([]byte, error) {
	return append(append([]byte(nil), msg.Value...), f.separator...), nil
}
