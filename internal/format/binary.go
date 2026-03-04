package format

import "github.com/marre/ksink/pkg/ksink"

// binaryFormatter writes the raw message value bytes. It is kept separate
// from textFormatter for clarity even though the implementation is currently
// identical.
type binaryFormatter struct {
	separator []byte
}

func (f *binaryFormatter) Format(msg *ksink.Message) ([]byte, error) {
	return append(append([]byte(nil), msg.Value...), f.separator...), nil
}
