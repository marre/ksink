package format

import "github.com/marre/ksink/pkg/ksink"

// textFormatter writes the message value as-is. It is semantically distinct
// from binaryFormatter to allow independent evolution (e.g. adding charset
// handling in the future).
type textFormatter struct {
	separator []byte
}

func (f *textFormatter) Format(msg *ksink.Message) ([]byte, error) {
	return append(append([]byte(nil), msg.Value...), f.separator...), nil
}
