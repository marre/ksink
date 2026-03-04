package format

import (
	"fmt"
	"strconv"

	"github.com/marre/ksink/pkg/ksink"
)

// kcatFormatter formats messages using a kcat-compatible format string.
//
// Supported format specifiers:
//
//	%t  topic name
//	%k  message key
//	%s  message value (payload)
//	%K  key length in bytes (-1 for nil key)
//	%S  value length in bytes
//	%o  message offset
//	%p  partition number
//	%T  message timestamp as Unix milliseconds (-1 if unset)
//
// Standard C escape sequences are supported: \n, \r, \t, \\, \0.
// A literal percent sign can be produced with %%.
type kcatFormatter struct {
	segments []segment
}

// segment is one piece of the compiled format: either literal bytes or a
// format verb that references a message field.
type segment struct {
	literal []byte
	verb    byte // 0 for literal-only segments
}

// compileKcatFormat parses a kcat format string into a slice of segments so
// that Format only needs to concatenate pre-parsed parts.
func compileKcatFormat(fmtStr string) ([]segment, error) {
	var segs []segment
	var lit []byte

	for i := 0; i < len(fmtStr); i++ {
		ch := fmtStr[i]

		switch {
		case ch == '\\' && i+1 < len(fmtStr):
			i++
			switch fmtStr[i] {
			case 'n':
				lit = append(lit, '\n')
			case 'r':
				lit = append(lit, '\r')
			case 't':
				lit = append(lit, '\t')
			case '\\':
				lit = append(lit, '\\')
			case '0':
				lit = append(lit, 0)
			default:
				lit = append(lit, '\\', fmtStr[i])
			}

		case ch == '%' && i+1 < len(fmtStr):
			next := fmtStr[i+1]
			if next == '%' {
				lit = append(lit, '%')
				i++
				continue
			}
			switch next {
			case 't', 'k', 's', 'K', 'S', 'o', 'p', 'T':
				// flush accumulated literal
				if len(lit) > 0 {
					segs = append(segs, segment{literal: lit})
					lit = nil
				}
				segs = append(segs, segment{verb: next})
				i++
			default:
				return nil, fmt.Errorf("unknown kcat format specifier: %%%c", next)
			}

		default:
			lit = append(lit, ch)
		}
	}

	if len(lit) > 0 {
		segs = append(segs, segment{literal: lit})
	}
	return segs, nil
}

func newKcatFormatter(fmtStr string) (*kcatFormatter, error) {
	segs, err := compileKcatFormat(fmtStr)
	if err != nil {
		return nil, err
	}
	return &kcatFormatter{segments: segs}, nil
}

func (f *kcatFormatter) Format(msg *ksink.Message) ([]byte, error) {
	var buf []byte
	for _, seg := range f.segments {
		if seg.verb == 0 {
			buf = append(buf, seg.literal...)
			continue
		}
		switch seg.verb {
		case 't':
			buf = append(buf, msg.Topic...)
		case 'k':
			buf = append(buf, msg.Key...)
		case 's':
			buf = append(buf, msg.Value...)
		case 'K':
			if msg.Key == nil {
				buf = append(buf, "-1"...)
			} else {
				buf = strconv.AppendInt(buf, int64(len(msg.Key)), 10)
			}
		case 'S':
			buf = strconv.AppendInt(buf, int64(len(msg.Value)), 10)
		case 'o':
			buf = strconv.AppendInt(buf, msg.Offset, 10)
		case 'p':
			buf = strconv.AppendInt(buf, int64(msg.Partition), 10)
		case 'T':
			if msg.Timestamp.IsZero() {
				buf = append(buf, "-1"...)
			} else {
				buf = strconv.AppendInt(buf, msg.Timestamp.UnixMilli(), 10)
			}
		}
	}
	return buf, nil
}
