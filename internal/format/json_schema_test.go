package format

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/marre/ksink/pkg/ksink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"
)

var schemaTestMessages = []struct {
	name string
	msg  *ksink.Message
}{
	{
		name: "full_message",
		msg:  sampleMessage(),
	},
	{
		name: "nil_key",
		msg: &ksink.Message{
			Topic:      "t",
			Partition:  1,
			Offset:     10,
			Value:      []byte("v"),
			ClientAddr: "127.0.0.1:1",
		},
	},
	{
		name: "zero_timestamp",
		msg: &ksink.Message{
			Topic:      "t",
			Partition:  0,
			Offset:     0,
			Key:        []byte("k"),
			Value:      []byte("v"),
			Timestamp:  time.Time{},
			ClientAddr: "127.0.0.1:2",
		},
	},
	{
		name: "with_headers",
		msg: &ksink.Message{
			Topic:      "t",
			Partition:  2,
			Offset:     99,
			Value:      []byte("v"),
			Headers:    map[string]string{"a": "1", "b": "2"},
			ClientAddr: "127.0.0.1:3",
		},
	},
	{
		name: "empty_key",
		msg: &ksink.Message{
			Topic:      "t",
			Partition:  0,
			Offset:     0,
			Key:        []byte{},
			Value:      []byte("v"),
			ClientAddr: "127.0.0.1:4",
		},
	},
}

func TestJSONOutputMatchesSchema(t *testing.T) {
	schemaLoader := gojsonschema.NewStringLoader(JSONSchema)

	sep := []byte("\n")

	testCases := []struct {
		name    string
		options []Option
	}{
		{name: "plain_json"},
		{name: "base64_key", options: []Option{WithJSONBase64Key()}},
		{name: "base64_value", options: []Option{WithJSONBase64Value()}},
		{name: "base64_key_and_value", options: []Option{WithJSONBase64Key(), WithJSONBase64Value()}},
	}

	for _, formatCase := range testCases {
		t.Run(formatCase.name, func(t *testing.T) {
			f, err := New("json", sep, formatCase.options...)
			require.NoError(t, err)

			for _, tc := range schemaTestMessages {
				t.Run(tc.name, func(t *testing.T) {
					data, err := f.Format(tc.msg)
					require.NoError(t, err)

					jsonBytes := data[:len(data)-len(sep)]
					require.True(t, json.Valid(jsonBytes), "output is not valid JSON")

					docLoader := gojsonschema.NewBytesLoader(jsonBytes)
					result, err := gojsonschema.Validate(schemaLoader, docLoader)
					require.NoError(t, err)

					for _, e := range result.Errors() {
						t.Errorf("schema validation error: %s", e)
					}
					assert.True(t, result.Valid(), "JSON output does not match schema")
				})
			}
		})
	}
}
