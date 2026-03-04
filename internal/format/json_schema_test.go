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

// TestJSONOutputMatchesSchema validates that the json formatter output
// conforms to the embedded JSON schema.
func TestJSONOutputMatchesSchema(t *testing.T) {
	schemaLoader := gojsonschema.NewStringLoader(JSONSchema)

	f, err := New("json", []byte("\n"))
	require.NoError(t, err)

	sep := []byte("\n")

	for _, tc := range schemaTestMessages {
		t.Run(tc.name, func(t *testing.T) {
			data, err := f.Format(tc.msg)
			require.NoError(t, err)

			// Strip trailing separator before validation.
			jsonBytes := data[:len(data)-len(sep)]

			// Sanity: must be valid JSON.
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
}

// TestJSONBase64OutputMatchesSchema validates that the json-base64 formatter
// output conforms to the embedded JSON schema.
func TestJSONBase64OutputMatchesSchema(t *testing.T) {
	schemaLoader := gojsonschema.NewStringLoader(JSONSchema)

	f, err := New("json-base64", []byte("\n"))
	require.NoError(t, err)

	sep := []byte("\n")

	for _, tc := range schemaTestMessages {
		t.Run(tc.name, func(t *testing.T) {
			data, err := f.Format(tc.msg)
			require.NoError(t, err)

			// Strip trailing separator before validation.
			jsonBytes := data[:len(data)-len(sep)]

			// Sanity: must be valid JSON.
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
}
