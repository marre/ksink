package format

// JSONSchema is a JSON Schema (draft-07) that validates the output of the
// json formatter. It is embedded as a Go string so that it can be used in
// tests and shipped with the binary without extra files.
const JSONSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ksink JSON message",
  "description": "Schema for one JSON-lines record produced by the ksink json formatter.",
  "type": "object",
  "required": ["topic", "partition", "offset", "value", "value_encoding", "client_addr"],
  "properties": {
    "topic": {
      "type": "string",
      "description": "Kafka topic name."
    },
    "partition": {
      "type": "integer",
      "description": "Partition number."
    },
    "offset": {
      "type": "integer",
      "description": "Message offset within the partition."
    },
    "key": {
      "type": "string",
      "description": "Message key. Encoding depends on the 'key_encoding' field. Omitted when the key is nil."
    },
    "key_encoding": {
      "type": "string",
      "description": "Encoding used for the key field when it is present.",
      "enum": ["utf-8", "base64"]
    },
    "value": {
      "type": "string",
      "description": "Message value. Encoding depends on the 'value_encoding' field."
    },
    "value_encoding": {
      "type": "string",
      "description": "Encoding used for the value field.",
      "enum": ["utf-8", "base64"]
    },
    "headers": {
      "type": "object",
      "description": "Optional message headers.",
      "additionalProperties": {
        "type": "string"
      }
    },
    "timestamp": {
      "type": "string",
      "description": "Message timestamp in Go time.Time.String() format (e.g. '2025-01-01 00:00:00 +0000 UTC'). Omitted when not set.",
      "pattern": "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"
    },
    "client_addr": {
      "type": "string",
      "description": "Address of the producing client."
    }
  },
  "additionalProperties": false
}`
