package ksrv

import "errors"

// ErrNotConnected is returned by ReadBatch when the server has not been started
// (Connect has not been called) or after the server has shut down.
var ErrNotConnected = errors.New("kafka server not connected")

// ErrEndOfInput is returned by ReadBatch when the server has been closed and
// no more messages will be produced.
var ErrEndOfInput = errors.New("kafka server end of input")
