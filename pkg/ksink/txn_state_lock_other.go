//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package ksink

import (
	"errors"
	"os"
)

func lockTxnStateFile(_ *os.File) error {
	return errors.New("durable transaction state is not supported on this operating system")
}

func unlockTxnStateFile(_ *os.File) error {
	return nil
}
