//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package output

import (
	"fmt"
	"os"
)

func acquireTxnFileLock(_ *os.File) error {
	return fmt.Errorf("transaction recovery locking is unsupported on this operating system")
}

func releaseTxnFileLock(_ *os.File) error {
	return nil
}
