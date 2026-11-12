//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package output

import (
	"os"

	"golang.org/x/sys/unix"
)

func acquireTxnFileLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func releaseTxnFileLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
