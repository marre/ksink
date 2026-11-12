//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package ksink

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockTxnStateFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}

func unlockTxnStateFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
