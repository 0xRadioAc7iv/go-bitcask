//go:build unix

package lock

import (
	"fmt"
	"os"
	"syscall"
)

func LockDirectory(path string) (*os.File, error) {
	lockFilePath := path + "/LOCK"

	f, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("unable to open lock file: %w", err)
	}

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("Directory already in use by another bitcask instance")
	}

	return f, nil
}

func UnlockDirectory(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}
