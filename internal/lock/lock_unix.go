//go:build unix

package lock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockDirectory attempts to acquire an exclusive, non-blocking advisory lock
// on the given directory using a lock file.
//
// On Unix systems, this uses flock(2) to place an exclusive lock on a file
// named "LOCK" inside the directory. If the lock cannot be acquired, the
// directory is assumed to be in use by another Bitcask instance.
//
// The returned file handle must remain open for the duration of the lock.
func LockDirectory(path string) (*os.File, error) {
	lockFilePath := filepath.Join(path, "LOCK")

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

// UnlockDirectory releases a directory lock acquired via LockDirectory.
//
// On Unix systems, this releases the advisory flock and closes the file.
func UnlockDirectory(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
}
