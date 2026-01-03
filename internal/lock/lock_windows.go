//go:build windows

package lock

import (
	"fmt"
	"os"
	"path/filepath"
)

// LockDirectory attempts to acquire an exclusive lock on the given directory
// using a lock file.
//
// On Windows, this is implemented by atomically creating a file named "LOCK"
// inside the directory. If the file already exists, the directory is assumed
// to be in use by another Bitcask instance.
//
// The returned file handle must be kept open for the duration of the lock.
func LockDirectory(path string) (*os.File, error) {
	lockFilePath := filepath.Join(path, "LOCK")

	f, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Directory already in use by another bitcask instance")
	}

	return f, nil
}

// UnlockDirectory releases a directory lock acquired via LockDirectory.
//
// On Windows, this removes the lock file from disk. UnlockDirectory should
// be called exactly once for each successful LockDirectory call.
func UnlockDirectory(f *os.File) {
	name := f.Name()
	f.Close()
	os.Remove(name)
}
