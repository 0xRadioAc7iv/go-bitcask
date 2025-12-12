//go:build windows

package lock

import (
	"fmt"
	"os"
)

func LockDirectory(path string) (*os.File, error) {
	lockFilePath := path + "/LOCK"

	f, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Directory already in use by another bitcask instance")
	}

	return f, nil
}

func UnlockDirectory(f *os.File) {
	name := f.Name()
	f.Close()
	os.Remove(name)
}
