package utils

import "os"

// Truncates a file at a given offset
func TruncateAt(f *os.File, offset int64) error {
	if err := f.Truncate(offset); err != nil {
		return err
	}
	return f.Sync()
}

// Indicates if the given path exists or not (works for both files and directories)
func PathExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return err == nil
}
