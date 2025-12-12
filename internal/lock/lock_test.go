package lock_test

import (
	"testing"

	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
	"github.com/0xRadioAc7iv/go-bitcask/pkg/bitcask"
)

func TestLockFile(t *testing.T) {
	t.Run("process does not allow access to directory while lock is active", func(t *testing.T) {
		dir := t.TempDir()

		bk := bitcask.Bitcask{DirectoryPath: dir, MaximumDatafileSize: utils.DefaultDataFileSizeMB * bitcask.OneMegabyte}
		bk2 := bitcask.Bitcask{DirectoryPath: dir, MaximumDatafileSize: utils.DefaultDataFileSizeMB * bitcask.OneMegabyte}

		err := bk.Start()
		if err != nil {
			t.Error("Could not start initial Bitcask instance")
		}

		err2 := bk2.Start()
		if err2 == nil {
			t.Error("Bitcask instance was not supposed to start")
		}

		bk.Stop()
	})

	t.Run("process allows access to directory while lock is not active", func(t *testing.T) {
		dir := t.TempDir()

		bk := bitcask.Bitcask{DirectoryPath: dir, MaximumDatafileSize: utils.DefaultDataFileSizeMB * bitcask.OneMegabyte}
		err := bk.Start()
		if err != nil {
			t.Error("Bitcask instance was supposed to start")
		}

		bk.Stop()
	})
}
