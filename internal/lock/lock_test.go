package lock_test

import (
	"testing"

	"github.com/0xRadioAc7iv/go-bitcask/core"
	"github.com/0xRadioAc7iv/go-bitcask/internal"
	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
)

func TestLockFile(t *testing.T) {
	t.Run("process does not allow access to directory while lock is active", func(t *testing.T) {
		dir := t.TempDir()

		bk := core.Bitcask{
			DirectoryPath:       dir,
			MaximumDatafileSize: utils.DefaultDataFileSizeMB * core.OneMegabyte,
			ListenerPort:        internal.DEFAULT_PORT,
			SyncInterval:        utils.DefaultSyncInterval,
			SizeCheckInterval:   utils.DefaultSizeCheckInterval,
		}
		bk2 := core.Bitcask{
			DirectoryPath:       dir,
			MaximumDatafileSize: utils.DefaultDataFileSizeMB * core.OneMegabyte,
			ListenerPort:        internal.DEFAULT_PORT,
			SyncInterval:        utils.DefaultSyncInterval,
			SizeCheckInterval:   utils.DefaultSizeCheckInterval,
		}

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

		bk := core.Bitcask{
			DirectoryPath:       dir,
			MaximumDatafileSize: utils.DefaultDataFileSizeMB * core.OneMegabyte,
			ListenerPort:        internal.DEFAULT_PORT,
			SyncInterval:        utils.DefaultSyncInterval,
			SizeCheckInterval:   utils.DefaultSizeCheckInterval,
		}
		err := bk.Start()
		if err != nil {
			t.Error("Bitcask instance was supposed to start")
		}

		bk.Stop()
	})
}
