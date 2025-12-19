package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/0xRadioAc7iv/go-bitcask/core"
	"github.com/0xRadioAc7iv/go-bitcask/internal"
	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
)

func main() {
	dirPath, maxDataFileSize, port, syncInterval, sizeCheckInterval, err := HandleCLIInputs()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	bitcask := core.Bitcask{
		DirectoryPath:       *dirPath,
		MaximumDatafileSize: *maxDataFileSize,
		ListenerPort:        *port,
		SyncInterval:        *syncInterval,
		SizeCheckInterval:   *sizeCheckInterval,
	}

	defer bitcask.Stop()
	if err := bitcask.Start(); err != nil {
		fmt.Println("Error while starting:", err)
	}

	utils.ListenForProcessInterruptOrKill()
}

func HandleCLIInputs() (*string, *int, *int, *uint, *uint, error) {
	directoryPath := flag.String("dir", core.DefaultDirectoryPath, "Directory Path to be used for this instance")
	maxDatafileSizeInMB := flag.Int("dfsize", core.DefaultDataFileSizeMB, "Max Datafile Size (in MB)")
	port := flag.Int("port", internal.DEFAULT_PORT, "Port to use for the TCP Server")
	syncIntervalInSeconds := flag.Uint("sync", core.DefaultSyncInterval, "Interval at each Data is Synced to the Disk")
	sizeCheckIntervalInSeconds := flag.Uint("sizecheck", core.DefaultSizeCheckInterval, "Interval at each Datafile Size is checked for rotation")
	flag.Parse()

	if *maxDatafileSizeInMB < core.MinimumDataFileSizeMB {
		err := fmt.Sprintf("Max Datafile Size must be >= %dMB", core.MinimumDataFileSizeMB)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *maxDatafileSizeInMB > core.MaximumDataFileSizeMB {
		err := fmt.Sprintf("Max Datafile Size must be <= %dMB", core.MaximumDataFileSizeMB)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *syncIntervalInSeconds < core.MinimumSyncInterval {
		err := fmt.Sprintf("Sync Interval must be >= %d seconds", core.MinimumSyncInterval)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *sizeCheckIntervalInSeconds < core.MinimumSizeCheckInterval {
		err := fmt.Sprintf("Size Check Interval must be >= %d seconds", core.MinimumSizeCheckInterval)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	MAX_DATAFILE_SIZE := *maxDatafileSizeInMB * core.OneMegabyte
	return directoryPath, &MAX_DATAFILE_SIZE, port, syncIntervalInSeconds, sizeCheckIntervalInSeconds, nil
}
