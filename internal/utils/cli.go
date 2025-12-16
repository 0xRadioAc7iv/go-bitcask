package utils

import (
	"errors"
	"flag"
	"fmt"

	"github.com/0xRadioAc7iv/go-bitcask/pkg/bitcask"
)

const MinimumDataFileSizeMB = 64
const MaximumDataFileSizeMB = 256
const MinimumSyncInterval = 5
const MinimumSizeCheckInterval = 5

const DefaultDirectoryPath = "./"
const DefaultDataFileSizeMB = 64
const DefaultPort = 6969
const DefaultSyncInterval = 15
const DefaultSizeCheckInterval = 30

func HandleCLIInputs() (*string, *int, *int, *uint, *uint, error) {
	directoryPath := flag.String("dir", DefaultDirectoryPath, "Directory Path to be used for this instance")
	maxDatafileSizeInMB := flag.Int("dfsize", DefaultDataFileSizeMB, "Max Datafile Size (in MB)")
	port := flag.Int("port", DefaultPort, "Port to use for the TCP Server")
	syncIntervalInSeconds := flag.Uint("sync", DefaultSyncInterval, "Interval at each Data is Synced to the Disk")
	sizeCheckIntervalInSeconds := flag.Uint("sizecheck", DefaultSizeCheckInterval, "Interval at each Datafile Size is checked for rotation")
	flag.Parse()

	if *maxDatafileSizeInMB < MinimumDataFileSizeMB {
		err := fmt.Sprintf("Max Datafile Size must be >= %dMB", MinimumDataFileSizeMB)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *maxDatafileSizeInMB > MaximumDataFileSizeMB {
		err := fmt.Sprintf("Max Datafile Size must be <= %dMB", MaximumDataFileSizeMB)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *syncIntervalInSeconds < MinimumSyncInterval {
		err := fmt.Sprintf("Sync Interval must be >= %d seconds", MinimumSyncInterval)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	if *sizeCheckIntervalInSeconds < MinimumSizeCheckInterval {
		err := fmt.Sprintf("Size Check Interval must be >= %d seconds", MinimumSizeCheckInterval)
		return nil, nil, nil, nil, nil, errors.New(err)
	}

	MAX_DATAFILE_SIZE := *maxDatafileSizeInMB * bitcask.OneMegabyte
	return directoryPath, &MAX_DATAFILE_SIZE, port, syncIntervalInSeconds, sizeCheckIntervalInSeconds, nil
}
