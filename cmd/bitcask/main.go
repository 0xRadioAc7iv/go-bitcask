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
	dirPath, maxDataFileSize, port, syncInterval, sizeCheckInterval, garbageRatio,
		minRequiredMergableFiles, minTotalSizeForMerge, maxMergeFiles, maxMergeBytes,
		err := HandleCLIInputs()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	bitcask := core.Bitcask{
		DirectoryPath:            *dirPath,
		MaximumDatafileSize:      *maxDataFileSize,
		ListenerPort:             *port,
		SyncInterval:             *syncInterval,
		SizeCheckInterval:        *sizeCheckInterval,
		GarbageRatio:             *garbageRatio,
		MinRequiredMergableFiles: *minRequiredMergableFiles,
		MinTotalSizeForMerge:     *minTotalSizeForMerge,
		MaxMergeFiles:            *maxMergeFiles,
		MaxMergeBytes:            *maxMergeBytes,
	}

	defer bitcask.Stop()
	if err := bitcask.Start(); err != nil {
		fmt.Println("Error while starting:", err)
		return
	}

	utils.ListenForProcessInterruptOrKill()
}

func HandleCLIInputs() (
	*string, *int64, *int, *uint, *uint,
	*float64, *int, *int64, *int, *int64,
	error,
) {
	directoryPath := flag.String("dir", core.DefaultDirectoryPath, "Directory Path to be used for this instance")
	maxDatafileSizeInMB := flag.Int64("df-size", core.DefaultDataFileSizeMB, "Max Datafile Size (in MB)")
	port := flag.Int("port", internal.DEFAULT_PORT, "Port to use for the TCP Server")
	syncIntervalInSeconds := flag.Uint("sync", core.DefaultSyncInterval, "Interval at each Data is Synced to the Disk")
	sizeCheckIntervalInSeconds := flag.Uint("size-check", core.DefaultSizeCheckInterval, "Interval at each Datafile Size is checked for rotation")
	garbageRatio := flag.Float64("dead-ratio", core.DefaultGarbageRatio, "Ratio of Garbage (Old/Deleted) Data to Total Data Size of each Datafile (Must be between 0.3 and 0.7)")
	minRequiredMergableFiles := flag.Int("merge-files", core.DefaultMinRequiredMergableFiles, "Minimum number of Merge files required for Merge and Compaction to Start")
	minTotalSizeForMerge := flag.Int64("merge-size", core.DefaultMinTotalSizeForMergeMB, "Minimum Total Merge Files Size (Aggregated) for Merge and Compaction to Start (in MB)")
	maxMergeFiles := flag.Int("max-merge-files", core.DefaultMaxMergeFiles, "Maximum number of files that can be merged at a time")
	maxMergeBytes := flag.Int64("max-merge-bytes", core.DefaultMaxMergeBytesGB, "Maximum Size of Data that can be merged at a time (in GB)")

	flag.Parse()

	if *maxDatafileSizeInMB < core.MinimumDataFileSizeMB || *maxDatafileSizeInMB > core.MaximumDataFileSizeMB {
		err := fmt.Sprintf("Max Datafile Size must be between %d and %d MB", core.MinimumDataFileSizeMB, core.MaximumDataFileSizeMB)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *syncIntervalInSeconds < core.MinimumSyncInterval {
		err := fmt.Sprintf("Sync Interval must be >= %d seconds", core.MinimumSyncInterval)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *sizeCheckIntervalInSeconds < core.MinimumSizeCheckInterval {
		err := fmt.Sprintf("Size Check Interval must be >= %d seconds", core.MinimumSizeCheckInterval)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *garbageRatio < core.MinGarbageRatio || *garbageRatio > core.MaxGarbageRatio {
		err := fmt.Sprintf("Size Check Interval must be between %f and %f", core.MinGarbageRatio, core.MaxGarbageRatio)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *minRequiredMergableFiles < core.MinRequiredMergableFiles {
		err := fmt.Sprintf("Min Required Mergable Files must be greater than %d", core.MinRequiredMergableFiles)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *minTotalSizeForMerge < core.MinTotalSizeForMergeMB {
		err := fmt.Sprintf("Min Total Size for Merge must be greater than %d MB", core.MinTotalSizeForMergeMB)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	if *maxMergeFiles < *minRequiredMergableFiles {
		err := fmt.Sprintf("Max Merge Files (%d) must be greater than or equal to Min Required Mergable Files (%d)", *maxMergeFiles, *minRequiredMergableFiles)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	MAX_MERGE_BYTES := *maxMergeBytes * core.OneGigabyte
	MIN_TOTAL_SIZE_FOR_MERGE := *minTotalSizeForMerge * core.OneMegabyte

	if MAX_MERGE_BYTES < MIN_TOTAL_SIZE_FOR_MERGE {
		err := fmt.Sprintf("Max Merge Bytes (%d) must be greater than or equal to Min Total Size for Merge (%d)", *maxMergeBytes, *minTotalSizeForMerge)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, errors.New(err)
	}

	MAX_DATAFILE_SIZE := *maxDatafileSizeInMB * core.OneMegabyte

	return directoryPath,
		&MAX_DATAFILE_SIZE,
		port,
		syncIntervalInSeconds,
		sizeCheckIntervalInSeconds,
		garbageRatio,
		minRequiredMergableFiles,
		&MIN_TOTAL_SIZE_FOR_MERGE,
		maxMergeFiles,
		&MAX_MERGE_BYTES,
		nil
}
