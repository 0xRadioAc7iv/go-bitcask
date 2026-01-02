package core

const (
	OneMegabyte = 1024 * 1024 // 1024 (1KB) * 1024 => 1MB
	OneGigabyte = 1024 * OneMegabyte

	DatafileZeroName      = "bk_0.data"
	DataDirName           = "data" // Name of the Datafile Directory
	DefaultDirectoryPath  = "./"
	DataFileSuffix        = "bk_"
	MergedFileSuffix      = "merged_"
	DataFileExt           = ".data"
	HintFileExt           = ".hint"
	DefaultDataFileSizeMB = 64
	MinimumDataFileSizeMB = 64
	MaximumDataFileSizeMB = 256

	DefaultSyncInterval = 15
	MinimumSyncInterval = 5

	DefaultSizeCheckInterval = 5
	MinimumSizeCheckInterval = 5

	DefaultGarbageRatio = 0.4
	MinGarbageRatio     = 0.33
	MaxGarbageRatio     = 0.75

	DefaultMinRequiredMergableFiles = 3
	MinRequiredMergableFiles        = 2

	DefaultMinTotalSizeForMergeMB = 256
	MinTotalSizeForMergeMB        = 128

	DefaultMaxMergeFiles   = 5
	DefaultMaxMergeBytesGB = 2
)
