package core

const (
	// OneMegabyte represents one mebibyte (1 MiB).
	OneMegabyte = 1024 * 1024

	// OneGigabyte represents one gibibyte (1 GiB).
	OneGigabyte = 1024 * OneMegabyte

	// MaxKeyOrValueSize is the maximum allowed size for a key or value.
	// This acts as a safety bound to prevent excessive memory usage.
	MaxKeyOrValueSize = 32 * OneMegabyte

	// DataDirName is the name of the directory where datafiles are stored.
	DataDirName = "data"

	// DefaultDirectoryPath is the default base path for Bitcask storage.
	DefaultDirectoryPath = "./"

	// DataFilePrefix is the prefix used for datafile names.
	DataFilePrefix = "bk_"

	// MergedFilePrefix is the prefix used for temporary merged datafiles
	// created during compaction.
	MergedFilePrefix = "merged_"

	// DataFileExt is the file extension for datafiles.
	DataFileExt = ".data"

	// HintFileExt is the file extension for hint files.
	HintFileExt = ".hint"

	// DatafileZeroName is the name of the initial datafile.
	DatafileZeroName = "bk_0.data"

	// DefaultDataFileSizeMB is the default maximum size of a datafile in megabytes.
	DefaultDataFileSizeMB = 64

	// MinimumDataFileSizeMB is the minimum allowed datafile size in megabytes.
	MinimumDataFileSizeMB = 64

	// MaximumDataFileSizeMB is the maximum allowed datafile size in megabytes.
	MaximumDataFileSizeMB = 256

	// DefaultSyncInterval is the default interval (in seconds) for fsync operations.
	DefaultSyncInterval = 15

	// MinimumSyncInterval is the minimum allowed sync interval in seconds.
	MinimumSyncInterval = 5

	// DefaultSizeCheckInterval is the default interval (in seconds) for checking
	// active datafile size and triggering rotation.
	DefaultSizeCheckInterval = 5

	// MinimumSizeCheckInterval is the minimum allowed size check interval in seconds.
	MinimumSizeCheckInterval = 5

	// DefaultGarbageRatio is the default minimum garbage ratio required
	// for a datafile to be eligible for compaction.
	DefaultGarbageRatio = 0.4

	// MinGarbageRatio is the lower bound for garbage ratio configuration.
	MinGarbageRatio = 0.33

	// MaxGarbageRatio is the upper bound for garbage ratio configuration.
	MaxGarbageRatio = 0.75

	// DefaultMinRequiredMergableFiles is the default minimum number of
	// immutable files required to trigger a merge.
	DefaultMinRequiredMergableFiles = 3

	// MinRequiredMergableFiles is the minimum allowed value for merge eligibility.
	MinRequiredMergableFiles = 2

	// DefaultMinTotalSizeForMergeMB is the default minimum total size (in MB)
	// of immutable datafiles required to trigger a merge.
	DefaultMinTotalSizeForMergeMB = 256

	// MinTotalSizeForMergeMB is the minimum allowed merge size threshold (in MB).
	MinTotalSizeForMergeMB = 128

	// DefaultMaxMergeFiles is the default maximum number of files that may
	// participate in a single merge operation.
	DefaultMaxMergeFiles = 5

	// DefaultMaxMergeBytesGB is the default maximum number of bytes (in GB)
	// that may be processed in a single merge operation.
	DefaultMaxMergeBytesGB = 2
)
