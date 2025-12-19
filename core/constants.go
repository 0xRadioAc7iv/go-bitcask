package core

const (
	OneMegabyte              = 1024 * 1024 // 1024 (1KB) * 1024 => 1MB
	DataDirName              = "data"      // Name of the Datafile Directory
	FileSuffix               = "bk_"
	DataFileExt              = ".data"
	HintFileExt              = ".hint"
	MinimumDataFileSizeMB    = 64
	MaximumDataFileSizeMB    = 256
	MinimumSyncInterval      = 5
	MinimumSizeCheckInterval = 5
	DefaultDirectoryPath     = "./"
	DefaultDataFileSizeMB    = 64
	DefaultSyncInterval      = 15
	DefaultSizeCheckInterval = 30
	ZERO_NAME                = "000"
)
