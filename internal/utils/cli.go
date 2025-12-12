package utils

import (
	"flag"

	"github.com/0xRadioAc7iv/go-bitcask/pkg/bitcask"
)

const DefaultDirectoryPath = "./"
const DefaultDataFileSizeMB = 64

func HandleCLIInputs() (*string, *int) {
	directoryPath := flag.String("dir", DefaultDirectoryPath, "Directory Path to be used for this instance")
	maxDatafileSizeInMB := flag.Int("dfsize", DefaultDataFileSizeMB, "Max Datafile Size (in MB)")
	flag.Parse()

	MAX_DATAFILE_SIZE := *maxDatafileSizeInMB * bitcask.OneMegabyte
	return directoryPath, &MAX_DATAFILE_SIZE
}
