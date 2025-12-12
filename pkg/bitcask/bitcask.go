package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/0xRadioAc7iv/go-bitcask/internal/lock"
)

type Bitcask struct {
	lockFile            *os.File
	activeDataFile      *os.File
	DirectoryPath       string
	MaximumDatafileSize int
}

func (bk *Bitcask) Start() error {
	lf, err := lock.LockDirectory(bk.DirectoryPath)
	if err != nil {
		return err
	}
	bk.lockFile = lf

	err = bk.openDataDirectory()
	if err != nil {
		fmt.Println("Error Opening Bitcask Datafiles Directory:", err)
	}

	files, err := bk.scanDatafiles()
	if err != nil {
		fmt.Println("Error Scanning for Datafiles:", err)
	}

	f, err := bk.createNewActiveDatafile(files)
	if err != nil {
		return err
	}
	bk.activeDataFile = f

	return nil
}

func (bk *Bitcask) openDataDirectory() error {
	fullDatafileDirectoryPath := bk.DirectoryPath + DataDirName

	_, err := os.Stat(fullDatafileDirectoryPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Datafile Directory does not exist! Creating one...")

			// 0 (special bit - ignored), 7 (rwx - owner), 5 (r-x - user group), 5 (r-x - others)
			err := os.Mkdir(fullDatafileDirectoryPath, 0755)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		fmt.Println("Datafile Directory already exists. Skipping creation...")
	}

	return nil
}

func (bk *Bitcask) scanDatafiles() ([]string, error) {
	// Looks for '.data' files inside the datafile directory
	files, err := os.ReadDir(bk.DirectoryPath + DataDirName)
	if err != nil {
		return nil, err
	}

	datafiles := []string{}

	for _, entry := range files {
		if !entry.IsDir() {
			if filepath.Ext(entry.Name()) == DataFileExt {
				datafiles = append(datafiles, entry.Name())
			}
		}
	}

	return datafiles, nil
}

func (bk *Bitcask) createNewActiveDatafile(datafiles []string) (*os.File, error) {
	datafileDirPathSuffix := bk.DirectoryPath + DataDirName + "/"
	if len(datafiles) == 0 {
		return os.OpenFile(datafileDirPathSuffix+DataFileSuffix+"0"+DataFileExt, os.O_CREATE|os.O_RDWR, 0644)
	}

	lastFile := datafiles[len(datafiles)-1]

	// filename like: "data_002.data"
	// split: ["data_002", "data"]
	base := strings.Split(lastFile, ".")[0]
	// split: ["data", "002"]
	numberStr := strings.Split(base, "_")[1]
	// Convert: "2" -> 2
	number, err := strconv.Atoi(numberStr)
	if err != nil {
		return nil, err
	}

	nextNumber := number + 1
	nextFilename := fmt.Sprintf("%s%s%d%s", datafileDirPathSuffix, DataFileSuffix, nextNumber, DataFileExt)

	return os.OpenFile(nextFilename, os.O_CREATE|os.O_RDWR, 0644)
}

func (bk *Bitcask) Stop() {
	err := bk.activeDataFile.Close()
	if err != nil {
		fmt.Println("Error while closing the Active Datafile:", err)
	}

	lock.UnlockDirectory(bk.lockFile)
}
