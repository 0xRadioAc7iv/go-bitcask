package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xRadioAc7iv/go-bitcask/internal/lock"
	"github.com/0xRadioAc7iv/go-bitcask/internal/protocol"
	"github.com/0xRadioAc7iv/go-bitcask/internal/record"
	"github.com/0xRadioAc7iv/go-bitcask/internal/server"
	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
)

type Bitcask struct {
	lockFile                 *os.File
	activeDataFile           *os.File
	activeOffset             int64
	serverCancel             context.CancelFunc
	syncCancel               context.CancelFunc
	sizeCheckCancel          context.CancelFunc
	mergeAndCompactionCancel context.CancelFunc
	keyDir                   KeyDir
	dataMu                   sync.Mutex   // for activeDataFile & activeOffset
	keyDirMu                 sync.RWMutex // for keyDir
	mergeMu                  sync.Mutex   // for Merge and Compaction

	DirectoryPath            string
	MaximumDatafileSize      int64
	ListenerPort             int
	SyncInterval             uint
	SizeCheckInterval        uint
	GarbageRatio             float64
	MinRequiredMergableFiles int
	MinTotalSizeForMerge     int64
	MaxMergeFiles            int
	MaxMergeBytes            int64
}

type candidate struct {
	path         string
	garbageRatio float64
	size         int64
}

// Start initializes the Bitcask instance and begins serving requests.
//
// It performs the following steps:
//   - Acquires an exclusive lock on the data directory
//   - Creates the data directory if it does not exist
//   - Rebuilds the KeyDir from hint files and/or datafiles
//   - Opens or creates the active datafile
//   - Starts the TCP server
//   - Launches background goroutines for syncing, rotation, and compaction
//
// Start returns an error if initialization fails. On success, the instance
// runs until Stop is called.
func (bk *Bitcask) Start() error {
	var latestFileName string

	lf, err := lock.LockDirectory(bk.DirectoryPath)
	if err != nil {
		fmt.Println("Error Locking Bitcask Datafiles Directory")
		return err
	}
	bk.lockFile = lf
	fmt.Println("[STARTUP] Datafile Directory Locked ✅")

	err = bk.openDataDirectory()
	if err != nil {
		fmt.Println("Error Opening Bitcask Datafiles Directory")
		return err
	}

	err = bk.cleanupExistingMergeFiles()
	if err != nil {
		return err
	}

	filenames, err := bk.scanForDatafileNames()
	if err != nil {
		fmt.Println("Error Scanning for Datafiles")
		return err
	}

	bk.keyDir = make(KeyDir)

	err = bk.loadDataInKeyDir(filenames)
	if err != nil {
		fmt.Println("Error loading data from files:", err)
		return err
	}

	if len(filenames) == 0 {
		latestFileName = DatafileZeroName
	} else {
		latestFileName = filenames[len(filenames)-1] + DataFileExt
	}

	f, err := bk.createNewActiveDatafile(latestFileName)
	if err != nil {
		fmt.Println("Error creating new active datafile")
		return err
	}

	// Sets the offset to the end of the active datafile
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	bk.activeOffset = offset
	bk.activeDataFile = f

	ctx, cancel := context.WithCancel(context.Background())
	bk.serverCancel = cancel
	go func() {
		if err := server.Start(ctx, bk.ListenerPort, bk.commandHandler); err != nil {
			log.Fatal("Server stopped abruptly", err)
		}
	}()
	fmt.Println("[STARTUP] TCP Server Started ✅")

	syncCtx, syncCancel := context.WithCancel(context.Background())
	bk.syncCancel = syncCancel
	go bk.handleSyncDiskInterval(syncCtx, bk.SyncInterval)
	fmt.Println("[STARTUP] Disk Sync Interval ✅")

	sizeCheckCtx, sizeCheckCancel := context.WithCancel(context.Background())
	bk.sizeCheckCancel = sizeCheckCancel
	go bk.handleActiveDatafileSizeCheckInterval(sizeCheckCtx, bk.SizeCheckInterval)
	fmt.Println("[STARTUP] Datafile Size Check Interval ✅")

	mergeAndCompactionCtx, mergeAndCompactionCancel := context.WithCancel(context.Background())
	bk.mergeAndCompactionCancel = mergeAndCompactionCancel
	go bk.handleDatafilesMergeAndCompactionInterval(mergeAndCompactionCtx)
	fmt.Println("[STARTUP] Datafiles Merge and Compaction Interval ✅")

	fmt.Println("Bitcask Started ✅")
	fmt.Printf("Server listening on http://localhost:%d...\n", bk.ListenerPort)
	return nil
}

func (bk *Bitcask) openDataDirectory() error {
	fullDatafileDirectoryPath := bk.getDatafileDirectoryPathSuffix()

	if utils.PathExists(fullDatafileDirectoryPath) {
		fmt.Println("[STARTUP] Datafile Directory already exists. Skipping creation...")
		return nil
	}

	fmt.Println("[STARTUP] Datafile Directory does not exist! Creating one...")
	// 0 (special bit - ignored), 7 (rwx - owner), 5 (r-x - user group), 5 (r-x - others)
	err := os.Mkdir(fullDatafileDirectoryPath, 0755)
	if err != nil {
		return err
	}
	return nil
}

func (bk *Bitcask) cleanupExistingMergeFiles() error {
	files, err := os.ReadDir(bk.getDatafileDirectoryPathSuffix())
	if err != nil {
		return err
	}

	for _, entry := range files {
		if !entry.IsDir() {
			isMergedFile := strings.HasPrefix(entry.Name(), MergedFilePrefix)
			if isMergedFile {
				err := os.Remove(bk.getDatafileDirectoryPathSuffix() + entry.Name())
				if err != nil {
					fmt.Printf("Error deleting file %s\n", entry.Name())
					return err
				}
			}
		}
	}

	return nil
}

// Looks for '.data' files inside the datafile directory but returns their names
func (bk *Bitcask) scanForDatafileNames() ([]string, error) {
	files, err := os.ReadDir(bk.getDatafileDirectoryPathSuffix())
	if err != nil {
		return nil, err
	}

	datafiles := []string{}

	for _, entry := range files {
		if !entry.IsDir() {
			if filepath.Ext(entry.Name()) == DataFileExt {
				// Splits and returns the name like => (bk_25.data) -> bk_25
				datafiles = append(datafiles, strings.Split(entry.Name(), ".")[0])
			}
		}
	}

	return datafiles, nil
}

func (bk *Bitcask) loadDataInKeyDir(datafileNames []string) error {
	if len(datafileNames) == 0 {
		return nil
	}

	filePathPrefix := bk.getDatafileDirectoryPathSuffix()

	for _, filename := range datafileNames {
		hintFilePath := filePathPrefix + filename + HintFileExt
		if utils.PathExists(hintFilePath) {
			fmt.Printf("[STARTUP] Reading Hint file: %s\n", hintFilePath)
			err := bk.readHintfile(hintFilePath)
			if err == nil {
				continue
			} else {
				fmt.Printf("Error reading hint file %s : %v\n", hintFilePath, err)
			}
		}

		dataFilePath := filePathPrefix + filename + DataFileExt
		fmt.Printf("[STARTUP] Reading Datafile: %s\n", dataFilePath)
		err := bk.readDatafile(dataFilePath)
		if err != nil {
			return err
		}
	}
	fmt.Println("[STARTUP] Data loaded in KeyDir ✅")

	return nil
}

func (bk *Bitcask) readHintfile(filepath string) error {
	f, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		hintRecordHeader := make([]byte, record.HintRecordHeaderSizeBytes)
		_, err = io.ReadFull(f, hintRecordHeader)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		buf := bytes.NewReader(hintRecordHeader)

		var keySize uint32
		var fileNameSize uint32
		var offset uint32
		var valueSize uint32
		var timestamp int64

		err := binary.Read(buf, binary.LittleEndian, &keySize)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &fileNameSize)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &offset)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &valueSize)
		if err != nil {
			return err
		}
		err = binary.Read(buf, binary.LittleEndian, &timestamp)
		if err != nil {
			return err
		}

		payload := make([]byte, keySize+fileNameSize)
		if _, err := io.ReadFull(f, payload); err != nil {
			return err
		}

		fullRecord := append(hintRecordHeader, payload...)
		hintRecord, err := record.DecodeHintRecordFromBytes(fullRecord)
		if err != nil {
			return err
		}

		key := string(hintRecord.Key)

		bk.keyDir[key] = KeyDirEntry{
			FileName:   string(hintRecord.FileName),
			Offset:     hintRecord.Offset,
			ValueSize:  hintRecord.ValueSize,
			RecordSize: record.DiskRecordHeaderSizeBytes + hintRecord.KeySize + hintRecord.ValueSize,
			Timestamp:  hintRecord.Timestamp,
		}
	}
}

func (bk *Bitcask) readDatafile(filepath string) error {
	f, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Error opening the file %s\n: %v", filepath, err)
		return err
	}
	defer f.Close()

	var offset int64 = 0

	for {
		recordStartOffset := offset

		recordHeader := make([]byte, record.DiskRecordHeaderSizeBytes)
		_, err = io.ReadFull(f, recordHeader)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return utils.TruncateAt(f, recordStartOffset)
			}
			return err
		}

		var crc uint32
		var timestamp int64
		var keySize uint32
		var valueSize uint32

		buf := bytes.NewReader(recordHeader)
		if err := binary.Read(buf, binary.LittleEndian, &crc); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}
		if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}
		if err := binary.Read(buf, binary.LittleEndian, &keySize); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}
		if err := binary.Read(buf, binary.LittleEndian, &valueSize); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}

		key := make([]byte, keySize)
		if _, err := io.ReadFull(f, key); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}

		keyString := string(key)

		if valueSize == 0 {
			delete(bk.keyDir, keyString)
			offset += int64(record.DiskRecordHeaderSizeBytes + keySize)
			continue
		}

		value := make([]byte, valueSize)
		if _, err := io.ReadFull(f, value); err != nil {
			return utils.TruncateAt(f, recordStartOffset)
		}

		if !record.ValidateCRC(key, value, crc) {
			return utils.TruncateAt(f, recordStartOffset)
		}

		entry, ok := bk.keyDir[keyString]

		// Sets the value, if the key does not exist in the KeyDir OR
		// the timestamp is greater than the one existing in the KeyDir
		if !ok || timestamp > entry.Timestamp {
			bk.keyDir[string(key)] = KeyDirEntry{
				FileName:   f.Name(),
				Offset:     uint32(recordStartOffset),
				ValueSize:  valueSize,
				RecordSize: record.DiskRecordHeaderSizeBytes + keySize + valueSize,
				Timestamp:  timestamp,
			}
		}

		offset += int64(record.DiskRecordHeaderSizeBytes + keySize + valueSize)
	}
}

func (bk *Bitcask) createNewActiveDatafile(latestFileName string) (*os.File, error) {
	var newFileNumber int
	datafileDirPathSuffix := bk.getDatafileDirectoryPathSuffix()

	base := strings.Split(latestFileName, ".")[0]
	numberStr := strings.Split(base, "_")[1]
	number, err := strconv.Atoi(numberStr)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(bk.getDatafileDirectoryPathSuffix()+latestFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Checks the size of the previous latest file, if it's size is less than
	// the allowed maximum, then returns it, saving disk space and prevents
	// from creating too many datafiles
	overTheAllowedMaxSize, err := bk.isFileSizeOverTheAllowedMaximum(f)
	if err != nil {
		return nil, err
	}

	if !overTheAllowedMaxSize {
		return f, nil
	}

	newFileNumber = number + 1
	newFileName := fmt.Sprintf("%s%s%d%s", datafileDirPathSuffix, DataFilePrefix, newFileNumber, DataFileExt)
	return os.OpenFile(newFileName, os.O_CREATE|os.O_RDWR, 0644)
}

func (bk *Bitcask) rotateActiveDatafile() error {
	bk.dataMu.Lock()
	defer bk.dataMu.Unlock()

	latestFileNameWithPath := bk.activeDataFile.Name()
	latestFileName := filepath.Base(latestFileNameWithPath)

	err := bk.activeDataFile.Sync()
	if err != nil {
		fmt.Println("There was an error while syncing the active datafile on rotation")
		return err
	}
	err = bk.activeDataFile.Close()
	if err != nil {
		fmt.Println("There was an error while closing the active datafile on rotation")
		return err
	}

	err = bk.generateHintFileForDatafile(latestFileNameWithPath)
	if err != nil {
		return err
	}

	f, err := bk.createNewActiveDatafile(latestFileName)
	if err != nil {
		return err
	}

	bk.activeDataFile = f
	bk.activeOffset = 0

	return nil
}

func (bk *Bitcask) commandHandler(conn net.Conn) {
	defer conn.Close()

	for {
		command, err := protocol.DecodeCommand(conn)
		if err != nil {
			return
		}

		if len(command.Key) > MaxKeyOrValueSize || len(command.Val) > MaxKeyOrValueSize {
			err := fmt.Sprintf("Error: Maximum Size of Key or Value must not exceed %d bytes\n", MaxKeyOrValueSize)
			bk.reply(conn, err)
			return
		}

		bk.handleCommand(command, conn)
	}
}

func (bk *Bitcask) handleCommand(command *protocol.Command, conn net.Conn) {
	cmd := strings.ToLower(command.Cmd)

	switch cmd {
	case "ping":
		bk.handleCommandPing(conn)
	case "set":
		bk.handleCommandSET(conn, command.Key, command.Val)
	case "get":
		bk.handleCommandGET(conn, command.Key)
	case "delete":
		bk.handleCommandDelete(conn, command.Key)
	case "exists":
		bk.handleCommandExists(conn, command.Key)
	case "count":
		bk.handleCommandCount(conn)
	case "list":
		bk.handleCommandList(conn)
	case "help":
		bk.handleCommandHelp(conn)
	default:
		bk.handleInvalidCommand(conn)
	}
}

func (bk *Bitcask) handleCommandPing(conn net.Conn) {
	bk.reply(conn, "PONG!")
}

func (bk *Bitcask) handleCommandGET(conn net.Conn, key string) {
	bk.keyDirMu.RLock()
	keyDirEntry, ok := bk.keyDir[key]
	bk.keyDirMu.RUnlock()

	if !ok {
		bk.reply(conn, "nil")
		return
	}

	value, err := bk.readValueFromFile(keyDirEntry.FileName, keyDirEntry.Offset, keyDirEntry.RecordSize)
	if err != nil {
		bk.reply(conn, "Error while reading value")
	}

	bk.reply(conn, value)
}

func (bk *Bitcask) handleCommandSET(conn net.Conn, key, value string) {
	diskRecord := record.CreateRecord(key, value)
	encoded, err := record.EncodeRecordToBytes(&diskRecord)
	if err != nil {
		bk.reply(conn, "Error while setting value")
		fmt.Println(err)
		return
	}

	offset, filename, err := bk.writeToActiveFile(encoded)
	if err != nil {
		bk.reply(conn, "Error while setting value")
		fmt.Println(err)
		return
	}
	bk.setDataKeyDir(filename, key, offset, diskRecord.KeySize, diskRecord.ValueSize)

	bk.reply(conn, "ok")
}

func (bk *Bitcask) handleCommandDelete(conn net.Conn, key string) {
	tombstoneRecord := record.CreateTombstoneRecord(key)
	encoded, err := record.EncodeRecordToBytes(&tombstoneRecord)
	if err != nil {
		bk.reply(conn, "Error while deleting value")
		return
	}

	_, _, err = bk.writeToActiveFile(encoded)
	if err != nil {
		bk.reply(conn, "Error while deleting value")
		return
	}
	bk.deleteDataKeyDir(key)

	bk.reply(conn, "ok")
}

func (bk *Bitcask) handleCommandExists(conn net.Conn, key string) {
	bk.keyDirMu.RLock()
	_, ok := bk.keyDir[key]
	bk.keyDirMu.RUnlock()

	if !ok {
		bk.reply(conn, "false")
		return
	}

	bk.reply(conn, "true")
}

func (bk *Bitcask) handleCommandCount(conn net.Conn) {
	bk.keyDirMu.RLock()
	count := len(bk.keyDir)
	bk.keyDirMu.RUnlock()

	bk.reply(conn, strconv.Itoa(count))
}

func (bk *Bitcask) handleCommandList(conn net.Conn) {
	var keyList string

	bk.keyDirMu.RLock()
	keys := make([]string, 0, len(bk.keyDir))
	for k := range bk.keyDir {
		keys = append(keys, k)
	}
	bk.keyDirMu.RUnlock()

	if len(keys) > 0 {
		keyList = "----- KEYS START -----\n" + strings.Join(keys, "\n") + "\n----- KEYS END -----"
	} else {
		keyList = "nil"
	}

	bk.reply(conn, keyList)
}

func (bk *Bitcask) handleCommandHelp(conn net.Conn) {
	helpString := `
Available Commands:

PING
  Check if the server is alive.
  Response: PONG!

SET <key> <value>
  Store a value for the given key.
  Overwrites the value if the key already exists.
  Response: ok

GET <key>
  Retrieve the value associated with the key.
  Response: value | nil

DELETE <key>
  Delete the key and its value.
  Response: ok

EXISTS <key>
  Check if a key exists.
  Response: true | false

COUNT
  Return the total number of keys stored.
  Response: integer

LIST
  List all stored keys.
  Response: list of keys | nil

HELP (cli only)
  Show this help message.

EXIT (cli only)
  Close the client connection.
`

	bk.reply(conn, strings.TrimSpace(helpString))
}

func (bk *Bitcask) handleInvalidCommand(conn net.Conn) {
	bk.reply(conn, "Invalid Command")
}

func (bk *Bitcask) writeToActiveFile(data []byte) (offset int64, filename string, err error) {
	bk.dataMu.Lock()
	defer bk.dataMu.Unlock()

	filename = bk.activeDataFile.Name()

	n, err := bk.activeDataFile.WriteAt(data, bk.activeOffset)
	if err != nil {
		return 0, "", err
	}

	offset = bk.activeOffset
	bk.activeOffset += int64(n)
	return offset, filename, nil
}

func (bk *Bitcask) readValueFromFile(filename string, offset, recordSize uint32) (string, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return "", err
	}

	buf := make([]byte, recordSize)
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return "", err
	}

	diskRecord, err := record.DecodeRecordFromBytes(buf)
	if err != nil {
		return "", err
	}

	ok := record.ValidateCRC(diskRecord.Key, diskRecord.Value, diskRecord.CRC)
	if !ok {
		return "", fmt.Errorf("Error while reading value")
	}

	return string(diskRecord.Value), nil

}

func (bk *Bitcask) setDataKeyDir(filename, key string, offset int64, keySize uint32, valueSize uint32) {
	keyDirEntry := KeyDirEntry{
		FileName:   filename,
		Offset:     uint32(offset),
		ValueSize:  valueSize,
		RecordSize: record.DiskRecordHeaderSizeBytes + keySize + valueSize,
		Timestamp:  time.Now().UnixNano(),
	}

	bk.keyDirMu.Lock()
	bk.keyDir[key] = keyDirEntry
	bk.keyDirMu.Unlock()
}

func (bk *Bitcask) deleteDataKeyDir(key string) {
	bk.keyDirMu.Lock()
	delete(bk.keyDir, key)
	bk.keyDirMu.Unlock()
}

func (bk *Bitcask) reply(conn net.Conn, msg string) {
	encodedResponse, err := protocol.EncodeResponse(msg)
	if err != nil {
		fmt.Println("Error encoding response:", err)
		return
	}

	_, err = conn.Write(encodedResponse)
	if err != nil {
		fmt.Printf("error replying to client: %v\n", err)
	}
}

func (bk *Bitcask) isFileSizeOverTheAllowedMaximum(file *os.File) (bool, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return false, err
	}

	fileSize := fileInfo.Size()
	if fileSize >= bk.MaximumDatafileSize {
		return true, nil
	}

	return false, nil
}

func (bk *Bitcask) generateHintFileForDatafile(datafilePath string) error {
	keys := []string{}

	datafileName := filepath.Base(datafilePath)

	bk.keyDirMu.RLock()
	for key, entry := range bk.keyDir {
		filename := entry.FileName
		if bk.getDatafileDirectoryPathSuffix()+datafileName != filename {
			continue
		}
		keys = append(keys, key)
	}
	bk.keyDirMu.RUnlock()

	err := bk.writeToHintFile(datafilePath, keys)
	if err != nil {
		fmt.Printf("Error writing to hint file %s : %v\n", datafilePath, err)
		return err
	}

	return nil
}

func (bk *Bitcask) writeToHintFile(fileFullPath string, keys []string) error {
	hintFileFullName := filepath.Base(fileFullPath)
	hintFileName := strings.Split(hintFileFullName, ".")[0]

	f, err := os.OpenFile(bk.getDatafileDirectoryPathSuffix()+hintFileName+HintFileExt, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Error opening hint file %s : %v", fileFullPath, err)
		return err
	}
	defer f.Close()

	for _, key := range keys {
		bk.keyDirMu.RLock()
		keyDirEntry := bk.keyDir[key]
		bk.keyDirMu.RUnlock()
		hintRecord := record.HintRecord{
			KeySize:      uint32(len(key)),
			FileNameSize: uint32(len(keyDirEntry.FileName)),
			Offset:       keyDirEntry.Offset,
			ValueSize:    keyDirEntry.ValueSize,
			Timestamp:    keyDirEntry.Timestamp,
			Key:          []byte(key),
			FileName:     []byte(keyDirEntry.FileName),
		}

		data, err := record.EncodeHintRecordToBytes(&hintRecord)
		if err != nil {
			return err
		}

		_, err = f.Write(data)
		if err != nil {
			return err
		}
	}

	err = f.Sync()
	if err != nil {
		fmt.Printf("Error Syncing Hint file: %s\n", fileFullPath)
		return err
	}

	return nil
}

func (bk *Bitcask) handleActiveDatafileSizeCheckInterval(ctx context.Context, seconds uint) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bk.dataMu.Lock()
			ok, err := bk.isFileSizeOverTheAllowedMaximum(bk.activeDataFile)
			bk.dataMu.Unlock()
			if err != nil {
				fmt.Println("[SIZE CHECK] Error while checking active data file size\n", err)
				continue
			}

			if ok {
				fmt.Printf("[SIZE CHECK] Active Datafile Size Over the Limits")
				err := bk.rotateActiveDatafile()
				if err != nil {
					fmt.Printf("[SIZE CHECK] Error rotating active datafile: %v\n", err)
				}
				fmt.Printf("[SIZE CHECK] Active Datafile Rotated to - %v\n", bk.activeDataFile.Name())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bk *Bitcask) handleSyncDiskInterval(ctx context.Context, seconds uint) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bk.dataMu.Lock()
			err := bk.activeDataFile.Sync()
			bk.dataMu.Unlock()

			if err != nil {
				fmt.Println("[SYNC] Error syncing active data file:", err)
			} else {
				fmt.Println("[SYNC] Disk Sync Completed Successfully")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bk *Bitcask) handleDatafilesMergeAndCompactionInterval(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("[MERGE] Setting up Merge and Compaction...")

			if !bk.mergeMu.TryLock() {
				fmt.Println("[MERGE] Cancelled Merge and Compaction - Another one running in Background.")
				continue
			}

			immutableFilePaths, err := bk.getImmutableDatafilePaths()
			if err != nil {
				fmt.Printf("Error while getting Immutable File Paths: %v\n", err)
				continue
			}

			// Takes a Snapshot of the Current KeyDir State
			bk.keyDirMu.RLock()
			keyDirSnapshot := make(map[string]KeyDirEntry, len(bk.keyDir))
			maps.Copy(keyDirSnapshot, bk.keyDir)
			bk.keyDirMu.RUnlock()

			totalImmutableBytes, garbageRatioMap, err := bk.getTotalImmutableSizeAndGarbageRatios(immutableFilePaths, keyDirSnapshot)
			if err != nil {
				fmt.Printf("Error while getting Garbage Ratio/Total File Size: %v\n", err)
				bk.mergeMu.Unlock()
				continue
			}

			if totalImmutableBytes < bk.MinTotalSizeForMerge {
				bk.mergeMu.Unlock()
				fmt.Printf("[MERGE] Cancelled Merge and Compaction - Total Immutable Bytes (%d) less than the configured threshold (%d)\n", totalImmutableBytes, bk.MinTotalSizeForMerge)
				continue
			}

			// Select candidate files based on garbage ratio
			candidateFiles := bk.selectMergeCandidates(immutableFilePaths, garbageRatioMap)

			if len(candidateFiles) < bk.MinRequiredMergableFiles {
				bk.mergeMu.Unlock()
				fmt.Printf("[MERGE] Cancelled Merge and Compaction - Number of Candidate Files (%d) less than the configured threshold (%d)\n", len(candidateFiles), bk.MinRequiredMergableFiles)
				continue
			}

			filteredKeyDir := bk.filterKeyDirForFiles(keyDirSnapshot, candidateFiles)

			// If filteredKeyDir is empty, it means none of the immutable files contain
			// live entries according to the KeyDir snapshot. All live keys already
			// reside in the active datafile, and the selected immutable files are
			// pure garbage. In this case, compaction would produce no output. The correct
			// behavior is to delete the obsolete datafiles (and their hint files) and skip
			// creating new merged files. This is a valid and optimal compaction outcome.
			if len(filteredKeyDir) == 0 {
				fmt.Println("[MERGE] All live keys reside in active datafile; deleting obsolete files")
				err = bk.deleteOlderDataAndHintfiles(candidateFiles)
				if err != nil {
					fmt.Println("[MERGE] Error deleting obsolete files:", err)
				}
				bk.mergeMu.Unlock()
				continue
			}

			fmt.Printf("[MERGE] Started Merge and Compaction with %d FILES and %d KEYS\n", len(candidateFiles), len(filteredKeyDir))
			err = bk.mergeAndCompactDatafiles(candidateFiles, filteredKeyDir)
			if err != nil {
				fmt.Println("[MERGE] Error while Merging and Compacting datafiles:", err)
			}
			bk.mergeMu.Unlock()
			fmt.Println("[MERGE] Completed Merge and Compaction")
		case <-ctx.Done():
			return
		}
	}
}

// Merge & compaction invariants:
//
//   - Active datafile is never merged
//   - Immutable datafiles are read-only
//   - A KeyDir snapshot is taken at merge start
//   - Only records matching the snapshot are rewritten
//   - New datafiles are fsynced and atomically renamed
//   - Old datafiles and hint files are deleted only after commit
//
// These invariants guarantee crash safety and correctness.
func (bk *Bitcask) mergeAndCompactDatafiles(immutableFiles []string, keyDirSnapshot map[string]KeyDirEntry) error {
	currentMergeFileNumber := 0
	mergeFiles := []string{}

	currentMergeFile, _, err := bk.createNewMergedFile(currentMergeFileNumber)
	if err != nil {
		fmt.Println("Error creating new merged file")
		return err
	}
	mergeFiles = append(mergeFiles, currentMergeFile.Name())

	var tempKeyDir = map[string]KeyDirEntry{}
	var mergeOffset int64 = 0

	for _, filePath := range immutableFiles {
		fmt.Printf("[MERGE] Reading Immutable File %s for Merging\n", filePath)
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Printf("Error opening file %s : %v", filePath, err)
			continue
		}

		var offset int64 = 0

		for {
			overTheSize, err := bk.isFileSizeOverTheAllowedMaximum(currentMergeFile)
			if err != nil {
				fmt.Printf("Error checking file size for %s : %v", currentMergeFile.Name(), err)
				return err
			}

			if overTheSize {
				err := currentMergeFile.Sync()
				if err != nil {
					fmt.Printf("Error syncing new merged file %s\n", currentMergeFile.Name())
					return err
				}
				currentMergeFile.Close()

				currentMergeFile, currentMergeFileNumber, err = bk.createNewMergedFile(currentMergeFileNumber)
				if err != nil {
					fmt.Println("Error creating new merged file")
					return err
				}
				mergeOffset = 0
				mergeFiles = append(mergeFiles, currentMergeFile.Name())
			}

			recordHeader := make([]byte, record.DiskRecordHeaderSizeBytes)
			_, err = io.ReadFull(f, recordHeader)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				return err
			}

			var crc uint32
			var timestamp int64
			var keySize uint32
			var valueSize uint32

			buf := bytes.NewReader(recordHeader)
			if err := binary.Read(buf, binary.LittleEndian, &crc); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.LittleEndian, &keySize); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.LittleEndian, &valueSize); err != nil {
				return err
			}

			if valueSize == 0 {
				if _, err := f.Seek(int64(keySize), io.SeekCurrent); err != nil {
					return err
				}

				offset += record.DiskRecordHeaderSizeBytes + int64(keySize)
				continue
			}

			key := make([]byte, keySize)
			if _, err := io.ReadFull(f, key); err != nil {
				fmt.Println("[MERGE] Error while reading key from Immutable File")
				return err
			}
			keyString := string(key)

			value := make([]byte, valueSize)
			if _, err := io.ReadFull(f, value); err != nil {
				fmt.Println("[MERGE] Error while reading value from Immutable File")
				return err
			}
			if !record.ValidateCRC(key, value, crc) {
				return fmt.Errorf("CRC validation failed for key %s in file %s", keyString, f.Name())
			}

			entry, ok := keyDirSnapshot[keyString]
			if !ok {
				continue
			}

			if entry.FileName == f.Name() && entry.Offset == uint32(offset) {
				recordToWrite := record.DiskRecord{
					CRC:       crc,
					Timestamp: timestamp,
					KeySize:   keySize,
					ValueSize: valueSize,
					Key:       key,
					Value:     value,
				}
				encodedRecord, err := record.EncodeRecordToBytes(&recordToWrite)
				if err != nil {
					return err
				}

				n, err := currentMergeFile.Write(encodedRecord)
				if err != nil {
					return err
				}

				newEntry := KeyDirEntry{
					FileName:   currentMergeFile.Name(),
					Offset:     uint32(mergeOffset),
					ValueSize:  valueSize,
					RecordSize: record.DiskRecordHeaderSizeBytes + keySize + valueSize,
					Timestamp:  timestamp,
				}

				tempKeyDir[keyString] = newEntry
				mergeOffset += int64(n)
			}

			offset += record.DiskRecordHeaderSizeBytes + int64(keySize) + int64(valueSize)
		}

		f.Close()
	}

	err = currentMergeFile.Sync()
	if err != nil {
		return err
	}
	currentMergeFile.Close()

	latestFileNumber, err := bk.getNextDatafileNumber()
	if err != nil {
		return err
	}

	newDataFiles := []string{}

	for _, mergeFilePath := range mergeFiles {
		newMergeFilePath := bk.getDatafileDirectoryPathSuffix() + DataFilePrefix + strconv.Itoa(latestFileNumber) + DataFileExt

		err := os.Rename(mergeFilePath, newMergeFilePath)
		if err != nil {
			fmt.Printf("Error renaming the file %s", mergeFilePath)
			return err
		}
		fmt.Printf("[MERGE] Created New *Merged* Datafile - %s\n", newMergeFilePath)

		newDataFiles = append(newDataFiles, newMergeFilePath)
		latestFileNumber++
	}

	mergeToFinal := make(map[string]string)
	for i := range mergeFiles {
		mergeToFinal[mergeFiles[i]] = newDataFiles[i]
	}

	for key, entry := range tempKeyDir {
		if newName, ok := mergeToFinal[entry.FileName]; ok {
			entry.FileName = newName
			tempKeyDir[key] = entry
		}
	}

	bk.keyDirMu.Lock()
	for key, entry := range bk.keyDir {
		fName := entry.FileName
		for _, fullImmutableFileName := range immutableFiles {
			fileName := filepath.Base(fullImmutableFileName)

			if fName == fileName {
				delete(bk.keyDir, key)
			}
		}
	}

	for key, entry := range tempKeyDir {
		v, ok := bk.keyDir[key]
		if !ok || entry.Timestamp > v.Timestamp {
			bk.keyDir[key] = entry
		}
	}
	bk.keyDirMu.Unlock()

	err = bk.deleteOlderDataAndHintfiles(immutableFiles)
	if err != nil {
		return err
	}

	return nil
}

func (bk *Bitcask) createNewMergedFile(number int) (*os.File, int, error) {
	numStr := strconv.Itoa(int(number))
	mergedFileName := MergedFilePrefix + numStr + DataFileExt

	f, err := os.OpenFile(bk.getDatafileDirectoryPathSuffix()+mergedFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}

	return f, number + 1, nil
}

func (bk *Bitcask) getImmutableDatafilePaths() ([]string, error) {
	immutableFilePaths := []string{}

	datafileNames, err := bk.scanForDatafileNames()
	if err != nil {
		return nil, err
	}

	for _, filename := range datafileNames {
		fullFileName := bk.getDatafileDirectoryPathSuffix() + filename + DataFileExt
		if fullFileName == bk.activeDataFile.Name() {
			continue
		}
		immutableFilePaths = append(immutableFilePaths, fullFileName)
	}

	return immutableFilePaths, nil
}

func (bk *Bitcask) getTotalImmutableSizeAndGarbageRatios(immutableFilePaths []string, keyDirSnapshot map[string]KeyDirEntry) (int64, map[string]float64, error) {
	totalImmutableBytes := int64(0)
	liveBytes := map[string]int64{}
	fileToGarbageRatioMap := make(map[string]float64)

	for _, entry := range keyDirSnapshot {
		liveBytes[entry.FileName] += int64(entry.RecordSize)
	}

	for _, filepath := range immutableFilePaths {
		info, err := os.Stat(filepath)
		if err != nil {
			return 0, nil, err
		}

		fileSize := info.Size()
		live := liveBytes[filepath]

		ratio := 1 - float64(live)/float64(fileSize)
		fileToGarbageRatioMap[filepath] = ratio
		totalImmutableBytes += fileSize
	}

	return totalImmutableBytes, fileToGarbageRatioMap, nil
}

func (bk *Bitcask) selectMergeCandidates(
	immutableFiles []string,
	garbageRatioMap map[string]float64,
) []string {
	// Filter by minimum garbage ratio
	candidates := []candidate{}
	for _, path := range immutableFiles {
		ratio := garbageRatioMap[path]
		if ratio >= bk.GarbageRatio {
			fInfo, err := os.Stat(path)
			if err != nil {
				return nil
			}
			size := fInfo.Size()
			candidates = append(candidates, candidate{path, ratio, size})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	if len(candidates) < bk.MinRequiredMergableFiles {
		return nil
	}

	// Sort by garbage ratio (highest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].garbageRatio > candidates[j].garbageRatio
	})

	// Select up to limits
	selected := []string{}
	var totalBytes int64

	for _, c := range candidates {
		if len(selected) == 0 {
			selected = append(selected, c.path)
			totalBytes += c.size
			continue
		}

		if len(selected) >= bk.MaxMergeFiles {
			break
		}
		if totalBytes+c.size > bk.MaxMergeBytes {
			break
		}

		selected = append(selected, c.path)
		totalBytes += c.size
	}

	return selected
}

// Filter keyDir to only include entries from selected files
func (bk *Bitcask) filterKeyDirForFiles(fullKeyDir map[string]KeyDirEntry, filePaths []string) map[string]KeyDirEntry {
	fileSet := make(map[string]bool, len(filePaths))
	for _, path := range filePaths {
		fileSet[path] = true
	}

	filtered := map[string]KeyDirEntry{}
	for key, entry := range fullKeyDir {
		if fileSet[entry.FileName] {
			filtered[key] = entry
		}
	}

	return filtered
}

func (bk *Bitcask) deleteOlderDataAndHintfiles(immutableFiles []string) error {
	hintFiles := []string{}

	for _, filePath := range immutableFiles {
		hintFile := strings.TrimSuffix(filePath, DataFileExt) + HintFileExt
		hintFiles = append(hintFiles, hintFile)
	}

	for _, oldFile := range immutableFiles {
		fmt.Printf("[MERGE] Deleted old datafiles - %s\n", oldFile)
		err := os.Remove(oldFile)
		if err != nil {
			fmt.Printf("Error deleting file %s\n", oldFile)
			return err
		}
	}

	for _, hintFile := range hintFiles {
		fmt.Printf("[MERGE] Deleted old hintfiles - %s\n", hintFile)
		err := os.Remove(hintFile)
		if err != nil {
			fmt.Printf("Error deleting file %s\n", hintFile)
			return err
		}
	}

	return nil
}

func (bk *Bitcask) getDatafileDirectoryPathSuffix() string {
	return filepath.Join(bk.DirectoryPath, DataDirName) + string(filepath.Separator)
}

func (bk *Bitcask) getNextDatafileNumber() (int, error) {
	files, err := os.ReadDir(bk.getDatafileDirectoryPathSuffix())
	if err != nil {
		return 0, err
	}

	max := -1

	for _, entry := range files {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, DataFileExt) {
			continue
		}

		base := strings.TrimSuffix(name, DataFileExt)
		parts := strings.Split(base, "_")
		if len(parts) != 2 {
			continue
		}

		n, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		if n > max {
			max = n
		}
	}

	return max + 1, nil
}

// Stop gracefully shuts down the Bitcask instance.
//
// It cancels all background goroutines, closes the active datafile,
// and releases the directory lock.
func (bk *Bitcask) Stop() {
	if bk.serverCancel != nil {
		bk.serverCancel()
	}

	if bk.syncCancel != nil {
		bk.syncCancel()
	}

	if bk.sizeCheckCancel != nil {
		bk.sizeCheckCancel()
	}

	if bk.mergeAndCompactionCancel != nil {
		bk.mergeAndCompactionCancel()
	}

	bk.dataMu.Lock()
	if bk.activeDataFile != nil {
		err := bk.activeDataFile.Close()
		if err != nil {
			fmt.Println("Error while closing the Active Datafile:", err)
		}
	}
	bk.dataMu.Unlock()

	if bk.lockFile != nil {
		lock.UnlockDirectory(bk.lockFile)
	}
}
