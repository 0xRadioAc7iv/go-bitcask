package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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
	lockFile        *os.File
	activeDataFile  *os.File
	activeOffset    int64
	serverCancel    context.CancelFunc
	syncCancel      context.CancelFunc
	sizeCheckCancel context.CancelFunc
	keyDir          KeyDir

	dataMu   sync.Mutex   // for activeDataFile + activeOffset
	keyDirMu sync.RWMutex // for keyDir

	DirectoryPath       string
	MaximumDatafileSize int
	ListenerPort        int
	SyncInterval        uint
	SizeCheckInterval   uint
}

func (bk *Bitcask) Start() error {
	var latestFileName string

	lf, err := lock.LockDirectory(bk.DirectoryPath)
	if err != nil {
		fmt.Println("Error Locking Bitcask Datafiles Directory")
		return err
	}
	bk.lockFile = lf

	err = bk.openDataDirectory()
	if err != nil {
		fmt.Println("Error Opening Bitcask Datafiles Directory")
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
		latestFileName = ZERO_NAME
	} else {
		latestFileName = filenames[len(filenames)-1] + DataFileExt
	}

	f, err := bk.createNewActiveDatafile(latestFileName)
	if err != nil {
		fmt.Println("Error creating new active datafile")
		return err
	}

	// Sets the offset to the end of the active datafile
	offset, _ := f.Seek(0, io.SeekEnd)
	bk.activeOffset = offset
	bk.activeDataFile = f

	ctx, cancel := context.WithCancel(context.Background())
	bk.serverCancel = cancel
	go func() {
		if err := server.Start(ctx, bk.ListenerPort, bk.commandHandler); err != nil {
			fmt.Println("Server stopped abruptly")
			panic(err)
		}
	}()

	syncCtx, syncCancel := context.WithCancel(context.Background())
	bk.syncCancel = syncCancel
	go bk.syncDiskInterval(syncCtx, bk.SyncInterval)

	sizeCheckCtx, sizeCheckCancel := context.WithCancel(context.Background())
	bk.sizeCheckCancel = sizeCheckCancel
	go bk.activeDatafileSizeCheckInterval(sizeCheckCtx, bk.SizeCheckInterval)

	fmt.Println("Bitcask started succesfully...")
	fmt.Printf("Server listening on http://localhost:%d...\n", bk.ListenerPort)

	return nil
}

func (bk *Bitcask) openDataDirectory() error {
	fullDatafileDirectoryPath := bk.DirectoryPath + DataDirName

	if utils.PathExists(fullDatafileDirectoryPath) {
		fmt.Println("Datafile Directory already exists. Skipping creation...")
		return nil
	}

	fmt.Println("Datafile Directory does not exist! Creating one...")
	// 0 (special bit - ignored), 7 (rwx - owner), 5 (r-x - user group), 5 (r-x - others)
	err := os.Mkdir(fullDatafileDirectoryPath, 0755)
	if err != nil {
		return err
	}
	return nil
}

// Looks for '.data' files inside the datafile directory but returns their names
func (bk *Bitcask) scanForDatafileNames() ([]string, error) {
	files, err := os.ReadDir(bk.DirectoryPath + DataDirName)
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

	filePathPrefix := bk.DirectoryPath + DataDirName + "/"

	for _, filename := range datafileNames {
		hintFilePath := filePathPrefix + filename + HintFileExt
		if utils.PathExists(hintFilePath) {
			fmt.Printf("Reading Hint file: %s\n", hintFilePath)
			err := bk.readHintfile(hintFilePath)
			if err == nil {
				continue
			}

			fmt.Printf("Error reading hint file %s : %v\n", hintFilePath, err)
		}

		dataFilePath := filePathPrefix + filename + DataFileExt
		fmt.Printf("Reading Datafile: %s\n", dataFilePath)
		err := bk.readDatafile(dataFilePath)
		if err != nil {
			panic(err)
		}
	}

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
		fmt.Printf("Error opening the file %v\n: %v", f.Name(), err)
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
	datafileDirPathSuffix := bk.DirectoryPath + DataDirName + "/"

	if latestFileName == ZERO_NAME {
		newFileNumber = 0
	} else {
		base := strings.Split(latestFileName, ".")[0]
		numberStr := strings.Split(base, "_")[1]
		number, err := strconv.Atoi(numberStr)
		if err != nil {
			return nil, err
		}

		f, err := os.OpenFile(datafileDirPathSuffix+latestFileName, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		// Checks the size of the previous latest file, if it's size is less than
		// the allowed maximum, then returns it, saving disk space and prevents
		// from creating too many datafiles
		overTheAllowedMaxSize, err := bk.isDatafileSizeOverTheAllowedMaximum(f)
		if err != nil {
			return nil, err
		}

		if !overTheAllowedMaxSize {
			return f, nil
		}

		newFileNumber = number + 1
	}

	newFileName := fmt.Sprintf("%s%s%d%s", datafileDirPathSuffix, FileSuffix, newFileNumber, DataFileExt)
	return os.OpenFile(newFileName, os.O_CREATE|os.O_RDWR, 0644)
}

func (bk *Bitcask) rotateActiveDatafile() error {
	bk.dataMu.Lock()
	defer bk.dataMu.Unlock()

	latestFileName := bk.activeDataFile.Name()

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
			fmt.Println("client disconnected")
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
	defer bk.keyDirMu.Unlock()

	bk.keyDir[key] = keyDirEntry
}

func (bk *Bitcask) deleteDataKeyDir(key string) {
	bk.keyDirMu.Lock()
	defer bk.keyDirMu.Unlock()

	delete(bk.keyDir, key)
}

func (bk *Bitcask) reply(conn net.Conn, msg string) {
	encodedResponse, err := protocol.EncodeResponse(msg)
	if err != nil {
		fmt.Println("Error encoding response:", err)
		return
	}

	_, err = conn.Write(encodedResponse)
	if err != nil {
		fmt.Println("client disconnected")
	}
}

func (bk *Bitcask) isDatafileSizeOverTheAllowedMaximum(datafile *os.File) (bool, error) {
	fileInfo, err := datafile.Stat()

	if err != nil {
		return false, errors.New("Error while fetching Active Datafile Info")
	}

	fileSize := fileInfo.Size()
	if fileSize >= int64(bk.MaximumDatafileSize) {
		return true, nil
	}

	return false, nil
}

func (bk *Bitcask) generateHintFiles() error {
	// Maps Filenames to Keys
	fileNameToKeyMap := make(map[string][]string)

	for key, entry := range bk.keyDir {
		filename := entry.FileName
		prev, ok := fileNameToKeyMap[filename]
		if !ok {
			fileNameToKeyMap[filename] = []string{key}
		} else {
			fileNameToKeyMap[filename] = append(prev, key)
		}
	}

	for filepath, keys := range fileNameToKeyMap {
		err := bk.writeToHintFile(filepath, keys)
		if err != nil {
			fmt.Printf("Error writing to hint file %s : %v\n", filepath, err)
			return err
		}
	}

	return nil
}

func (bk *Bitcask) writeToHintFile(filepath string, keys []string) error {
	datafileDirPathSuffix := bk.DirectoryPath + DataDirName + "/"

	parts := strings.Split(filepath, "/")
	hintFileFullName := parts[len(parts)-1]
	hintFileName := strings.Split(hintFileFullName, ".")[0]

	f, err := os.OpenFile(datafileDirPathSuffix+hintFileName+HintFileExt, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Error opening hint file %s : %v", filepath, err)
	}
	defer f.Close()

	for _, key := range keys {
		keyDirEntry := bk.keyDir[key]
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
		fmt.Printf("Error Syncing Hint file: %s\n", filepath)
		return err
	}

	return nil
}

func (bk *Bitcask) activeDatafileSizeCheckInterval(ctx context.Context, seconds uint) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bk.dataMu.Lock()
			ok, err := bk.isDatafileSizeOverTheAllowedMaximum(bk.activeDataFile)
			bk.dataMu.Unlock()
			if err != nil {
				fmt.Println("Error while checking active data file size:", err)
				continue
			}

			if ok {
				err := bk.rotateActiveDatafile()
				if err != nil {
					panic(err)
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func (bk *Bitcask) syncDiskInterval(ctx context.Context, seconds uint) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bk.dataMu.Lock()
			err := bk.activeDataFile.Sync()
			bk.dataMu.Unlock()

			if err != nil {
				fmt.Println("Error syncing active data file:", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

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

	bk.dataMu.Lock()
	if bk.activeDataFile != nil {
		err := bk.activeDataFile.Close()
		if err != nil {
			fmt.Println("Error while closing the Active Datafile:", err)
		}
	}
	bk.dataMu.Unlock()

	err := bk.generateHintFiles()
	if err != nil {
		fmt.Println("Error while generating hint files:", err)
	}

	if bk.lockFile != nil {
		lock.UnlockDirectory(bk.lockFile)
	}
}
