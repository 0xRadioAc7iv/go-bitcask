package bitcask

import (
	"bufio"
	"context"
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
	"github.com/0xRadioAc7iv/go-bitcask/internal/record"
	"github.com/0xRadioAc7iv/go-bitcask/internal/server"
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

const ZERO_NAME = "000"

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

	files, err := bk.scanDatafiles()
	if err != nil {
		fmt.Println("Error Scanning for Datafiles")
		return err
	}

	bk.keyDir = make(KeyDir)

	// Read All Scanned Datafiles and load data into KeyDir
	// Later, do this with hint files, and make datafiles reading a fallback
	// for nonexistent hint files

	if len(files) == 0 {
		latestFileName = ZERO_NAME
	} else {
		latestFileName = files[len(files)-1]
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

		newFileNumber = number + 1
	}

	newFileName := fmt.Sprintf("%s%s%d%s", datafileDirPathSuffix, DataFileSuffix, newFileNumber, DataFileExt)
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

	reader := bufio.NewReader(conn)

	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("client disconnected")
			return
		}

		command = strings.TrimSpace(command)
		bk.handleCommand(command, conn)
	}
}

func (bk *Bitcask) handleCommand(command string, conn net.Conn) {
	parts := strings.Fields(command)

	if len(parts) == 0 {
		bk.reply(conn, "")
		return
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "ping":
		bk.handleCommandPing(conn)
	case "set":
		bk.handleCommandSET(conn, parts)
	case "get":
		bk.handleCommandGET(conn, parts)
	case "delete":
		bk.handleCommandDelete(conn, parts)
	case "exists":
		bk.handleCommandExists(conn, parts)
	case "count":
		bk.handleCommandCount(conn, parts)
	case "list":
		bk.handleCommandList(conn, parts)
	default:
		bk.handleInvalidCommand(conn)
	}
}

func (bk *Bitcask) handleCommandPing(conn net.Conn) {
	bk.reply(conn, "PONG!")
}

func (bk *Bitcask) handleCommandGET(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		bk.reply(conn, "Command Error (GET): Key is required")
		return
	}

	key := parts[1]

	bk.keyDirMu.RLock()
	keyDirEntry, ok := bk.keyDir[key]
	bk.keyDirMu.RUnlock()

	if !ok {
		bk.reply(conn, "nil")
		return
	}

	value, err := bk.readFromFile(keyDirEntry.FileName, keyDirEntry.Offset, keyDirEntry.RecordSize)
	if err != nil {
		bk.reply(conn, "Error while reading value")
	}

	bk.reply(conn, value)
}

func (bk *Bitcask) handleCommandSET(conn net.Conn, parts []string) {
	if len(parts) > 3 {
		bk.reply(conn, "Command Error (SET): Too many arguments")
		return
	}
	if len(parts) < 3 {
		bk.reply(conn, "Command Error (SET): Key and Value is required")
		return
	}

	key := parts[1]
	value := parts[2]

	diskRecord := record.CreateRecord(key, value)
	encoded, err := record.EncodeRecordToBytes(&diskRecord)
	if err != nil {
		bk.reply(conn, "Error while setting value")
		return
	}

	offset, filename, err := bk.writeToActiveFile(encoded)
	if err != nil {
		bk.reply(conn, "Error while setting value")
		return
	}
	bk.setDataKeyDir(filename, key, offset, diskRecord.KeySize, diskRecord.ValueSize)

	bk.reply(conn, "ok")
}

func (bk *Bitcask) handleCommandDelete(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		bk.reply(conn, "Command Error (DELETE): Key is required")
		return
	}

	key := parts[1]

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

func (bk *Bitcask) handleCommandExists(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		bk.reply(conn, "Command Error (EXISTS): Key is required")
		return
	}

	key := parts[1]

	bk.keyDirMu.RLock()
	_, ok := bk.keyDir[key]
	bk.keyDirMu.RUnlock()

	if !ok {
		bk.reply(conn, "false")
		return
	}

	bk.reply(conn, "true")
}

func (bk *Bitcask) handleCommandCount(conn net.Conn, parts []string) {
	if len(parts) != 1 {
		bk.reply(conn, "Command Error (COUNT): Too many arguments")
		return
	}

	bk.keyDirMu.RLock()
	count := len(bk.keyDir)
	bk.keyDirMu.RUnlock()

	bk.reply(conn, strconv.Itoa(count))
}

func (bk *Bitcask) handleCommandList(conn net.Conn, parts []string) {
	if len(parts) != 1 {
		bk.reply(conn, "Command Error (LIST): Too many arguments")
		return
	}

	bk.keyDirMu.RLock()
	keys := make([]string, 0, len(bk.keyDir))
	for k := range bk.keyDir {
		keys = append(keys, k)
	}
	bk.keyDirMu.RUnlock()

	keyList := "----- KEYS START -----\n" + strings.Join(keys, "\n") + "\n----- KEYS END -----"

	bk.reply(conn, keyList)
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

func (bk *Bitcask) readFromFile(filename string, offset, recordSize uint32) (string, error) {
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
		RecordSize: 20 + keySize + valueSize, // 20 = CRC (4) + Timestamp (8) + KeySizeField (4) + ValueSizeField (4)
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
	_, err := conn.Write([]byte(msg + "\n"))
	if err != nil {
		fmt.Println("client disconnected")
	}
}

func (bk *Bitcask) isActiveDatafileSizeOverTheAllowedMaximum() (bool, error) {
	bk.dataMu.Lock()
	fileInfo, err := bk.activeDataFile.Stat()
	bk.dataMu.Unlock()

	if err != nil {
		return false, errors.New("Error while fetching Active Datafile Info")
	}

	fileSize := fileInfo.Size()
	if fileSize >= int64(bk.MaximumDatafileSize) {
		return true, nil
	}

	return false, nil
}

func (bk *Bitcask) activeDatafileSizeCheckInterval(ctx context.Context, seconds uint) {
	ticker := time.NewTicker(time.Duration(seconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ok, err := bk.isActiveDatafileSizeOverTheAllowedMaximum()
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
