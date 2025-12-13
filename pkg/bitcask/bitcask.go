package bitcask

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/0xRadioAc7iv/go-bitcask/internal/lock"
	"github.com/0xRadioAc7iv/go-bitcask/internal/record"
	"github.com/0xRadioAc7iv/go-bitcask/internal/server"
)

type Bitcask struct {
	lockFile       *os.File
	activeDataFile *os.File
	activeOffset   int64
	serverCancel   context.CancelFunc
	keyDir         KeyDir

	DirectoryPath       string
	MaximumDatafileSize int
	ListenerPort        int
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

	bk.keyDir = make(KeyDir)

	// Read All Scanned Datafiles and load data into KeyDir
	// Later, do this with hint files, and make datafiles reading a fallback
	// for nonexistent hint files

	f, err := bk.createNewActiveDatafile(files)
	if err != nil {
		return err
	}

	offset, _ := f.Seek(0, io.SeekEnd)
	bk.activeOffset = offset
	bk.activeDataFile = f

	ctx, cancel := context.WithCancel(context.Background())
	bk.serverCancel = cancel

	go func() {
		if err := server.Start(ctx, bk.ListenerPort, bk.commandHandler); err != nil {
			fmt.Println("Server stopped abruptly:", err)
		}
	}()

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
	keyDirEntry, ok := bk.keyDir[key]
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

	offset, err := bk.writeToActiveFile(encoded)
	if err != nil {
		bk.reply(conn, "Error while setting value")
		return
	}
	bk.setDataKeyDir(key, offset, diskRecord.KeySize, diskRecord.ValueSize)

	bk.reply(conn, "ok")
}

func (bk *Bitcask) handleCommandDelete(conn net.Conn, parts []string) {
	if len(parts) != 2 {
		bk.reply(conn, "Command Error (DELETE): Key is required")
		return
	}

	key := parts[1]

	tombstoneRecord := record.CreateTombstoneRecord(key)
	fmt.Println(tombstoneRecord)
	encoded, err := record.EncodeRecordToBytes(&tombstoneRecord)
	if err != nil {
		bk.reply(conn, "Error while deleting value")
		return
	}

	_, err = bk.writeToActiveFile(encoded)
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
	_, ok := bk.keyDir[key]
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

	count := len(bk.keyDir)
	bk.reply(conn, strconv.Itoa(count))
}

func (bk *Bitcask) handleInvalidCommand(conn net.Conn) {
	bk.reply(conn, "Invalid Command")
}

func (bk *Bitcask) writeToActiveFile(data []byte) (offset int64, err error) {
	n, err := bk.activeDataFile.WriteAt(data, bk.activeOffset)
	if err != nil {
		return 0, err
	}
	err = bk.activeDataFile.Sync()
	if err != nil {
		return 0, err
	}

	offset = bk.activeOffset
	bk.activeOffset += int64(n)
	return offset, nil
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

func (bk *Bitcask) setDataKeyDir(key string, offset int64, keySize uint32, valueSize uint32) {
	keyDirEntry := KeyDirEntry{
		FileName:   bk.activeDataFile.Name(),
		Offset:     uint32(offset),
		ValueSize:  valueSize,
		RecordSize: 20 + keySize + valueSize, // 20 = CRC (4) + Timestamp (8) + KeySizeField (4) + ValueSizeField (4)
	}

	bk.keyDir[key] = keyDirEntry
}

func (bk *Bitcask) deleteDataKeyDir(key string) {
	delete(bk.keyDir, key)
}

func (bk *Bitcask) reply(conn net.Conn, msg string) {
	_, err := conn.Write([]byte(msg + "\n"))
	if err != nil {
		fmt.Println("client disconnected")
	}
}

func (bk *Bitcask) Stop() {
	if bk.serverCancel != nil {
		bk.serverCancel()
	}

	if bk.activeDataFile != nil {
		err := bk.activeDataFile.Close()
		if err != nil {
			fmt.Println("Error while closing the Active Datafile:", err)
		}
	}

	if bk.lockFile != nil {
		lock.UnlockDirectory(bk.lockFile)
	}
}
