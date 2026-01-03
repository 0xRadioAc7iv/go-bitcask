package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"
)

// DiskRecord represents a single append-only record stored in a Bitcask
// datafile.
//
// DiskRecords are written sequentially and never modified in place.
// Newer versions of a key supersede older ones based on the Timestamp.
//
// Layout on disk (little-endian):
//
//	CRC (4 bytes)
//	Timestamp (8 bytes)
//	KeySize (4 bytes)
//	ValueSize (4 bytes)
//	Key (KeySize bytes)
//	Value (ValueSize bytes)
//
// A record with ValueSize == 0 represents a tombstone (logical delete).
type DiskRecord struct {
	CRC       uint32 // CRC checksum of Key and Value
	Timestamp int64  // Unix timestamp in nanoseconds
	KeySize   uint32 // Length of Key in bytes
	ValueSize uint32 // Length of Value in bytes (0 indicates tombstone)
	Key       []byte // Raw key bytes
	Value     []byte // Raw value bytes
}

// HintRecord represents a compact index entry stored in a hint file.
//
// Hint files are advisory structures used to reconstruct the in-memory
// KeyDir quickly on startup without scanning full datafiles.
//
// Layout on disk (little-endian):
//
//	KeySize (4 bytes)
//	FileNameSize (4 bytes)
//	Offset (4 bytes)
//	ValueSize (4 bytes)
//	Timestamp (8 bytes)
//	Key (KeySize bytes)
//	FileName (FileNameSize bytes)
type HintRecord struct {
	KeySize      uint32 // Length of Key in bytes
	FileNameSize uint32 // Length of FileName in bytes
	Offset       uint32 // Byte offset of the record in the datafile
	ValueSize    uint32 // Length of Value in bytes
	Timestamp    int64  // Timestamp of the record
	Key          []byte // Raw key bytes
	FileName     []byte // Datafile name containing the record
}

// CRC (4) + Timestamp (8) + KeySize (4) + ValueSize (4)
const DiskRecordHeaderSizeBytes = 20

// KeySize (4) + FileNameSize (4) + Offset (4) + ValueSize (4) + Timestamp (8)
const HintRecordHeaderSizeBytes = 24

// CreateRecord constructs a new DiskRecord for the given key and value.
//
// It computes the CRC over the key and value and assigns the current
// time as the record timestamp.
func CreateRecord(key, value string) DiskRecord {
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	record := DiskRecord{
		CRC:       CalculateCRC(keyBytes, valueBytes),
		Timestamp: time.Now().UnixNano(),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       keyBytes,
		Value:     valueBytes,
	}

	return record
}

// CreateTombstoneRecord constructs a DiskRecord representing a logical
// delete (tombstone) for the given key.
//
// Tombstone records have ValueSize == 0 and are used to invalidate
// older versions of the key during reads and compaction.
func CreateTombstoneRecord(key string) DiskRecord {
	keyBytes := []byte(key)

	record := DiskRecord{
		CRC:       0,
		Timestamp: time.Now().UnixNano(),
		KeySize:   uint32(len(key)),
		ValueSize: 0,
		Key:       keyBytes,
		Value:     nil,
	}

	return record
}

// EncodeRecordToBytes serializes a DiskRecord into its on-disk binary form.
//
// The returned byte slice is suitable for direct append-only writing
// to a Bitcask datafile.
func EncodeRecordToBytes(record *DiskRecord) ([]byte, error) {
	buf := &bytes.Buffer{} // Initializes an empty (zero-valued) buffer

	if err := binary.Write(buf, binary.LittleEndian, record.CRC); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, record.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, record.KeySize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, record.ValueSize); err != nil {
		return nil, err
	}
	if _, err := buf.Write(record.Key); err != nil {
		return nil, err
	}
	if _, err := buf.Write(record.Value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeRecordFromBytes deserializes a DiskRecord from its binary form.
//
// The input byte slice must contain a complete record, including the
// header, key, and value. Tombstone records (ValueSize == 0) are rejected.
func DecodeRecordFromBytes(data []byte) (*DiskRecord, error) {
	var crc uint32
	var timestamp int64
	var keySize uint32
	var valueSize uint32

	buf := bytes.NewReader(data)

	if err := binary.Read(buf, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &keySize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &valueSize); err != nil {
		return nil, err
	}

	if keySize == 0 || valueSize == 0 {
		return nil, errors.New("invalid record sizes")
	}

	key := make([]byte, keySize)
	if _, err := io.ReadFull(buf, key); err != nil {
		return nil, err
	}

	value := make([]byte, valueSize)
	if _, err := io.ReadFull(buf, value); err != nil {
		return nil, err
	}

	return &DiskRecord{
		CRC:       crc,
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}, nil
}

// EncodeHintRecordToBytes serializes a HintRecord into its on-disk binary form.
//
// Hint records are written sequentially into hint files and are used
// only during startup for faster KeyDir reconstruction.
func EncodeHintRecordToBytes(hintRecord *HintRecord) ([]byte, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, hintRecord.KeySize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, hintRecord.FileNameSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, hintRecord.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, hintRecord.ValueSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, hintRecord.Timestamp); err != nil {
		return nil, err
	}
	if _, err := buf.Write(hintRecord.Key); err != nil {
		return nil, err
	}
	if _, err := buf.Write(hintRecord.FileName); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}

// DecodeHintRecordFromBytes deserializes a HintRecord from its binary form.
//
// The input byte slice must contain a complete hint record, including
// the header, key, and file name.
func DecodeHintRecordFromBytes(data []byte) (*HintRecord, error) {
	var keySize uint32
	var fileNameSize uint32
	var offset uint32
	var valueSize uint32
	var timestamp int64

	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, &keySize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &fileNameSize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &offset); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &valueSize); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &timestamp); err != nil {
		return nil, err
	}

	key := make([]byte, keySize)
	if _, err := io.ReadFull(buf, key); err != nil {
		return nil, err
	}
	fileName := make([]byte, fileNameSize)
	if _, err := io.ReadFull(buf, fileName); err != nil {
		return nil, err
	}

	return &HintRecord{
		KeySize:      keySize,
		FileNameSize: fileNameSize,
		Offset:       offset,
		ValueSize:    valueSize,
		Timestamp:    timestamp,
		Key:          key,
		FileName:     fileName,
	}, nil
}
