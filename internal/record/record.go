package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"
)

type DiskRecord struct {
	CRC       uint32 // Checksum of Data
	Timestamp int64  // Unix Timestamp in Nanoseconds
	KeySize   uint32 // Length of Key in Bytes
	ValueSize uint32 // Length of Value in Bytes
	Key       []byte
	Value     []byte
}

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
