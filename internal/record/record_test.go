package record

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestEncodeDecodeRecord(t *testing.T) {
	key := []byte("language")
	value := []byte("go")

	original := &DiskRecord{
		CRC:       CalculateCRC(key, value),
		Timestamp: time.Now().UnixNano(),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}

	encoded, err := EncodeRecordToBytes(original)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	decoded, err := DecodeRecordFromBytes(encoded)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	// field-by-field comparison
	if decoded.CRC != original.CRC {
		t.Errorf("CRC mismatch: got %v, want %v", decoded.CRC, original.CRC)
	}
	if decoded.Timestamp != original.Timestamp {
		t.Errorf("Timestamp mismatch: got %v, want %v", decoded.Timestamp, original.Timestamp)
	}
	if decoded.KeySize != original.KeySize {
		t.Errorf("KeySize mismatch: got %v, want %v", decoded.KeySize, original.KeySize)
	}
	if decoded.ValueSize != original.ValueSize {
		t.Errorf("ValueSize mismatch: got %v, want %v", decoded.ValueSize, original.ValueSize)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key mismatch: got %v, want %v", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value mismatch: got %v, want %v", decoded.Value, original.Value)
	}
}

func TestDecodeErrorsOnTruncatedData(t *testing.T) {
	record := &DiskRecord{
		CRC:       555,
		Timestamp: 123123123,
		KeySize:   3,
		ValueSize: 2,
		Key:       []byte("abc"),
		Value:     []byte("xy"),
	}

	encoded, _ := EncodeRecordToBytes(record)

	for i := 0; i < len(encoded); i++ {
		_, err := DecodeRecordFromBytes(encoded[:i])
		if err == nil {
			t.Fatalf("expected error when decoding truncated data of length %d, got nil", i)
		}
	}
}

func TestEncodedByteLayout(t *testing.T) {
	key := []byte("a")
	value := []byte("b")

	r := &DiskRecord{
		CRC:       1,
		Timestamp: 2,
		KeySize:   1,
		ValueSize: 1,
		Key:       key,
		Value:     value,
	}

	encoded, err := EncodeRecordToBytes(r)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Expected bytes structure:
	// uint32 CRC
	// int64 Timestamp
	// uint32 KeySize
	// uint32 ValueSize
	// []byte Key
	// []byte Value
	offset := 0

	expectUint32 := func(name string, want uint32) {
		got := binary.LittleEndian.Uint32(encoded[offset : offset+4])
		if got != want {
			t.Fatalf("%s mismatch: got %v want %v", name, got, want)
		}
		offset += 4
	}

	expectInt64 := func(name string, want int64) {
		got := int64(binary.LittleEndian.Uint64(encoded[offset : offset+8]))
		if got != want {
			t.Fatalf("%s mismatch: got %v want %v", name, got, want)
		}
		offset += 8
	}

	expectUint32("CRC", r.CRC)
	expectInt64("Timestamp", r.Timestamp)
	expectUint32("KeySize", r.KeySize)
	expectUint32("ValueSize", r.ValueSize)

	if encoded[offset] != 'a' {
		t.Fatalf("expected key byte 'a', got %v", encoded[offset])
	}
	offset++

	if encoded[offset] != 'b' {
		t.Fatalf("expected value byte 'b', got %v", encoded[offset])
	}
}
