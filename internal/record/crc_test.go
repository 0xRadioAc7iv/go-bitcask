package record

import (
	"hash/crc32"
	"testing"
)

func TestCRC(t *testing.T) {
	var key = []byte("language")
	var value = []byte("go")

	want := crc32.ChecksumIEEE(append(key, value...))

	t.Run("CalculateCRC computes expected checksum", func(t *testing.T) {
		got := CalculateCRC(key, value)
		if got != want {
			t.Errorf("CalculateCRC() = %v, want %v", got, want)
		}
	})

	t.Run("ValidateCRC returns true for matching checksum", func(t *testing.T) {
		if !ValidateCRC(key, value, want) {
			t.Errorf("ValidateCRC() returned false, expected true")
		}
	})

	t.Run("ValidateCRC returns false for mismatched checksum", func(t *testing.T) {
		badChecksum := want + 1
		if ValidateCRC(key, value, badChecksum) {
			t.Errorf("ValidateCRC() returned true for wrong checksum")
		}
	})
}
