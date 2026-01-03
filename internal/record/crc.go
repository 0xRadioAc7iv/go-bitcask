package record

import "hash/crc32"

// CalculateCRC computes the CRC32 checksum of the key-value pair using IEEE polynomial.
func CalculateCRC(key, value []byte) uint32 {
	checksumData := append(key, value...)
	return crc32.ChecksumIEEE(checksumData)
}

// ValidateCRC returns true if the provided checksum matches the computed CRC32 of the key-value pair
func ValidateCRC(key, value []byte, checksum uint32) bool {
	expectedChecksum := CalculateCRC(key, value)
	return expectedChecksum == checksum
}
