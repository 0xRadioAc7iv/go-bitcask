package record

import "hash/crc32"

func CalculateCRC(key, value []byte) uint32 {
	checksumData := append(key, value...)
	return crc32.ChecksumIEEE(checksumData)
}

func ValidateCRC(key, value []byte, checksum uint32) bool {
	expectedChecksum := CalculateCRC(key, value)
	return expectedChecksum == checksum
}
