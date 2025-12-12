package bitcask

type KeyDirEntry struct {
	FileName   string
	Offset     uint32 // byte position in file where record starts
	ValueSize  uint32
	RecordSize uint32
}

type KeyDir map[string]KeyDirEntry
