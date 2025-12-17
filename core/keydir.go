package core

type KeyDirEntry struct {
	FileName   string
	Offset     uint32 // byte position in file where record starts
	ValueSize  uint32
	RecordSize uint32
	Timestamp  int64
}

type KeyDir map[string]KeyDirEntry
