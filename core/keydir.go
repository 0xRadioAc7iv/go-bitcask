package core

// KeyDirEntry represents the in-memory index entry for a single key.
//
// Each entry points to the latest known version of a key stored on disk.
// Older versions may still exist in immutable datafiles but are ignored
// based on the Timestamp.
//
// The KeyDir is rebuilt on startup by scanning datafiles or reading
// hint files.
type KeyDirEntry struct {
	FileName   string // Datafile name containing the record
	Offset     uint32 // Byte offset in the datafile where the record starts
	ValueSize  uint32 // Size of the value in bytes
	RecordSize uint32 // Total size of the record on disk (header + key + value)
	Timestamp  int64  // Timestamp of the record
}

// KeyDir is the in-memory index mapping keys to their latest on-disk entries.
//
// It is the primary structure used to service read requests efficiently
// without scanning datafiles.
type KeyDir map[string]KeyDirEntry
