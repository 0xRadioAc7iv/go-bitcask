package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

// Command represents a decoded client command received by the Bitcask server.
//
// A Command consists of a command name (Cmd), an optional key, and an optional
// value. The meaning of Key and Val depends on the command type (e.g. GET,
// SET, DELETE).
type Command struct {
	Cmd string // Command name (e.g. "get", "set", "delete")
	Key string // Key argument (may be empty)
	Val string // Value argument (may be empty)
}

// EncodeCommand serializes a client command into its wire format.
//
// The command is encoded as:
//
//	<cmd_len:uint8><key_len:uint32><val_len:uint32><cmd><key><val>
//
// All integer fields are encoded using big-endian byte order.
// The command name length is limited to 255 bytes.
//
// The returned byte slice is suitable for writing directly to a TCP
// connection.
func EncodeCommand(cmd, key, val string) ([]byte, error) {
	cmdB := []byte(cmd)
	keyB := []byte(key)
	valB := []byte(val)

	buf := &bytes.Buffer{}

	buf.WriteByte(uint8(len(cmdB)))
	if err := binary.Write(buf, binary.BigEndian, uint32(len(keyB))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(valB))); err != nil {
		return nil, err
	}

	buf.Write(cmdB)
	buf.Write(keyB)
	buf.Write(valB)

	return buf.Bytes(), nil
}

// DecodeCommand reads and decodes a command from a TCP connection.
//
// It first reads the length-prefixed header fields, then reads the
// command name, key, and value payloads in sequence.
//
// DecodeCommand blocks until the full command has been read or an
// error occurs. A successfully decoded Command is returned on success.
func DecodeCommand(conn net.Conn) (*Command, error) {
	var cmdLen uint8
	var keyLen uint32
	var valLen uint32

	// Read lengths
	if err := binary.Read(conn, binary.BigEndian, &cmdLen); err != nil {
		return nil, err
	}
	if err := binary.Read(conn, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(conn, binary.BigEndian, &valLen); err != nil {
		return nil, err
	}

	// Read payload
	cmdB := make([]byte, cmdLen)
	keyB := make([]byte, keyLen)
	valB := make([]byte, valLen)

	if _, err := io.ReadFull(conn, cmdB); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(conn, keyB); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(conn, valB); err != nil {
		return nil, err
	}

	return &Command{
		Cmd: string(cmdB),
		Key: string(keyB),
		Val: string(valB),
	}, nil
}
