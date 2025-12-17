package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

type Command struct {
	Cmd string
	Key string
	Val string
}

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
