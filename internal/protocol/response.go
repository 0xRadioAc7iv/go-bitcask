package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

func EncodeResponse(resp string) ([]byte, error) {
	respB := []byte(resp)

	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(respB))); err != nil {
		return nil, err
	}

	buf.Write(respB)

	return buf.Bytes(), nil
}

func DecodeResponse(conn net.Conn) (string, error) {
	var respLen uint32

	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return "", err
	}

	buf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}
