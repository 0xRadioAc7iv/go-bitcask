package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
)

// EncodeResponse serializes a server response into its wire format.
//
// The response is encoded as:
//
//	<length:uint32><response bytes>
//
// where length is the number of bytes in the response payload.
//
// The returned byte slice is suitable for writing directly to a TCP
// connection.
func EncodeResponse(resp string) ([]byte, error) {
	respB := []byte(resp)

	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(respB))); err != nil {
		return nil, err
	}

	buf.Write(respB)

	return buf.Bytes(), nil
}

// DecodeResponse reads and decodes a response from a TCP connection.
//
// It first reads a 4-byte big-endian length prefix, followed by exactly
// that many bytes from the connection. The payload is returned as a string.
//
// DecodeResponse blocks until the full response has been read or an
// error occurs.
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
