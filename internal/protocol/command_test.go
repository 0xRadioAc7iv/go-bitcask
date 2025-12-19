package protocol_test

import (
	"net"
	"testing"
	"time"

	"github.com/0xRadioAc7iv/go-bitcask/internal/protocol"
)

func TestEncodeDecodeCommand(t *testing.T) {
	tests := []struct {
		name string
		cmd  string
		key  string
		val  string
	}{
		{"SET command", "set", "foo", "bar"},
		{"GET command", "get", "hello", ""},
		{"COUNT command", "count", "", ""},
		{"empty key and value", "ping", "", ""},
		{"value with spaces", "set", "city", "new york"},
		{"unicode value", "set", "emoji", "🚀🔥"},
		{"large value", "set", "big", string(make([]byte, 1024))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			payload, err := protocol.EncodeCommand(tt.cmd, tt.key, tt.val)
			if err != nil {
				t.Fatalf("EncodeCommand failed: %v", err)
			}

			go func() {
				_, _ = client.Write(payload)
			}()

			cmd, err := protocol.DecodeCommand(server)
			if err != nil {
				t.Fatalf("DecodeCommand failed: %v", err)
			}

			if cmd.Cmd != tt.cmd {
				t.Errorf("Cmd mismatch: got %q, want %q", cmd.Cmd, tt.cmd)
			}
			if cmd.Key != tt.key {
				t.Errorf("Key mismatch: got %q, want %q", cmd.Key, tt.key)
			}
			if cmd.Val != tt.val {
				t.Errorf("Val mismatch: got %q, want %q", cmd.Val, tt.val)
			}
		})
	}
}

func TestDecodeCommand_TruncatedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload, err := protocol.EncodeCommand("set", "key", "value")
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	// Write only part of the payload
	go func() {
		_, _ = client.Write(payload[:len(payload)/2])
		client.Close()
	}()

	if _, err := protocol.DecodeCommand(server); err == nil {
		t.Fatalf("expected error on truncated payload, got nil")
	}
}

func TestDecodeCommand_BlocksUntilComplete(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload, err := protocol.EncodeCommand("get", "foo", "")
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	done := make(chan struct{})

	go func() {
		_, _ = protocol.DecodeCommand(server)
		close(done)
	}()

	// Ensure decoder is blocked
	select {
	case <-done:
		t.Fatal("DecodeCommand returned early")
	case <-time.After(50 * time.Millisecond):
	}

	_, _ = client.Write(payload)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("DecodeCommand did not return after full payload")
	}
}
