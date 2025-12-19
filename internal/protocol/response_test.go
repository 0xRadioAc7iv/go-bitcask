package protocol_test

import (
	"net"
	"testing"
	"time"

	"github.com/0xRadioAc7iv/go-bitcask/internal/protocol"
)

func TestEncodeDecodeResponse(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{"simple response", "ok"},
		{"nil response", "nil"},
		{"empty response", ""},
		{"long response", "this is a longer response with spaces"},
		{"multiline response", "line1\nline2\nline3"},
		{"unicode response", "こんにちは世界"},
		{"large response", string(make([]byte, 2048))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			payload, err := protocol.EncodeResponse(tt.response)
			if err != nil {
				t.Fatalf("EncodeResponse failed: %v", err)
			}

			go func() {
				_, _ = client.Write(payload)
			}()

			resp, err := protocol.DecodeResponse(server)
			if err != nil {
				t.Fatalf("DecodeResponse failed: %v", err)
			}

			if resp != tt.response {
				t.Errorf("Response mismatch: got %q, want %q", resp, tt.response)
			}
		})
	}
}

func TestDecodeResponse_TruncatedPayload(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload, err := protocol.EncodeResponse("hello world")
	if err != nil {
		t.Fatalf("EncodeResponse failed: %v", err)
	}

	go func() {
		_, _ = client.Write(payload[:len(payload)/2])
		client.Close()
	}()

	if _, err := protocol.DecodeResponse(server); err == nil {
		t.Fatalf("expected error on truncated response, got nil")
	}
}

func TestDecodeResponse_BlocksUntilComplete(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload, err := protocol.EncodeResponse("blocking test")
	if err != nil {
		t.Fatalf("EncodeResponse failed: %v", err)
	}

	done := make(chan struct{})

	go func() {
		_, _ = protocol.DecodeResponse(server)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("DecodeResponse returned early")
	case <-time.After(50 * time.Millisecond):
	}

	_, _ = client.Write(payload)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("DecodeResponse did not return after full payload")
	}
}
