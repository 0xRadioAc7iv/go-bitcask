package bitcask_test

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/0xRadioAc7iv/go-bitcask/bitcask"
	"github.com/0xRadioAc7iv/go-bitcask/internal/protocol"
)

func startTestServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			cmd, err := protocol.DecodeCommand(conn)
			if err != nil {
				return
			}

			var resp string

			switch strings.ToLower(cmd.Cmd) {
			case "ping":
				resp = "PONG!"
			case "set":
				resp = "ok"
			case "get":
				resp = "value:" + cmd.Key
			case "delete":
				resp = "ok"
			case "exists":
				resp = "true"
			case "count":
				resp = "42"
			case "list":
				resp = "a\nb\nc"
			default:
				resp = "error"
			}

			encoded, _ := protocol.EncodeResponse(resp)
			_, _ = conn.Write(encoded)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
	}
}

func TestConnect(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)

	client, err := bitcask.Connect(
		bitcask.WithHost(host),
		bitcask.WithPort(port),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer client.Close()
}

func TestClientSET(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.SET("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	if resp != "ok" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientGET(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.GET("hello")
	if err != nil {
		t.Fatal(err)
	}

	if resp != "value:hello" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientDELETE(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.DELETE("key")
	if err != nil {
		t.Fatal(err)
	}

	if resp != "ok" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientEXISTS(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.EXISTS("key")
	if err != nil {
		t.Fatal(err)
	}

	if resp != "true" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientCOUNT(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.COUNT()
	if err != nil {
		t.Fatal(err)
	}

	if resp != "42" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientLIST(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.LIST()
	if err != nil {
		t.Fatal(err)
	}

	if resp != "a\nb\nc" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientExecute(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	resp, err := client.Execute("count", "", "")
	if err != nil {
		t.Fatal(err)
	}

	if resp != "42" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestClientMultipleCommands(t *testing.T) {
	addr, shutdown := startTestServer(t)
	defer shutdown()

	client := mustConnect(t, addr)
	defer client.Close()

	if _, err := client.SET("a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SET("b", "2"); err != nil {
		t.Fatal(err)
	}

	resp, err := client.COUNT()
	if err != nil {
		t.Fatal(err)
	}

	if resp != "42" {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func mustConnect(t *testing.T, addr string) *bitcask.Client {
	t.Helper()

	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)

	client, err := bitcask.Connect(
		bitcask.WithHost(host),
		bitcask.WithPort(port),
	)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	return client
}
