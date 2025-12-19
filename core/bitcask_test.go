package core_test

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/0xRadioAc7iv/go-bitcask/bitcask"
	"github.com/0xRadioAc7iv/go-bitcask/core"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir() + string(os.PathSeparator)
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func startBitcask(t *testing.T, dir string, port int) *core.Bitcask {
	t.Helper()

	bk := &core.Bitcask{
		DirectoryPath:       dir,
		MaximumDatafileSize: 1024 * 1024,
		ListenerPort:        port,
		SyncInterval:        1,
		SizeCheckInterval:   1,
	}

	if err := bk.Start(); err != nil {
		t.Fatalf("failed to start bitcask: %v", err)
	}

	// Give the TCP server a moment to bind
	time.Sleep(50 * time.Millisecond)

	t.Cleanup(func() {
		bk.Stop()
	})

	return bk
}

func connectClient(t *testing.T, port int) *bitcask.Client {
	t.Helper()

	client, err := bitcask.Connect(
		bitcask.WithHost("127.0.0.1"),
		bitcask.WithPort(port),
	)
	if err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client
}

func TestBitcaskStartStop(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	bk := startBitcask(t, dir, port)
	bk.Stop()
}

func TestBitcaskSetGet(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	startBitcask(t, dir, port)
	client := connectClient(t, port)

	if _, err := client.SET("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	val, err := client.GET("foo")
	if err != nil {
		t.Fatal(err)
	}

	if val != "bar" {
		t.Fatalf("expected bar, got %q", val)
	}
}

func TestBitcaskDelete(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	startBitcask(t, dir, port)
	client := connectClient(t, port)

	_, err := client.SET("a", "1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DELETE("x")
	if err != nil {
		t.Fatal(err)
	}

	val, _ := client.GET("x")
	if val != "nil" {
		t.Fatalf("expected nil, got %q", val)
	}
}

func TestBitcaskExists(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	startBitcask(t, dir, port)
	client := connectClient(t, port)

	_, err := client.SET("a", "1")
	if err != nil {
		t.Fatal(err)
	}

	exists, err := client.EXISTS("a")
	if err != nil {
		t.Fatal(err)
	}

	if exists != "true" {
		t.Fatalf("expected true, got %q", exists)
	}
}

func TestBitcaskCount(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	startBitcask(t, dir, port)
	client := connectClient(t, port)

	_, err := client.SET("a", "1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SET("b", "2")
	if err != nil {
		t.Fatal(err)
	}

	count, _ := client.COUNT()
	if count != "2" {
		t.Fatalf("expected 2, got %q", count)
	}
}

func TestBitcaskList(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	startBitcask(t, dir, port)
	client := connectClient(t, port)

	_, err := client.SET("a", "1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SET("b", "2")
	if err != nil {
		t.Fatal(err)
	}

	list, _ := client.LIST()

	if list == "nil" {
		t.Fatal("expected non-empty list")
	}
}

func TestBitcaskPersistence(t *testing.T) {
	dir := tempDir(t)
	port := freePort(t)

	{
		bk := startBitcask(t, dir, port)
		client := connectClient(t, port)

		_, err := client.SET("persist", "yes")
		if err != nil {
			t.Fatal(err)
		}
		client.Close()
		bk.Stop()
	}

	// restart
	{
		startBitcask(t, dir, port)
		client := connectClient(t, port)

		val, err := client.GET("persist")
		if err != nil {
			t.Fatal(err)
		}

		if val != "yes" {
			t.Fatalf("expected persisted value, got %q", val)
		}
	}
}
