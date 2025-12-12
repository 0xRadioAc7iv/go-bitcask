package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
)

// Starts the TCP Server
func Start(ctx context.Context, port int, handler func(conn net.Conn)) error {
	var ln net.Listener
	var err error

	// Look for an open port (default is 6969)
	for {
		addr := fmt.Sprintf(":%d", port)
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			if errors.Is(err, syscall.EADDRINUSE) {
				port++
				continue
			}
			return err
		}
		break
	}

	// When ctx is cancelled, close listener
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	// Accept Loop
	for {
		conn, err := ln.Accept()
		if err != nil {
			// When ln.Close() is called, Accept() returns an error.
			// This is how we break out of the loop cleanly.
			select {
			case <-ctx.Done():
				return nil // graceful shutdown
			default:
				fmt.Println("Error accepting connection:", err)
				continue
			}
		}

		go handler(conn)
	}
}
