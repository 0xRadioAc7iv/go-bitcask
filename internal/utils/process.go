package utils

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// ListenForProcessInterruptOrKill blocks until it receives an interrupt (Ctrl+C)
// or termination signal (SIGTERM), then returns. This is typically used to keep
// a program running until the user requests shutdown.
func ListenForProcessInterruptOrKill() {
	// Listen for Ctrl+C or kill
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("press Ctrl+C to exit")

	<-sigChan // block until signal arrives
}
