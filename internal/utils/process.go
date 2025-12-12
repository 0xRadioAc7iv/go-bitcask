package utils

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func ListenForProcessInterruptOrKill() {
	// Listen for Ctrl+C or kill
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Bitcask running... press Ctrl+C to exit")

	<-sigChan // block until signal arrives
}
