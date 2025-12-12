package main

import (
	"fmt"

	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
	"github.com/0xRadioAc7iv/go-bitcask/pkg/bitcask"
)

func main() {
	dirPath, maxDataFileSize, port := utils.HandleCLIInputs()

	bitcask := bitcask.Bitcask{
		DirectoryPath:       *dirPath,
		MaximumDatafileSize: *maxDataFileSize,
		ListenerPort:        *port,
	}

	defer bitcask.Stop()
	if err := bitcask.Start(); err != nil {
		fmt.Println("Error while starting:", err)
	}

	utils.ListenForProcessInterruptOrKill()
}
