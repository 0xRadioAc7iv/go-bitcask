package main

import (
	"fmt"

	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
	"github.com/0xRadioAc7iv/go-bitcask/pkg/bitcask"
)

func main() {
	dirPath, maxDataFileSize, port, syncInterval, sizeCheckInterval, err := utils.HandleCLIInputs()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	bitcask := bitcask.Bitcask{
		DirectoryPath:       *dirPath,
		MaximumDatafileSize: *maxDataFileSize,
		ListenerPort:        *port,
		SyncInterval:        *syncInterval,
		SizeCheckInterval:   *sizeCheckInterval,
	}

	defer bitcask.Stop()
	if err := bitcask.Start(); err != nil {
		fmt.Println("Error while starting:", err)
	}

	utils.ListenForProcessInterruptOrKill()
}
