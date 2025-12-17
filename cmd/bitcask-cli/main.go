package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/0xRadioAc7iv/go-bitcask/bitcask"
	"github.com/0xRadioAc7iv/go-bitcask/internal"
	"github.com/0xRadioAc7iv/go-bitcask/internal/utils"
)

func main() {
	host := flag.String("host", internal.DEFAULT_HOST, "Bitcask server host")
	port := flag.Int("port", internal.DEFAULT_PORT, "Bitcask server port")
	flag.Parse()

	client, err := bitcask.Connect(bitcask.WithHost(*host), bitcask.WithPort(*port))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Printf("Connected to %v:%d\n", *host, *port)
	fmt.Println("Type commands. 'help' for information or 'exit' to quit.")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("input error:", err)
			return
		}

		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		if line == "exit" {
			return
		}

		cmd, key, value, err := utils.SplitStringIntoCommandAndArguments(line)
		if err != nil {
			fmt.Println("parse error:", err)
			continue
		}

		resp, err := client.Execute(cmd, key, value)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(resp)
	}
}
