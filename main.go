package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"strings"
	"time"
)

// Using JSONL here
//  1. Makes it easier to read/write line by line
//  2. Ability to just append to the file unlike json files
//     where the whole file needs to be rewritten
const DATA_FILE_NAME = "data.jsonl"
const WAL_FILE_NAME = "wal.jsonl" // Write Ahead Log File for Recovery

var datamap = make(map[string]string)
var diff []string = []string{}
var wal *os.File

func main() {
	handleStartup()

	go writeToDiskPeriodically()
	go walFsyncInterval()

	startListening()
}

func writeToDiskPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if len(diff) == 0 {
			continue
		}

		fmt.Println("[CRON] Writing to disk...")

		var dataToWrite = make(map[string]string)

		for i := 0; i < len(diff); i++ {
			key := diff[i]
			value := datamap[key]
			dataToWrite[key] = value
		}

		dataStr, err := json.Marshal(dataToWrite)
		if err != nil {
			fmt.Println(err)
			continue
		}

		f, err := os.OpenFile("data.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Append JSON + newline
		if _, err := f.Write(append(dataStr, '\n')); err != nil {
			fmt.Println(err)
		}

		f.Close()
		diff = diff[:0]

		fmt.Println("[CRON] Disk Write Completed. ")
	}
}

func walFsyncInterval() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		err := wal.Sync()
		if err != nil {
			fmt.Println("Error Syncing WAL to Disk: ", err)
		}
	}
}

func fileExists(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}

func loadDataFileInMemory() {
	f, err := os.Open(DATA_FILE_NAME)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var obj map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			panic(err)
		}

		maps.Copy(datamap, obj)
	}
}

func replayWAL() {
	f, err := os.Open(WAL_FILE_NAME)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	seen := make(map[string]bool)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var obj map[string]string

		err = json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			fmt.Println(err)
		}

		// Write WAL to Hashmap
		maps.Copy(datamap, obj)

		// Write WAL to Diff (which is periodically written to disk)
		for k := range obj {
			if !seen[k] {
				diff = append(diff, k)
				seen[k] = true
			}
		}
	}

}

func handleStartup() {
	var err error

	if !fileExists(DATA_FILE_NAME) {
		fmt.Println("Data file not found. Creating one...")
		_, err = os.Create(DATA_FILE_NAME)
		if err != nil {
			fmt.Println(err)
		}
	} else {
		loadDataFileInMemory()
	}

	if !fileExists(WAL_FILE_NAME) {
		fmt.Println("Write Ahead Log file not found. Creating one...")
		wal, err = os.OpenFile(WAL_FILE_NAME, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		return
	}

	replayWAL()

	wal, err = os.OpenFile(WAL_FILE_NAME, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
}

func startListening() {
	ln, err := net.Listen("tcp", ":6969")
	if err != nil {
		panic(err)
	}
	fmt.Println("listening on :6969...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("error accepting connection:", err)
			continue
		}

		// Handle each client in a goroutine.
		go handle(conn)
	}
}

func reply(conn net.Conn, reply string) {
	_, err := conn.Write([]byte(reply))

	if err != nil {
		fmt.Println("client disconnected")
		return
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		// Read until newline for simplicity.
		command, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("client disconnected")
			return
		}

		command = strings.TrimSpace(command)

		fmt.Printf("Received Command: %s\n", command)

		cmd, key, value, err := parseCommand(command)

		if err != nil {
			reply(conn, err.Error())
		}

		replyMsg := executeCommand(cmd, key, value)
		reply(conn, replyMsg)
	}
}

func parseCommand(commandString string) (cmd, key, value string, err error) {
	cmdArray := strings.Split(commandString, " ")
	cmdArrayLen := len(cmdArray)

	cmd = cmdArray[0]

	if cmd != "GET" && cmd != "SET" {
		return "", "", "", errors.New("Invalid Command\n")
	}

	if cmd == "GET" {
		if cmdArrayLen != 2 {
			return "", "", "", errors.New("Invalid Command Length\n")
		}

		key = cmdArray[1]

		return cmd, key, "", nil
	}

	if cmd == "SET" {
		if cmdArrayLen != 3 {
			return "", "", "", errors.New("Invalid Command Length\n")
		}

		key = cmdArray[1]
		value = cmdArray[2]

		return cmd, key, value, nil
	}

	return "", "", "", errors.New("UNREACHABLE CODE\n")
}

func executeCommand(cmd, key, value string) (reply string) {
	switch cmd {
	case "GET":
		return command_GET(key)
	case "SET":
		return command_SET(key, value)
	default:
		return ""
	}
}

func command_GET(key string) (reply string) {
	val, ok := datamap[key]

	if ok {
		return val + "\n"
	}

	return "nil\n"
}

func command_SET(key, value string) (reply string) {
	writeToWal(key, value)

	datamap[key] = value
	diff = append(diff, key)

	fmt.Println("SET data:", datamap[key])
	return "OK\n"
}

func writeToWal(key, value string) {
	data := make(map[string]string)
	data[key] = value

	dataBytes, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	_, err = wal.Write(append(dataBytes, '\n'))
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
