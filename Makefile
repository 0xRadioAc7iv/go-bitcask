ENGINE=bitcask
CLI_NAME=bitcask-cli
BUILD_DIR=bin

.PHONY: fmt build-cli build-engine build test run-cli run-engine clean

fmt:
	go fmt ./...

build-cli:
	go build -o $(BUILD_DIR)/$(CLI_NAME) ./cmd/bitcask-cli

build-engine:
	go build -o $(BUILD_DIR)/$(ENGINE) ./cmd/bitcask

build: build-cli build-engine

test:
	go test ./...

run-cli: build-cli
	$(BUILD_DIR)/$(CLI_NAME)

run-engine: build-engine
	$(BUILD_DIR)/$(ENGINE)

clean:
	go clean
	rm -rf $(BUILD_DIR)
