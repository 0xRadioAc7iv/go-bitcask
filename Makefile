APP_NAME=bitcask
BUILD_DIR=bin

.PHONY: fmt build test run clean

fmt:
	go fmt ./...

build:
	go build -o $(BUILD_DIR)/$(APP_NAME) ./cmd/bitcask

test:
	go test ./...

run: build
	$(BUILD_DIR)/$(APP_NAME)

clean:
	go clean
	rm -rf $(BUILD_DIR)
