# Bitcask (Go Implementation)

A clean, from-scratch implementation of the **Bitcask** key–value storage engine in Go, inspired by the original Basho (Riak) design.

---

## What is Bitcask?

Bitcask is a **log-structured, append-only** key–value storage engine optimized for:

- **Fast writes** — sequential, append-only I/O
- **Fast reads** — O(1) lookups via an in-memory index
- **Crash recovery** — deterministic rebuild from disk
- **Operational simplicity** — minimal on-disk structures

### Core Design Principles

- Append-only datafiles (no in-place updates)
- In-memory KeyDir mapping keys → file offsets
- Immutable datafiles once rotated
- Background merge & compaction
- Atomic file replacement

---

## What This Project Contains

### Storage Engine

- Append-only Bitcask datafiles
- In-memory KeyDir
- File rotation and disk sync
- Background merge & compaction
- Hint files for fast startup

### Client Library

A Go client for programmatic access:

- Simple API: `GET`, `SET`, `DELETE`, `EXISTS`, `COUNT`, `LIST`
- Binary protocol
- Designed for correctness and testability

### CLI Tool

- Interactive REPL
- Full command coverage
- Useful for debugging and exploration

---

## Quick Start

### Prerequisites

- **Go ≥ 1.23**
- **Make** (optional)

### Build

```bash
git clone https://github.com/0xRadioAc7iv/go-bitcask.git
cd go-bitcask
make build
```

This produces:

- `bin/bitcask` — server
- `bin/bk-cli` — CLI client

---

## Running the Server

```bash
./bin/bitcask
```

### Custom Configuration

```bash
./bin/bitcask \
  --dir ./data \
  --df-size 128 \
  --port 9999 \
  --sync 15 \
  --size-check 5 \
  --dead-ratio 0.4
```

### Configuration Options

| Flag                | Description                 | Default |
| ------------------- | --------------------------- | ------- |
| `--dir`             | Data directory              | `./`    |
| `--df-size`         | Max datafile size (MB)      | `64`    |
| `--port`            | TCP port                    | `9999`  |
| `--sync`            | fsync interval (s)          | `15`    |
| `--size-check`      | Rotation check interval (s) | `5`     |
| `--dead-ratio`      | Garbage ratio threshold     | `0.4`   |
| `--merge-files`     | Min files for merge         | `3`     |
| `--merge-size`      | Min merge size (MB)         | `256`   |
| `--max-merge-files` | Max files per merge         | `5`     |
| `--max-merge-bytes` | Max merge bytes (GB)        | `2`     |

---

## Using the CLI

```bash
./bin/bk-cli --host 127.0.0.1 --port 9999
```

Example session:

```text
> SET foo bar
ok

> GET foo
bar

> COUNT
1

> DELETE foo
ok
```

---

## Using the Client Library

```bash
go get github.com/0xRadioAc7iv/go-bitcask/bitcask
```

```go
client, _ := bitcask.Connect()
defer client.Close()

client.SET("user:1", "Alice")
v, _ := client.GET("user:1")
fmt.Println(v)
```

---

## How It Works (High-Level)

### Write Path

1. Append record to active datafile
2. Update KeyDir in memory
3. fsync periodically

### Read Path

1. Lookup key in KeyDir
2. Read record at offset
3. Validate CRC
4. Return value

### Merge & Compaction

- Runs periodically
- Selects files with high garbage ratio
- Rewrites only live records
- Atomically replaces old files

### Crash Recovery

- Rebuilds KeyDir from hint files (or datafiles)
- Truncates corrupted tails
- Resumes safely

---

## Development

```bash
make build
make test
make fmt
```

A data generation script is provided for stress-testing merge and compaction:

```bash
make datagen
```

---

## References

- **Bitcask: A Log-Structured Hash Table for Fast Key/Value Data** - [Original PDF](https://riak.com/assets/bitcask-intro.pdf)
- **Bitcask - A Log-Structured fast KV store** - [Arpit Bhayani's Blog](https://arpitbhayani.me/blogs/bitcask/)

---

### NOTE: This is a **learning-focused implementation**, not production software.
