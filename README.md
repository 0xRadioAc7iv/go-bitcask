# Bitcask (Go Implementation)

A clean, from-scratch implementation of the Bitcask storage engine in Go—
inspired by the original paper by Basho (Riak), built to be simple, fast, and developer-friendly.

This project is a work-in-progress, focused on correctness, clarity, and matching Bitcask’s core design principles:

- **Append-only datafiles**
- **In-memory key directory (KeyDir)**
- **Crash recovery via hint files**
- **Efficient writes, constant-time reads**

## 🛠️ Development Setup

### Prerequisites

- **Go** ≥ 1.25.4
- **Make** (macOS/Linux already have it; Windows users can install make via MinGW or Chocolatey)

## 📦 **Build**

```bash
make build
```

Produces the `bitcask` binary inside `./bin/`.

## ▶️ **Run**

```bash
make run
```

This runs the `bitcask` CLI with default settings.

To specify your own directory and max datafile size:

```bash
./bin/bitcask --dir ./store --dfsize 128
```

## 🧪 **Test**

```bash
make test
```

## 📚 **Reference**

- [**Bitcask: A Log-Structured Hash Table for Fast Key/Value Data**] (https://riak.com/assets/bitcask-intro.pdf)
  (original whitepaper by Basho)
