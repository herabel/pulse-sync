# pulse-sync

A differential peer-to-peer file synchronization daemon written in Go, built around Content-Defined Chunking (FastCDC), SIMD BLAKE3 cryptographic hashing, and a zero-reflection binary framing protocol.

Unlike fixed-block synchronization tools, `pulse-sync` uses content-defined boundaries to prevent byte-shift cascading when files undergo insertions or deletions. Only modified chunks are transmitted across the network, while unchanged chunks are indexed and assembled directly from the local target file.

---

## Architecture

The system is organized into modular subsystems:

```text
pkg/
├── cdc/         FastCDC dual-masking engine & SIMD BLAKE3 chunk hashing
├── protocol/    16-byte binary framing protocol over TCP (io.Reader/Writer)
├── manifest/    Chunk manifest generation, diff calculation, binary codecs
└── engine/      Network session orchestration and atomic zero-alloc reconstruction
cmd/
└── pulse-sync/  CLI entrypoint (sender / receiver daemon)
```

### Core Primitives

- **FastCDC Chunking (`pkg/cdc`)**: Implements the USENIX ATC '16 dual-mask algorithm (`MaskA=0x1FFF`, `MaskB=0x03FF`) with a 256-element 64-bit Gear table. Generates dynamic chunks bounded between 2 KB and 64 KB (target average: 8 KB).
- **SIMD BLAKE3 (`pkg/cdc`)**: Generates 256-bit cryptographic hashes for each chunk via a streaming pipeline without intermediate heap allocations.
- **Binary Wire Protocol (`pkg/protocol`)**: All network frames use a compact 16-byte Big-Endian header (`Magic=0x504C5343`, `Version=1`, `MsgType`, `SeqID`, `PayloadLen`). Payloads are read strictly via `io.ReadFull` to prevent partial TCP frame corruption.
- **Binary Manifest Codec (`pkg/manifest`)**: Custom binary serialization for manifests and requested hash slices. Avoids reflection, JSON bloat, and `encoding/gob` allocation overhead.
- **Atomic Reconstruct Engine (`pkg/engine`)**: Assembles target files into `.pulsetmp` by merging local disk reads (`ReadAt`) with received network deltas (`WriteAt`). Uses a single pre-allocated 64 KB buffer with sub-slice indexing, yielding 0 allocations per chunk during the write loop. Flushes with `Sync()` and renames atomically upon completion.

---

## Protocol Flow

Synchronization executes over a single TCP connection in three phases:

```text
Sender (SendFile)                               Receiver (ReceiveFile)
-----------------                               ----------------------
1. Scan local file -> Manifest
2. [MsgManifest] -----------------------------> 1. Decode sender manifest
                                                2. Stat target path:
                                                   - if exists: scan local manifest
                                                   - compute diff via manifest.Compare
                                                3. Encode missing chunk hashes
                                <-------------- 4. [MsgNeededChunks] (hash list)
3. Decode needed hashes
4. For each requested hash:
   - Read chunk from disk
   - Payload: [32B Hash + Chunk Data]
5. [MsgChunkData] (streamed) -----------------> 5. Collect delta chunks into memory
                                                6. Reconstruct(target, base, deltas)
                                                7. Atomic rename: .pulsetmp -> target
```

---

## Benchmarks

Measurements conducted on an **AMD Ryzen 5 7500F 6-Core Processor (12 threads)**:

### Microbenchmarks & Subsystems

| Subsystem | Operation | Throughput / Latency | Memory / Allocs |
|---|---|---|---|
| **CDC Raw Cut** | `BenchmarkCutPosition_Raw` (L1 Gear Scan) | **19,665 MB/s** (3.33 µs/chunk) | 0 B/op, 0 allocs |
| **CDC + BLAKE3** | `BenchmarkFastCDC_Pipeline` (1 thread) | **1,740 MB/s** | Streaming channel |
| **CDC + BLAKE3** | `BenchmarkFastCDC_Parallel` (12 threads) | **4,453 MB/s** | Parallel worker pool |
| **Protocol Header** | `BenchmarkHeaderRoundtrip` (Encode + Decode) | **4.51 ns/op** | 0 B/op, 0 allocs |
| **Framing Stream** | `BenchmarkFrame_Roundtrip` (16B Header + Payload) | **5,158 MB/s** (1.58 µs/op) | 3 allocs/op |
| **Hash Codec** | `BenchmarkHashes_Encode1024` (1024 hashes, 32 KB) | **6,836 MB/s** (4.79 µs/op) | 1 alloc/op |
| **Hash Codec** | `BenchmarkHashes_Decode1024` (1024 hashes, 32 KB) | **7,363 MB/s** (4.45 µs/op) | 1 alloc/op |
| **Manifest Diff** | `BenchmarkCompare` (1000 chunks, non-identical) | **58.17 µs/op** | $O(N)$ hash map diff |
| **Manifest Diff** | `BenchmarkCompare_Identical` (1000 chunks match) | **1.29 µs/op** | $O(1)$ fast-path |
| **Disk Reconstruct** | `BenchmarkReconstruct_ReusedBuffer_16MB` | **291.57 MB/s** (NTFS sync) | 789 KB, 0 allocs/loop |

### End-to-End Pipeline (Chunking + BLAKE3 + Manifest + Codec + Compare + Header)

```text
BenchmarkEndToEnd_Pipeline-12            1746.27 MB/s (single-thread pipeline)
BenchmarkEndToEndParallel_Pipeline-12    6000.72 MB/s (12 threads on Ryzen 7500F)
```

### Live LAN Synchronization (Windows 11 $\leftrightarrow$ EndeavourOS Linux, Wi-Fi)

- **Test Payload:** 131.59 MB binary
- **Cold Transfer (0% local chunks):** 11.311 s (effective throughput: 11.63 MB/s, wire-limited)
- **Differential Transfer (100% chunk match):** **297 ms** (effective throughput: **442.54 MB/s**, 38x speedup)
- **Integrity:** SHA-256 bit-for-bit parity verified across platforms.

---

## CLI Usage

### Receiver Mode (Listen on TCP port)

```bash
# Listen on port 9000, write received file to destination
pulse-sync -listen :9000 -out target.bin

# If -out is omitted, defaults to the original filename sent by the peer
pulse-sync -listen :9000
```

### Sender Mode (Connect to remote receiver)

```bash
pulse-sync -target 192.168.1.157:9000 -file source.bin
```

---

## Build & Test

### Run Test Suite
```bash
go test -v ./tests/...
```

### Run Benchmarks
```bash
go test -bench . -benchmem ./tests/...
```

### Compile Native Binary
```bash
# Windows
go build -o pulse-sync.exe ./cmd/pulse-sync

# Linux (static amd64 binary without cgo)
GOOS=linux GOARCH=amd64 go build -o pulse-sync-linux ./cmd/pulse-sync
```

---

## References

- **FastCDC Paper:** Wen Xia, Yukun Zhou, Hong Jiang, Dan Feng, Yu Hua, Yuchong Hu, Qing Liu, Yucheng Zhang. *[FastCDC: a Fast and Efficient Content-Defined Chunking Approach for Data Deduplication](https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia)*. 2016 USENIX Annual Technical Conference (USENIX ATC '16), Denver, CO, pp. 101–114.
- **BLAKE3 Specification:** Jack O'Connor, Jean-Philippe Aumasson, Samuel Neves, Zooko Wilcox-O'Hearn. *[BLAKE3: one function, fast everywhere](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf)*.

---

## License

MIT
