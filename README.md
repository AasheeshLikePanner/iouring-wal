# iouring-wal — io_uring Append-Only Log in Go

A write-ahead log using raw Linux `io_uring` syscalls, implemented in Go with zero external dependencies.

```
Naive fsync per write:                ~5K ops/sec
iouring-wal interrupt (64 writers):  ~27K ops/sec    5x faster
iouring-wal SQPOLL (64 writers):     ~5K ops/sec     hardware-dependent
```

## What we built

Raw syscall `io_uring` — no cgo, no liburing, no third-party Go libraries. Implemented the SQE/CQ ring protocol from scratch including mmap setup, kernel struct layouts with correct padding, and the `IORING_ENTER` syscall for submit/harvest.

SQ/CQ ring management with atomic head/tail pointers, SQE array indexing, completion harvesting with timeout, and proper mmap cleanup.

**SQPOLL mode** — `IORING_SETUP_SQPOLL` with kernel thread wakeup. Works correctly on any kernel 5.6+, but only outperforms interrupt mode on fast NVMe hardware with spare CPU cores (see trade-offs).

**`FakeRing` test double** — implements the same `Ring` interface using real `pwrite`/`pwritev`/`fsync`. All tests run deterministically on any OS without io_uring.

**Condvar-based coalescer** — concurrent `Append()` calls batch into groups. Each caller blocks on `sync.Cond` until the batch completes. Replaced per-entry result channels with per-batch broadcast, eliminating 76% scheduler overhead.

**SQE struct bug** — Go struct was 56 bytes, kernel expects 64. Missing `_pad1`/`_pad2`/`_pad3` padding fields. Every SQE after the first read garbage for `fd`/`offset`/`addr`. Fixed by matching the exact kernel layout.

**Binary entry format** — `[4B bodyLen][8B seq][4B CRC32][payload]`. Zero-alloc encode path, CRC integrity on every entry.

**Segments with rotation** — Fixed-size files with 40B header/trailer, automatic rotation on fill, sealed with CRC + fsync.

**Crash recovery** — Scans the last segment entry-by-entry up to the first CRC failure. Validates bodyLen boundaries to avoid runaway scans.

**Reader via mmap** — Per-segment mmap cache, `ReadAt(seq)` for point lookups, `ReadAll()` for iteration.

## Benchmarks

Linux ARM64 VM (Ubuntu, Kernel 6.17, 4 vCPUs, slow disk). 1KB entries, durable writes (fsync per op).

![Throughput by method](chart1_methods.png)

```
Method                              Ops/sec     vs fsync
────────────────────────────────────────────────────────
Raw fsync per write                  4,923       1.0x
io_uring interrupt, 1 writer        10,173       2.1x
io_uring interrupt, 64 writers      26,936       5.5x
io_uring SQPOLL, 64 writers          5,273       1.1x
```

![Concurrency scaling](chart2_scaling.png)

io_uring interrupt mode scales with concurrency. SQPOLL doesn't on this hardware — the kernel thread's context switch cost (150-550µs) dwarfs disk latency.

## Tests

All tests use `FakeRing` — deterministic, fast, cross-platform. No io_uring needed for development.

```
Package           Tests
─────────────────────────
uring             10 tests
encoder           9 tests
segment           16 tests
writer            9 tests
reader            10 tests
recovery          11 tests
tests/concurrent  5 tests
tests/crash       5 tests
tests/chaos       9 tests
```

All packages pass `go test -race ./...` on macOS and Linux.

## Trade-offs & Limitations

**io_uring interrupt mode is the right default** unless you have fast NVMe and spare cores. On any hardware where disk latency exceeds syscall overhead, it wins.

**SQPOLL requires fast hardware.** The kernel thread context switch adds 150-550µs per batch on contended systems. Only beneficial on hardware with <10µs I/O latency and dedicated CPU cores.

**Seq gaps on crash are expected.** Sequence numbers are pre-allocated atomically before write. A crash between allocation and write creates a gap.

**No readers during write.** The mmap reader only touches sealed segments, never the active write segment.

## Build & Run

```bash
go build ./cmd/urlog
```

Benchmarks on Linux:
```bash
TMPDIR=/var/tmp go test -bench=. -benchtime=5s ./bench/...
```

Full test suite:
```bash
go test -race -timeout 300s ./...
```
