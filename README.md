# urlog — io_uring Append-Only Log in Go

A production-grade append-only write-ahead log using raw Linux `io_uring` syscalls, implemented in Go with zero external dependencies.

```
Naive fsync per write:          ~5K ops/sec
urlog io_uring (64 writers):   ~27K ops/sec    5x faster
urlog SQPOLL (64 writers):     ~5K ops/sec     hardware-dependent
```

## Why

Most Go developers reach for `os.File.Write` + `Sync` for durable logging. This blocks the calling goroutine on disk I/O and serializes writes behind a kernel mutex. With `io_uring`, you batch submissions and harvest completions asynchronously — no per-write syscall overhead, no contention on the file's inode lock.

## Architecture

```
Goroutines → Coalescer (batcher) → Segment Manager → io_uring Ring → Disk
```

**`Coalescer`** — Concurrent `Append()` calls batch into groups by count/size/timeout. Each caller blocks on a `sync.Cond` until its batch completes.

**`Encoder`** — Binary entry format: `[4B bodyLen][8B seq][4B CRC32][payload]`. Zero-alloc encode path.

**`Segment`** — Fixed-size files (`segment_N.urlog`) with 40B header/trailer. Manager rotates automatically when full, seals old segments with CRC + fsync.

**`Ring`** — Abstract io_uring interface. `LinuxRing` uses raw `syscall.Syscall6` for `io_uring_setup`/`io_uring_enter`. `FakeRing` substitutes real `pwrite`/`pwritev` for deterministic tests on any OS.

## Benchmarks

All benchmarks on Linux ARM64 VM (Ubuntu, Kernel 6.17, 4 vCPUs, slow disk). 1KB entries, durable writes (fsync per op).

![Chart 1](chart1_methods.png)

```
Method                              Ops/sec     vs fsync
────────────────────────────────────────────────────────
Raw fsync per write                  4,923       1.0x
io_uring interrupt, 1 writer        10,173       2.1x
io_uring interrupt, 64 writers      26,936       5.5x
io_uring SQPOLL, 64 writers          5,273       1.1x
```

![Chart 2](chart2_scaling.png)

io_uring interrupt mode scales with concurrency. SQPOLL doesn't on this hardware — the kernel thread's context switch cost (150-550µs) dwarfs the slow disk's latency.

![Chart 3](chart3_sqpoll.png)

**SQPOLL is a hardware optimization.** It eliminates the ~0.5µs `io_uring_enter` syscall, but on a VM with 70µs+ disk latency, that saving is noise. The kernel thread adds a context switch that costs 150-550µs per batch. SQPOLL only wins on fast NVMe (<10µs I/O) with dedicated CPU cores.

## Tests

All tests use `FakeRing` — deterministic, fast, cross-platform. No io_uring needed for development.

```
Testing methodology: FakeRing + real filesystem + seq verification + CRC integrity

Package           Tests                        Coverage
────────────────────────────────────────────────────────
uring             SQ/CQ lifecycle, overflow    10 tests
encoder           100k round-trips, CRC        9 tests
segment           Rotation, scan, recovery     16 tests
writer            Single/multi/chaos write     9 tests
reader            ReadAt, ReadAll, cache       10 tests
recovery          Clean/crash/multi-segment    11 tests
tests/concurrent  200 writers, race detection  5 tests
tests/crash       SIGKILL simulation           5 tests
tests/chaos       Disk full, corruption, edge   9 tests
```

All packages pass `go test -race ./...` on both macOS and Linux.

## Trade-offs & Limitations

**io_uring interrupt mode is the right default.** On any hardware where disk latency exceeds syscall overhead (all but the fastest NVMe), it wins.

**SQPOLL requires fast hardware and spare CPUs.** The kernel thread competes for CPU time. On a 4-core VM, it's a net loss. On a 64-core NVMe server, it can double throughput.

**O_DIRECT is deferred.** Direct I/O would require 512-byte aligned buffers and registered buffers. The benefit only materializes when disk bandwidth is the bottleneck — not the case on this VM.

**Seq gaps on crash are expected.** Sequence numbers are pre-allocated atomically before write. A crash between allocation and write creates a gap. Recovery scans to the last valid CRC and picks up from there.

**No readers during write.** The mmap reader assumes segments are immutable after sealing. Writes go to the active segment, which the reader does not touch until rotation.

## Build & Run

```bash
go build ./cmd/urlog
# No CGo. No external dependencies.
```

Benchmarks on Linux ARM64 VM (Ubuntu via lima):

```bash
TMPDIR=/var/tmp go test -bench=. -benchtime=5s ./bench/...
```

Full test suite with race detector:

```bash
go test -race -timeout 300s ./...
```
