# urlog — io_uring Append-Only Log in Go
## Architecture, Algorithms, Testing, Benchmarks

---

## The villain and the fix

**The villain:** fsync() is synchronous and blocks. Every database write that needs durability blocks the writer until the kernel confirms data is on disk. At 100k writes/sec, fsync becomes the entire bottleneck. Standard `os.File.Write` + `Sync()` tops out around 50-100K ops/sec on commodity SSDs.

**The fix:** io_uring lets the writer submit N operations into a shared ring buffer with the kernel. The kernel processes them asynchronously. Completion notifications come back in another ring. The writer never blocks on the syscall boundary. You get the throughput of async writes with the durability guarantees of explicit fsync.

**The target:**
```
Standard os.File.Write + fsync:   ~80K writes/sec
mmap-based WAL:                   ~250K writes/sec  
urlog (io_uring + batching):      500K+ writes/sec
```

---

## The full data flow

```
Application
    │
    │  Append(payload)
    ▼
[Entry encoder]
    │
    │  [length 4B | seq 8B | crc32 4B | payload N B]
    ▼
[Write coalescer] ──── batches up to 64 entries
    │
    │  iovec[] for writev-style submission
    ▼
[io_uring Submission Queue]
    │
    │  IORING_OP_WRITEV → current segment FD
    │
    ├──► [Kernel: async write]
    │
    ▼
[io_uring Completion Queue]
    │
    │  harvest completions, notify waiters
    │
    ▼
[Durability tracker]
    │
    │  periodic IORING_OP_FSYNC for crash safety
    ▼
[Application gets ACK]
```

Readers use a separate path:

```
Reader.ReadAt(offset)
    │
    ▼
[Segment locator] ──── find segment for offset
    │
    ▼
[mmap'd segment file]
    │
    │  zero-copy read from page cache
    ▼
[Entry decoder] ──── validate CRC, return payload
```

Writes go through io_uring. Reads go through mmap. Two completely separate hot paths.

---

## Repository layout

```
urlog/
├── internal/
│   ├── uring/         # io_uring wrapper (SQ/CQ management)
│   ├── segment/       # segment file format + rotation
│   ├── encoder/       # entry serialization with CRC
│   ├── writer/        # write coalescer + submission engine
│   ├── reader/        # mmap-based reader
│   └── recovery/      # crash recovery scanner
├── bench/
│   ├── compare/       # vs standard file I/O, vs mmap, vs SQLite WAL
│   └── workloads/     # random write, batch write, sustained write
├── cmd/
│   └── urlog/         # demo binary
└── tests/
    ├── crash/         # process kill + recovery tests
    ├── concurrent/    # multi-writer correctness
    └── chaos/         # disk full, corruption, ring overflow
```

---

## Component 1 — io_uring Wrapper

The submission/completion ring management. This is the lowest layer everything depends on.

**Dependencies:** `github.com/pawelgaczynski/giouring` or `github.com/iceber/iouring-go`. Both are real Go bindings for io_uring. Pick whichever has better API ergonomics — both work.

**Data structure:**

```go
type Ring struct {
    submissionDepth uint32   // power of 2, e.g. 4096
    completionDepth uint32   // 2x submissionDepth typically
    
    // The actual io_uring instance
    iour            *iouring.IoURing
    
    // For polling completions
    cqeChan         chan *iouring.CompletionEvent
    
    // Outstanding ops awaiting completion
    pending         sync.Map  // userdata -> completion callback
}
```

**Two modes to support:**

1. **Interrupt-driven mode** (default) — submit ops, kernel raises interrupts when complete. Reasonable throughput, low CPU when idle.

2. **SQPOLL mode** (high throughput) — kernel polls SQ continuously, no syscall needed to submit ops. Higher throughput, higher CPU usage. Reserved for production.

**Key operations:**

```go
ring.SubmitWrite(fd int, buf []byte, offset int64, userdata uint64) error
ring.SubmitWritev(fd int, iovecs []syscall.Iovec, offset int64, userdata uint64) error
ring.SubmitFsync(fd int, userdata uint64) error
ring.HarvestCompletions(timeout time.Duration) []Completion
```

**Test UR-1: Single write completes successfully**
Submit one write of "hello", wait for completion, read file with standard syscall, verify "hello" is there.

**Test UR-2: Batched submission**
Submit 1000 writes in one call to `Submit()`, harvest all 1000 completions, verify each one succeeded.

**Test UR-3: SQ ring overflow handling**
Submit more operations than the SQ ring can hold. The wrapper must either block, return ErrRingFull, or grow — but never silently drop. Test all three behaviors.

**Test UR-4: CQ ring overflow handling**
Submit many writes without harvesting CQEs. Verify the wrapper detects CQ overflow and either backpressures submissions or grows.

**Test UR-5: Cleanup on shutdown**
Submit 100 ops, immediately close the ring. Verify no goroutine leaks, no panics, no zombie file descriptors.

**Benchmark UR-B1:**
```
BenchmarkSubmitWrite_Interrupt-8     2000000     500 ns/op   2M ops/s
BenchmarkSubmitWrite_SQPOLL-8        5000000     200 ns/op   5M ops/s
BenchmarkHarvest_1024-8                10000   80000 ns/op  12K batches/s
```

These are targets for the raw wrapper. Real numbers go in the blog.

---

## Component 2 — Entry Encoder

Wire format for entries inside a segment.

**Format:**
```
| length (4B BE) | seq (8B BE) | crc32 (4B) | payload (length bytes) |
```

- `length`: payload size in bytes (max 64MB enforced)
- `seq`: monotonically increasing sequence number across the entire log
- `crc32`: CRC32 of payload only (header is fixed-size, validated by structure)
- `payload`: opaque bytes provided by caller

Total overhead: 16 bytes per entry. For 1KB payloads, that's 1.6% overhead.

**Why this format:**

Fixed-size header means readers can always read 16 bytes, parse length, then read payload without any speculative reads. Sequence number outside payload means we can identify gaps after crash recovery. CRC32 hardware-accelerated on every modern x86 and ARM CPU (CRC32C instruction).

**Test E-1: Round-trip 100k random entries**
Generate 100k entries with random sizes (1B to 64KB) and random content. Encode all. Decode all. Byte-for-byte match.

**Test E-2: Corruption detection**
Encode an entry, flip a single bit in the payload, attempt decode. Must return ErrCorrupted, not return wrong data.

**Test E-3: Truncated entry detection**
Encode an entry, truncate the buffer to length/2. Decode must return ErrTruncated, not panic, not return partial data.

**Test E-4: Max size enforcement**
Encode a 100MB entry. Must return ErrTooLarge. The system must never attempt to write something larger than `MaxEntrySize`.

**Benchmark E-B1:**
```
BenchmarkEncode_1KB-8        50000000     30 ns/op    0 allocs/op
BenchmarkDecode_1KB-8        50000000     25 ns/op    0 allocs/op
BenchmarkCRC32_1KB-8        100000000     12 ns/op    (hardware-accelerated)
```

Zero allocs is mandatory. If you see allocations in the hot path, find the buffer reuse you missed.

---

## Component 3 — Segment Management

A segment is a single file that holds a contiguous chunk of the log.

**File structure:**

```
segment_0000000001.urlog
| magic (8B) | segment_id (8B) | created_unix (8B) | flags (4B) | reserved (12B) |   ← header: 40B
| entry 1 |
| entry 2 |
| entry N |
| trailer (40B) — written on segment close, contains: last_seq, entry_count, crc32 of header |
```

Default segment size: **128MB**. When the current segment reaches this limit, open the next one. Old segments are immutable.

**Why 128MB:**

Small enough that mmap'ing a segment doesn't bloat virtual memory. Large enough that segment creation overhead is amortized across millions of entries. Large enough for OS readahead to work well. This is what Kafka uses by default for the same reasons.

**Segment manager responsibilities:**

```go
type Manager struct {
    dir          string
    segmentSize  int64
    
    activeSegment   *Segment   // currently being written
    activeMutex     sync.RWMutex
    
    allSegments     []*Segment  // ordered by segment_id
    segmentLookup   map[uint64]*Segment  // for finding segment by entry seq
}
```

**On startup:**
1. Scan `dir` for `*.urlog` files
2. Sort by segment_id from filename
3. Validate each segment's header magic
4. The last segment is the "active" one (or create a new one if all are sealed)

**On rotation:**
1. Write the trailer to current segment
2. Mark segment immutable
3. Open the next segment with id = current_id + 1
4. Write header to new segment
5. Update `activeSegment`

**Test S-1: Segment rotation across boundary**
Write entries until current segment reaches 128MB. Verify next entry goes to a new segment file. Verify both files exist on disk with correct headers.

**Test S-2: Resume after restart**
Start the log, write 1000 entries, close. Restart. Verify the log reopens with `activeSegment` pointing to the last segment and `nextSeq` correctly set.

**Test S-3: Reject corrupted segments**
Truncate a segment header. Restart. Verify the system either skips the corrupted segment with a warning, or refuses to start with a clear error. Never silently produces wrong data.

**Test S-4: Concurrent reader during rotation**
A reader is reading from segment N. Writer rotates to segment N+1. Reader must continue without errors. Test by injecting rotation while a reader holds a reference.

---

## Component 4 — Write Coalescer + Submission Engine

This is the hot path. Where the throughput wins or loses.

**The coalescer:**

When multiple goroutines call `Append()`, instead of each one submitting its own io_uring op (high overhead per op), coalesce them into batches.

```go
type Coalescer struct {
    pending     []entryBuf       // entries waiting to be submitted
    pendingSize int              // current batch size in bytes
    
    maxBatch    int              // e.g. 64 entries
    maxBatchSize int             // e.g. 1MB
    maxWait     time.Duration    // e.g. 100µs
    
    flushChan   chan struct{}    // signal to flush
}
```

**Algorithm:**

```
Append(payload):
  encode entry into buffer
  acquire lock
  append to pending
  if pending count >= maxBatch OR pendingSize >= maxBatchSize:
    signal flush
  release lock
  wait for completion notification
  return

Flusher goroutine:
  loop:
    wait for flushChan signal OR maxWait timeout
    acquire lock
    snapshot pending entries
    clear pending
    release lock
    
    build iovec for all entries
    submit single IORING_OP_WRITEV to io_uring
    on completion: notify all waiters in the batch
```

**Why this design:**

A single `writev` syscall with 64 iovecs is dramatically faster than 64 separate `write` calls. The kernel can write all 64 buffers in one operation. Coalescing across goroutines exploits this without making the application code do anything special.

**Test C-1: Single writer correctness**
1 goroutine writes 1M entries sequentially. Read them back. Sequence numbers go 0..999999. No gaps. Payloads match.

**Test C-2: Concurrent writers correctness**
100 goroutines each write 10k entries (1M total). Read back. Verify 1M distinct entries exist. Sequence numbers are unique (no duplicates) and form a contiguous range 0..999999. The order across writers doesn't matter, but each individual goroutine's entries should be in the order it submitted them.

**Test C-3: Coalescing actually coalesces**
With instrumentation, count the number of io_uring submissions. Write 1000 entries from 1 goroutine. Verify the submission count is much less than 1000 — should be close to 1000/batch_size.

**Test C-4: Backpressure under disk slowness**
Inject 50ms delay into the io_uring completion path. Verify Append() blocks (doesn't return until durable) rather than silently buffering indefinitely.

**Benchmark C-B1:**
```
BenchmarkAppend_1Writer_1KB-8     500000   3000 ns/op   330K ops/s
BenchmarkAppend_64Writers_1KB-8  3000000    400 ns/op   2.5M ops/s
BenchmarkAppend_Batch_64x1KB-8   3000000    300 ns/op   3.3M ops/s
```

The high-concurrency number is the real story. Single-writer is decent. Many writers coalescing through one io_uring submission is where it wins.

---

## Component 5 — Reader

Readers use mmap, not io_uring. Reads from the page cache are already as fast as memory access — io_uring doesn't help for reads of recently-written data.

**Architecture:**

```go
type Reader struct {
    manager *segment.Manager
    
    // Each opened segment mmap'd once, shared by all readers
    mmapCache map[uint64]*mmapSegment
    cacheLock sync.RWMutex
}

func (r *Reader) ReadAt(seq uint64) (Entry, error) {
    seg := r.manager.SegmentForSeq(seq)
    msg := r.getMmap(seg)
    
    // Find offset of entry with this seq using the sparse index
    offset := msg.IndexLookup(seq)
    
    // Read header (16 bytes) directly from mmap
    header := msg.data[offset:offset+16]
    
    length := binary.BigEndian.Uint32(header[0:4])
    seqInFile := binary.BigEndian.Uint64(header[4:12])
    crc := binary.BigEndian.Uint32(header[12:16])
    
    if seqInFile != seq {
        return Entry{}, ErrSequenceMismatch
    }
    
    payload := msg.data[offset+16 : offset+16+int(length)]
    
    if crc32.ChecksumIEEE(payload) != crc {
        return Entry{}, ErrCorrupted
    }
    
    return Entry{Seq: seq, Payload: payload}, nil
}
```

**Sparse index:** every Nth entry (e.g. every 4096) records its byte offset in the segment. To find entry K, find the nearest indexed entry ≤ K, then scan forward through entries until you reach K. With 4096-entry granularity, the scan is bounded.

**Test R-1: Random read correctness**
Write 1M entries. Generate 10k random sequence numbers in range. Read each one. Verify payload matches what was written.

**Test R-2: Concurrent readers**
1 writer producing 100K entries/sec. 100 reader goroutines each reading random sequences. Verify all reads succeed, no torn reads, no corruption.

**Test R-3: Reader during segment rotation**
Active reader is reading from segment N. Writer fills segment N and rotates to N+1. Reader should continue with no errors and be able to read entries from N+1 once they exist.

**Benchmark R-B1:**
```
BenchmarkReadRandom_1KB-8        10000000    150 ns/op   (mmap cache hit)
BenchmarkReadSequential_1KB-8    20000000     50 ns/op   (OS prefetching)
BenchmarkReadAfterRestart-8       5000000    500 ns/op   (cold cache)
```

The cold cache number matters. After process restart, the first read pays a page fault. Subsequent reads in the same segment are L1/L2 cache hits.

---

## Component 6 — Recovery

When the process crashes mid-write, some entries may have been written to disk but not fsync'd. On restart, we need to find the last valid entry.

**Algorithm:**

```
Recover(dir):
  segments = list segments in dir, sorted by id
  
  for each segment except the last:
    validate header magic
    validate trailer exists (means segment was cleanly sealed)
    if either fails: declare segment corrupted, abort recovery
  
  lastSegment = segments[-1]
  validate header magic
  
  # the last segment may or may not have a trailer
  # if it does, segment is fully written
  # if not, we need to find the last valid entry
  
  if trailer exists:
    lastSeq = trailer.lastSeq
    return
  
  # scan from start of segment forward
  offset = headerSize
  lastValidOffset = offset
  lastValidSeq = 0
  
  while offset < segmentSize:
    read 16-byte header at offset
    
    if header is all zeros:
      # we've reached the unwritten portion
      break
    
    length = header.length
    
    if offset + 16 + length > segmentSize:
      # truncated entry
      break
    
    payload = read length bytes at offset+16
    
    if crc32(payload) != header.crc:
      # corruption — could be partial write
      break
    
    lastValidOffset = offset + 16 + length
    lastValidSeq = header.seq
    offset = lastValidOffset
  
  # truncate segment to last valid entry
  truncate(lastSegment, lastValidOffset)
  nextSeq = lastValidSeq + 1
```

**Test RC-1: Recovery after clean shutdown**
Write 10k entries, call Close() cleanly. Reopen. Verify all 10k entries readable, nextSeq is 10000.

**Test RC-2: Recovery after SIGKILL**
Write 10k entries. SIGKILL the process. Restart. Verify the recovery scan completes. The number of recovered entries should be ≤ 10000 (some may have been in-flight). Whatever's recovered must be valid — every entry has correct CRC.

**Test RC-3: Recovery with partial last entry**
Write 1000 entries. Manually corrupt the last entry's payload by flipping bytes. Restart. Recovery should detect the bad CRC, truncate the segment to entry 999, and resume with nextSeq=999.

**Test RC-4: Recovery with corrupted middle segment**
Have 3 segments. Corrupt segment 2's header. Restart. Recovery must refuse to start with a clear error, not silently skip segment 2 and lose data.

**Test RC-5: Recovery time scaling**
Measure recovery time with 1 segment, 10 segments, 100 segments. Recovery should be O(segments) — only scans the last segment in detail; older segments just need header validation.

---

## Benchmark harness

Build a standalone binary `urlog-bench` that runs comparable workloads against urlog, standard file I/O, mmap, and SQLite WAL.

**Workload definitions:**

```go
type Workload struct {
    Writers      int           // number of concurrent writer goroutines
    Duration     time.Duration // how long to run
    EntrySize    int           // bytes per entry
    DurableEvery int           // call Sync() every N entries (0 = never explicitly)
}
```

**Reference implementations to beat:**

1. **Standard `os.File.Write` + `Sync()` after each entry**
   ```go
   f.Write(entry)
   f.Sync()
   ```
   This is what naive WAL implementations do. Targets 20-50K ops/sec.

2. **`os.File.Write` + periodic `Sync()`**
   Write to file, fsync every 100ms or every N entries. Targets 100-200K ops/sec.

3. **mmap-based append**
   Write to mmap'd region, periodic msync. Targets 250-300K ops/sec.

4. **SQLite WAL mode**
   Standard SQLite with WAL=on, write transactions. Targets 30-100K ops/sec.

5. **BadgerDB WAL**
   The Go embedded database. Has its own WAL. Targets 50-150K ops/sec.

**Test matrix:**

| Writers | Entry Size | Durability | What's measured |
|---------|------------|------------|-----------------|
| 1       | 64B        | every entry | latency p50/p99 |
| 1       | 64B        | periodic    | throughput      |
| 64      | 1KB        | periodic    | throughput + scaling |
| 256     | 1KB        | periodic    | contention behavior |
| 1       | 64KB       | every entry | large entry latency |

**Run each:**
- 30-second sustained workload
- Warm-up 5 seconds (discarded)
- Measure: p50, p99, p99.9 latency + total throughput
- Report memory usage (RSS) before, during, after
- 3 runs per config, report median

**The graph that proves the point:**

Y-axis: writes per second.
X-axis: number of concurrent writers (1, 4, 16, 64, 256).
Lines: urlog, mmap WAL, SQLite WAL, naive Write+Sync.

urlog should scale roughly linearly with writers up to some point (where the io_uring SQ becomes the bottleneck), while the others flatten quickly because of their syscall overhead per write.

---

## Stage-by-stage build plan

### Week 1: io_uring wrapper + entry encoder

**Goal:** prove you can submit and complete io_uring ops correctly. No log yet.

1. Wrap the chosen io_uring Go library with the clean API
2. Implement entry encoder + decoder with CRC32
3. Pass UR-1 through UR-5, E-1 through E-4
4. Benchmark raw io_uring submission throughput
5. Decision point: if raw io_uring is below 1M ops/sec, something is wrong with the wrapper. Profile.

**You're done with Week 1 when:**
- 1 million writes via io_uring complete with valid data on disk
- Entry encode/decode is zero-allocation
- All wrapper tests pass

### Week 2: segments + writer + coalescer

**Goal:** the actual log works for a single writer.

1. Segment file format, header, trailer
2. Segment manager — create, rotate, list
3. Write coalescer + submission engine
4. Pass S-1 through S-4, C-1, C-3, C-4
5. Benchmark single-writer throughput

**You're done with Week 2 when:**
- Single writer hits 200K+ entries/sec sustained
- Segment rotation works invisibly to the caller
- No goroutine leaks across rotations

### Week 3: readers + recovery + multi-writer + benchmark

**Goal:** complete system. Compare against alternatives.

1. mmap-based reader with sparse index
2. Recovery scanner
3. Multi-writer coalescing
4. Pass R-1 through R-3, RC-1 through RC-5, C-2
5. Build comparison harness
6. Run benchmark matrix vs file+sync, mmap, SQLite, BadgerDB
7. Generate graphs

**You're done with Week 3 when:**
- 1M entries written, killed mid-flight, recovered with no data loss
- Benchmark shows urlog above mmap-based WAL at high writer counts
- All correctness tests pass under `-race`

---

## What makes this not a toy

**Correctness tests run first.**
Every commit runs the test suite under `go test -race`. RC-2 (SIGKILL recovery) is the gate. If that test fails, nothing else matters. No benchmark numbers get published until correctness is proven.

**Reproducible benchmarks.**
The bench harness is checked in. Any reader can clone the repo, run `make bench`, and produce numbers within 5% of yours. No hand-tuned configurations. No special hardware. Default settings only — Linux 6.x, ext4 filesystem, standard SSD.

**Honest comparison.**
You're benchmarking against fair opponents. SQLite WAL is the reasonable production alternative for embedded durable storage. BadgerDB is the closest pure-Go competitor. Don't compare against raw `os.File.Write` and claim victory — that's not what real applications do.

**Crash tests are real crashes.**
Not graceful shutdowns. `kill -9` mid-write. `dd if=/dev/zero` over segment files. Disk full simulation by writing to a small tmpfs. The system must either recover correctly or refuse to start with a clear error. Never silently produce wrong data.

**Memory accounting.**
Report bytes allocated per write. Report sustained memory (RSS) over a 1-hour benchmark. Show that the system doesn't leak under load. This number separates real systems from prototypes.

**Failure modes documented.**
What happens when:
- The io_uring submission ring fills?
- A segment file is deleted while the log is running?
- Two processes open the same log directory?
- The disk fills mid-write?

Every failure mode has an answer. The blog post documents them.

---

## The blog post

Title: *"How TigerBeetle gets its performance: I built an io_uring append-only log in Go and benchmarked it against everything"*

Structure:
1. The problem — fsync is synchronous, throughput dies at ~80K writes/sec
2. The fix — io_uring decouples submission from completion
3. Architecture walkthrough with the data flow diagram
4. The hot path — write coalescer + writev submission (the optimization that actually matters)
5. Correctness — recovery after SIGKILL, the tests that prove it works
6. Benchmarks — graph showing urlog vs mmap WAL vs SQLite vs naive
7. Honest limitations — Linux-only, single-machine, single-leader writes
8. Why this matters — every database, every streaming system, every event store depends on a log like this

That post, with real numbers, recovery proof, and the architecture diagram, gets attention from infra engineers at ByteDance, Alibaba, every company running Kafka or storage systems at scale.