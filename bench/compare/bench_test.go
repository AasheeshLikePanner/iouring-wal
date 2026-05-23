package bench

import (
	"crypto/rand"
	"os"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func BenchmarkAppend_1Writer_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 128<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.DefaultConfig
	coalescer := writer.NewCoalescer(mgr, ring, cfg)
	defer coalescer.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := coalescer.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkAppend_1Writer_1KB_MaxWait1us(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-fast-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 128<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.Config{
		MaxBatch:     64,
		MaxBatchSize: 1 << 20,
		MaxWait:      1 * time.Microsecond,
	}
	coalescer := writer.NewCoalescer(mgr, ring, cfg)
	defer coalescer.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := coalescer.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkBaseline_WriteOnly(b *testing.B) {
	f, err := os.CreateTemp("", "urlog-baseline-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkBaseline_WriteSync(b *testing.B) {
	f, err := os.CreateTemp("", "urlog-baseline-sync-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

// Write + Sync every 64 entries — mimics batch fsync strategy.
func BenchmarkBaseline_WriteSyncEvery64(b *testing.B) {
	f, err := os.CreateTemp("", "urlog-baseline-sync64-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(payload); err != nil {
			b.Fatal(err)
		}
		if i%64 == 63 {
			if err := f.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

// mmap append: write to mmap'd region, msync every 64 entries.
// Uses raw syscall for msync since Go's syscall package doesn't export it.
func BenchmarkBaseline_MmapSyncEvery64(b *testing.B) {
	f, err := os.CreateTemp("", "urlog-baseline-mmap-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	const maxEntries = 65536
	sz := maxEntries * 1024
	if err := f.Truncate(int64(sz)); err != nil {
		b.Fatal(err)
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, sz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		b.Fatal(err)
	}
	defer syscall.Munmap(data)

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	const SYS_MSYNC = 227
	const MS_SYNC = 4

	var n int
	b.ResetTimer()
	for i := 0; i < b.N && i < maxEntries; i++ {
		off := i * 1024
		copy(data[off:off+1024], payload)
		if i%64 == 63 {
			start := (i - 63) * 1024
			length := off + 1024 - start
			if _, _, err := syscall.Syscall(SYS_MSYNC, uintptr(unsafe.Pointer(&data[start])), uintptr(length), MS_SYNC); err != 0 {
				b.Fatal(err)
			}
		}
		n++
	}
	b.StopTimer()

	b.ReportMetric(float64(n)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkAppend_Batch_64x1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-batch-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 128<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.DefaultConfig
	cfg.MaxWait = 10 * time.Millisecond
	coalescer := writer.NewCoalescer(mgr, ring, cfg)
	defer coalescer.Close()

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := coalescer.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkAppend_64Writers_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-64w-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.DefaultConfig
	cfg.MaxBatchSize = 4 << 20
	coalescer := writer.NewCoalescer(mgr, ring, cfg)

	payload := make([]byte, 1024)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := coalescer.Append(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	coalescer.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkHarvest_1024(b *testing.B) {
	ring := uring.NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-harvest-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())

	data := []byte("benchmark harvest")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1024; j++ {
			ring.SubmitWrite(f, data, 0, uint64(j))
		}
		completions := ring.HarvestCompletions(time.Second)
		if len(completions) != 1024 {
			b.Fatalf("expected 1024 completions, got %d", len(completions))
		}
	}

	f.Close()
}

func BenchmarkWritev_64Buffers(b *testing.B) {
	ring := uring.NewFakeRing()
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-writev-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	bufs := make([][]byte, 64)
	for i := range bufs {
		bufs[i] = make([]byte, 1024)
		_, _ = rand.Read(bufs[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iovecs := make([]syscall.Iovec, 64)
		for j := range bufs {
			iovecs[j] = syscall.Iovec{Base: &bufs[j][0], Len: uint64(len(bufs[j]))}
		}
		ring.SubmitWritev(f, iovecs, 0, uint64(i))
		ring.HarvestCompletions(time.Second)
	}
}
