//go:build linux

package bench

import (
	"crypto/rand"
	"os"
	"syscall"
	"testing"
	"time"

	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func newRing(b *testing.B, sqpoll bool) *uring.LinuxRing {
	entries := uint32(4096)
	if sqpoll {
		ring, err := uring.NewRingWithSQPoll(entries)
		if err != nil {
			b.Skipf("SQPOLL not available: %v", err)
		}
		return ring
	}
	ring, err := uring.NewRing(entries)
	if err != nil {
		b.Fatal(err)
	}
	return ring
}

func BenchmarkLinux_Int_Append_1Writer_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-linux-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := newRing(b, false)
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

func BenchmarkLinux_SQ_Append_1Writer_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-sq-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := newRing(b, true)
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

func BenchmarkLinux_Int_Append_64Writers_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-linux-64w-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := newRing(b, false)
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

func BenchmarkLinux_SQ_Append_64Writers_1KB(b *testing.B) {
	dir, err := os.MkdirTemp("", "urlog-bench-sq-64w-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.Close()

	ring := newRing(b, true)
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

func BenchmarkLinux_Int_Harvest_1024(b *testing.B) {
	ring := newRing(b, false)
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-harvest-int-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())

	data := []byte("benchmark harvest")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1024; j++ {
			if err := ring.SubmitWrite(f, data, 0, uint64(j)); err != nil {
				b.Fatal(err)
			}
		}
		completions := ring.HarvestCompletions(time.Second)
		if len(completions) != 1024 {
			b.Fatalf("expected 1024 completions, got %d", len(completions))
		}
	}
	f.Close()
}

func BenchmarkLinux_SQ_Harvest_1024(b *testing.B) {
	ring := newRing(b, true)
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-harvest-sq-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(f.Name())

	data := []byte("benchmark harvest sq")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1024; j++ {
			if err := ring.SubmitWrite(f, data, 0, uint64(j)); err != nil {
				b.Fatal(err)
			}
		}
		deadline := time.Now().Add(time.Second)
		var completions []uring.Completion
		for len(completions) < 1024 {
			remaining := time.Until(deadline)
			if remaining < time.Millisecond {
				break
			}
			c := ring.HarvestCompletions(remaining)
			completions = append(completions, c...)
		}
		if len(completions) != 1024 {
			b.Fatalf("expected 1024 completions, got %d", len(completions))
		}
	}
	f.Close()
}

func BenchmarkLinux_Int_Writev_64Buffers(b *testing.B) {
	ring := newRing(b, false)
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-writev-int-*")
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
		completions := ring.HarvestCompletions(time.Second)
		if len(completions) != 1 {
			b.Fatalf("expected 1 completion, got %d", len(completions))
		}
	}
}

func BenchmarkLinux_SQ_Writev_64Buffers(b *testing.B) {
	ring := newRing(b, true)
	defer ring.Close()

	f, err := os.CreateTemp("", "urlog-writev-sq-*")
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
		completions := ring.HarvestCompletions(time.Second)
		if len(completions) != 1 {
			b.Fatalf("expected 1 completion, got %d", len(completions))
		}
	}
}
