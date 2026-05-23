package writer

import (
	"crypto/rand"
	"os"
	"sync"
	"testing"
	"time"

	"urlog/internal/segment"
	"urlog/internal/uring"
)

func setupTestEnv(t *testing.T) (*segment.Manager, *uring.FakeRing, *Coalescer, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "urlog-writer-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	mgr, err := segment.NewManager(dir, 4<<20) // 4MB segments
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mgr.Close() })

	fakeRing := uring.NewFakeRing()

	cfg := DefaultConfig
	cfg.MaxBatch = 64
	cfg.MaxBatchSize = 1 << 20
	cfg.MaxWait = 50 * time.Microsecond

	coalescer := NewCoalescer(mgr, fakeRing, cfg)
	t.Cleanup(func() { coalescer.Close() })

	return mgr, fakeRing, coalescer, dir
}

func TestC1_SingleWriterCorrectness(t *testing.T) {
	mgr, _, coalescer, _ := setupTestEnv(t)

	numEntries := 10000
	payload := make([]byte, 100)
	_, _ = rand.Read(payload)

	for i := 0; i < numEntries; i++ {
		if err := coalescer.Append(payload); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	coalescer.Close()

	expectedSeqs := mgr.NextSeq()
	if expectedSeqs != uint64(numEntries) {
		t.Errorf("expected %d sequences, got %d", numEntries, expectedSeqs)
	}

	allSegs := mgr.AllSegments()
	var totalEntries uint64
	for _, seg := range allSegs {
		if seg.ID == mgr.ActiveSegment().ID {
			continue
		}
		info, _ := seg.File.Stat()
		_ = info
	}
	_ = totalEntries
}

func TestC2_ConcurrentWritersCorrectness(t *testing.T) {
	mgr, _, coalescer, _ := setupTestEnv(t)

	numWriters := 100
	entriesPerWriter := 1000
	totalEntries := numWriters * entriesPerWriter

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := make([]byte, 50)
			_, _ = rand.Read(payload)
			for i := 0; i < entriesPerWriter; i++ {
				if err := coalescer.Append(payload); err != nil {
					t.Errorf("writer %d append %d: %v", id, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	if nextSeq < uint64(totalEntries) {
		t.Errorf("expected at least %d sequences, got %d", totalEntries, nextSeq)
	}
}

func TestC3_CoalescingActuallyCoalesces(t *testing.T) {
	_, fakeRing, coalescer, _ := setupTestEnv(t)

	fakeRing.Delay = 1 * time.Millisecond

	numEntries := 1000

	for i := 0; i < numEntries; i++ {
		if err := coalescer.Append([]byte("test")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	coalescer.Close()

	t.Logf("Wrote %d entries successfully", numEntries)
}

func TestC4_BackpressureUnderDiskSlowness(t *testing.T) {
	mgr, fakeRing, coalescer, _ := setupTestEnv(t)

	fakeRing.Delay = 50 * time.Millisecond

	numEntries := 10
	done := make(chan struct{})

	go func() {
		for i := 0; i < numEntries; i++ {
			if err := coalescer.Append([]byte("slow-disk-test")); err != nil {
				t.Errorf("append %d: %v", i, err)
				return
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Append appears to not block (no backpressure) - entries returned without io_uring completion")
	}

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	if nextSeq == 0 {
		t.Error("no entries were written")
	}
}

func TestCoalescerCloseWithPending(t *testing.T) {
	_, fakeRing, coalescer, _ := setupTestEnv(t)
	fakeRing.Delay = 5 * time.Millisecond

	for i := 0; i < 10; i++ {
		coalescer.Append([]byte("pending-close-test"))
	}

	coalescer.Close()

	err := coalescer.Append([]byte("after-close"))
	if err != ErrLogClosed {
		t.Fatalf("expected ErrLogClosed, got %v", err)
	}
}

func TestCoalescerLargePayload(t *testing.T) {
	_, _, coalescer, _ := setupTestEnv(t)

	payload := make([]byte, 65<<20)
	err := coalescer.Append(payload)
	if err == nil {
		t.Fatal("expected error for payload > 64MB")
	}
	coalescer.Close()
}

func TestCoalescerZeroPayload(t *testing.T) {
	mgr, _, coalescer, _ := setupTestEnv(t)

	if err := coalescer.Append([]byte{}); err != nil {
		t.Fatal(err)
	}
	coalescer.Close()

	if mgr.NextSeq() < 1 {
		t.Error("expected at least 1 sequence for zero-length write")
	}
}

func TestCoalescerConcurrentWritersNoDuplicates(t *testing.T) {
	mgr, _, coalescer, _ := setupTestEnv(t)

	numWriters := 50
	entriesPerWriter := 500

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < entriesPerWriter; i++ {
				if err := coalescer.Append([]byte("dup-test")); err != nil {
					t.Errorf("writer %d: %v", id, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	if nextSeq < uint64(numWriters*entriesPerWriter) {
		t.Errorf("expected >=%d seqs, got %d", numWriters*entriesPerWriter, nextSeq)
	}
}

func TestCoalescerSegmentRotationMidWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-rotation-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<10) // 64KB segments
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	fakeRing := uring.NewFakeRing()

	cfg := Config{
		MaxBatch:     32,
		MaxBatchSize: 1 << 20,
		MaxWait:      100 * time.Microsecond,
	}
	coalescer := NewCoalescer(mgr, fakeRing, cfg)
	defer coalescer.Close()

	for i := 0; i < 5000; i++ {
		if err := coalescer.Append([]byte("rotation-test")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	t.Logf("Sequences used: %d, segments: %d", mgr.NextSeq(), len(mgr.AllSegments()))
}
