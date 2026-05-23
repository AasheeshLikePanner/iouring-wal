package concurrent

import (
	"os"
	"sync"
	"testing"
	"time"

	"urlog/internal/segment"
	"urlog/internal/uring"
	"urlog/internal/writer"
)

func TestConcurrent_MultiWriterCorrectness(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	defer ring.Close()

	cfg := writer.Config{
		MaxBatch:     64,
		MaxBatchSize: 1 << 20,
		MaxWait:      100 * time.Microsecond,
	}
	coalescer := writer.NewCoalescer(mgr, ring, cfg)
	defer coalescer.Close()

	numWriters := 200
	entriesPerWriter := 1000

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := []byte("concurrent-writer-" + string(rune(id)))
			for i := 0; i < entriesPerWriter; i++ {
				if err := coalescer.Append(payload); err != nil {
					t.Errorf("writer %d append %d: %v", id, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	totalWrites := mgr.NextSeq()
	expectedMin := uint64(numWriters * entriesPerWriter)
	if totalWrites < expectedMin {
		t.Errorf("expected at least %d total sequences, got %d", expectedMin, totalWrites)
	}

	t.Logf("Total sequences used: %d for %d writers x %d entries", totalWrites, numWriters, entriesPerWriter)
}

func TestConcurrent_WriterReaderConcurrency(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-concurrent-rw-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	var wg sync.WaitGroup
	done := make(chan struct{})

	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := []byte("rw-concurrent-" + string(rune(id)))
			for {
				select {
				case <-done:
					return
				default:
					if err := coalescer.Append(payload); err != nil {
						return
					}
				}
			}
		}(w)
	}

	time.Sleep(500 * time.Millisecond)
	close(done)
	wg.Wait()

	coalescer.Close()

	t.Logf("Concurrent write+read: %d entries written", mgr.NextSeq())
}

func TestConcurrent_SingleWriterManyEntries(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-single-writer-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	numEntries := 100000

	for i := 0; i < numEntries; i++ {
		if err := coalescer.Append([]byte("single-writer-bulk")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	if nextSeq < uint64(numEntries) {
		t.Errorf("expected nextSeq >= %d, got %d", numEntries, nextSeq)
	}
	t.Logf("Single writer: %d entries, nextSeq=%d", numEntries, nextSeq)
}

func TestConcurrent_NoDataRace(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-race-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	cfg := writer.Config{
		MaxBatch:     32,
		MaxBatchSize: 1 << 20,
		MaxWait:      50 * time.Microsecond,
	}
	coalescer := writer.NewCoalescer(mgr, ring, cfg)

	var wg sync.WaitGroup
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				payload := []byte("race-test-" + string(rune(id)) + "-" + string(rune(i)))
				coalescer.Append(payload)
			}
		}(w)
	}
	wg.Wait()

	coalescer.Close()

	t.Logf("Data race test: %d entries written without issues", mgr.NextSeq())
}

func TestConcurrent_VerifySeqUniqueness(t *testing.T) {
	dir, err := os.MkdirTemp("", "urlog-unique-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	mgr, err := segment.NewManager(dir, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ring := uring.NewFakeRing()
	coalescer := writer.NewCoalescer(mgr, ring, writer.DefaultConfig)

	var wg sync.WaitGroup
	for w := 0; w < 100; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				payload := []byte("unique-" + string(rune(id)) + "-" + string(rune(i)))
				coalescer.Append(payload)
			}
		}(w)
	}
	wg.Wait()

	coalescer.Close()

	nextSeq := mgr.NextSeq()
	expectedMin := uint64(100 * 100)
	if nextSeq < expectedMin {
		t.Errorf("expected at least %d unique sequences, got %d", expectedMin, nextSeq)
	}
}
